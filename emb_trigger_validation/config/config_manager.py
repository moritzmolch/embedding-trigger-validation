# coding: utf-8

"""
Load YAML configuration and materialize it into configuration objects of classes from the :py:package:`order` package.
"""

from functools import wraps
from law.util import DotDict
from order import Campaign, Channel, Config, Dataset, Process, UniqueObjectIndex
import os
import threading
from typing import Any, Dict, Mapping, List
import yaml

from emb_trigger_validation.config import get_dataset_metadata


def check_keys(required=None, optional=None):
    required = required or set()
    optional = optional or set()

    def inner_func(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            for map in args:
                # check if the positional argument is a mapping, skip the argument otherwise
                if not isinstance(map, Mapping):
                    continue
                    #raise ValueError("positional arguments must be mappings")

                # get the keys of the mapping
                required_keys = set([item[0] for item in required])
                optional_keys = set([item[0] for item in optional])
                keys = set(map.keys())

                # check if all required keys are set
                required_but_not_set = keys.intersection(required_keys).difference(required_keys)
                if len(required_but_not_set) > 0:
                    raise ValueError("keys {} are required but not set".format(required_but_not_set))
            
                # check if only required and optional keys are set
                unknown_keys = keys.difference(required_keys.union(optional_keys))
                if len(unknown_keys) > 0:
                    raise ValueError("keys {} are unknown".format(unknown_keys))

                # check if all values of the map have the correct type if they are set
                object_types = {
                    key: object_type
                    for key, object_type in required.union(optional)
                }
                for k, v in map.items():
                    if not isinstance(v, object_types[k]):
                        raise ValueError("expected object of type '{}' for element with key '{}'".format(object_types[k], k))

            return func(*args, **kwargs)

        return wrapper
    
    return inner_func


class ConfigManagerMeta(type):

    def __new__(meta_cls, class_name, bases, class_dict):
        # add instance attributes to ensure the thread-safe initialization of this class according to the singleton
        # pattern
        class_dict.setdefault("_instance", None)
        class_dict.setdefault("_lock", threading.Lock())

        # create the class
        cls = super().__new__(meta_cls, class_name, bases, class_dict)

        return cls
    
    def __call__(cls, *args, **kwargs):
        # create the instance if it has not been done before with double-checked locking
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class ConfigManager(metaclass=ConfigManagerMeta):

    def __init__(self):

        # index that holds the config objects
        self._configs = UniqueObjectIndex(Config)

        # lock for the duration a config is being loaded
        self._load_config_lock = threading.Lock()

    def instance(cls):
        if cls._instance is None:
            raise RuntimeError("ConfigManager has not been initialized")
        return cls._instance

    def has_config(self, obj: Any) -> bool:
        return self._configs.has(obj)
    
    def has_config_path(self, path: str) -> bool:
        return any([cfg.x.config_path == path for cfg in self._configs.values()])
    
    def get_config(self, obj: Any) -> Config:
        return self._configs.get(obj)

    def get_config_by_path(self, config_path: str) -> Config:
        for v in self._configs.values():
            if v.x.config_path == os.path.abspath(config_path):
                return v
        raise ValueError("config loaded from path {} not found".format(config_path))

    def add_config(self, *args, **kwargs) -> Config:
        return self._configs.add(*args, **kwargs)

    def load_config(self, config_path: str) -> Config:
        # load config object from the file with double-checked locking
        if not self.has_config_path(config_path):
            with self._load_config_lock:
                if not self.has_config_path(config_path):
                    config = self._read_config(config_path)
                    if not self.has_config(config.name):
                        return self.add_config(config)
                    return self.get_config(config.name)
                return self.get_config_by_path(config_path)
        return self.get_config_by_path(config_path)

    @property
    def config_path(self) -> str:
        return self._config_path

    @property
    def config(self) -> Config:
        return self._config

    def _read_config(self, config_path: str):
        # read information from the YAML configuration
        with open(config_path, mode="r") as f:
            cfg = yaml.safe_load(f)

        # load the campaign and create the config object
        cpn = self._create_campaign(cfg["campaign"])
        config = self._create_config(cfg["config"], cpn)

        # add the path of the original configuration file to the auxiliary data
        config.x.config_path = config_path

        # check if a config with the same name already exists
        if self.has_config(config.name):
            raise ValueError("config '{}' already exists".format(config.name))

        # add processes, datasets and channels to the config
        _ = self._create_processes(cfg["processes"], config)
        _ = self._create_datasets(cfg["datasets"], config)
        _ = self._create_channels(cfg["channels"], config)

        return config

    @check_keys(
        required={
            ("name", str),
        },
        optional={
            ("label", str),
            ("aux", dict),
        }
    )
    def _create_config(self, cfg_cfg: Dict[str, Any], cpn: Campaign) -> Config:
        # create the config given the information from the loaded configuration
        config = Config(name=cfg_cfg.pop("name"), id="+", campaign=cpn, aux=DotDict.wrap(cfg_cfg.get("aux", {})))
        return config

    @check_keys(
        required={
            ("name", str),
        },
        optional={
            ("aux", dict),
        }
    )
    def _create_campaign(self, cfg_cpn: Dict[str, Any]) -> Campaign:
        # create the campaign given the information from the loaded configuration
        cpn = Campaign(name=cfg_cpn["name"], id="+", aux=DotDict.wrap(cfg_cpn.get("aux", {})))
        return cpn

    def _create_processes(self, cfg_processes: List[Dict[str, Any]], config: Config) -> UniqueObjectIndex:
        # create processes given the information from the loaded configuration and add them to the config object
        for cfg_proc in cfg_processes:
            config.add_process(self._create_process(cfg_proc, config))
        return config.processes

    def _create_datasets(self, cfg_datasets: List[Dict[str, Any]], config: Config) -> UniqueObjectIndex:
        # create datasets given the information from the loaded configuration and add them to the config object
        for cfg_ds in cfg_datasets:
            config.add_dataset(self._create_dataset(cfg_ds, config))
        return config.datasets

    def _create_channels(self, cfg_channels: List[Dict[str, Any]], config: Config) -> UniqueObjectIndex:
        # create datasets given the information from the loaded configuration and add them to the config object
        for cfg_ch in cfg_channels:
            config.add_channel(self._create_channel(cfg_ch, config))
        return config.channels

    @check_keys(
        required={
            ("name", str),
        },
        optional={
            ("color1", list),
            ("color2", list),
            ("color3", list),
            ("label", str),
            ("processes", list),
            ("aux", dict),
        },
    )
    def _create_process(self, cfg_proc: Dict[str, Any], config: Config) -> Process:
        # create the process object
        process = Process(
            name=cfg_proc["name"],
            id="+",
            color1=cfg_proc.get("color1", None),
            color2=cfg_proc.get("color2", None),
            color3=cfg_proc.get("color3", None),
            label=cfg_proc.get("label", None),
        )

        # figure out if this process has children and add them recursively
        if "processes" in cfg_proc:
            for cfg_subproc in cfg_proc["processes"]:
                process.add_process(self._create_process(cfg_subproc, config))

        return process

    @check_keys(
        required={
            ("name", str),
            ("key", str),
        },
        optional={
            ("get_dataset_metadata_callable", str),
            ("process", str),
            ("is_data", bool),
            ("aux", dict),
        }
    )
    def _create_dataset(self, cfg_ds: Dict[str, Any], config: Config) -> UniqueObjectIndex:
        # try to get the process object matching the name in the dataset configuration
        process = None
        if "process" in cfg_ds:
            try:
                process = config.get_process(cfg_ds["process"], deep=True)
            except ValueError:
                raise ValueError("process '{}' not found in config '{}'".format(cfg_ds["process"], config.name))

        # get the function name, which should be used to retrieve the metadata of the dataset
        # if no function is declared, try to perform a DAS query
        get_dataset_metadata_callable = None
        get_dataset_metadata_callable_name = cfg_ds.pop("get_dataset_metadata_callable", None)
        if get_dataset_metadata_callable_name is None:
            get_dataset_metadata_callable = get_dataset_metadata.get_dataset_metadata_from_das
        else:
            get_dataset_metadata_callable = getattr(
                get_dataset_metadata,
                get_dataset_metadata_callable_name,
            )

        # create the prototype dataset object
        dataset = Dataset(
            name=cfg_ds["name"],
            id="+",
            processes=[process],
            keys=[cfg_ds["key"]],
            aux=DotDict.wrap(cfg_ds.get("aux", {})),
        )

        # add the path to the metadata cache file to the auxiliary data
        dataset.x.metadata_path = os.path.join(os.path.dirname(config.x.config_path), "datasets", "{}.yaml".format(dataset.name))

        # retrieve the metadata
        metadata = {}
        try:
            # first, try to get the cached metadata file
            metadata = get_dataset_metadata.get_dataset_metadata_from_yaml(dataset)

        except FileNotFoundError as _:
            # if the file is not found, try out to get the metadata with the declared function and dump it to a YAML file
            metadata = get_dataset_metadata_callable(dataset)

            # ensure that the parent directory exists
            parent_dir = os.path.dirname(dataset.x.metadata_path)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            with open(dataset.x.metadata_path, mode="w") as f:
                yaml.safe_dump(metadata, f)

        except Exception as e:
            # other exceptions than `FileNotFoundError` must be raised
            raise e

        # create a copy of the dataset, that also contains the pulled metadata values
        dataset.n_events = metadata.pop("n_events")
        dataset.n_files = metadata.pop("n_files")
        dataset.aux.update(metadata)

        return dataset

    @check_keys(
        required={
            ("name", str),
        },
        optional={
            ("label", str),
            ("aux", dict),
        },
    )
    def _create_channel(self, cfg_ch: Dict[str, Any], config: Config) -> Channel:
        # create the channel object
        channel = Channel(
            name=cfg_ch["name"],
            id="+",
            label=cfg_ch.get("label", None),
            aux=DotDict.wrap(cfg_ch.get("aux", {})),
        )

        return channel
