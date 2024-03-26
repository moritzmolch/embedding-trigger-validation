from functools import wraps
import json
from law.util import DotDict
from order import Campaign, Channel, Config, Dataset, Process, UniqueObjectIndex
import os
import subprocess
import threading
from typing import Any, Dict, Iterable, Mapping, List
import yaml


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

        metadata_file_path = os.path.join(os.path.dirname(config.x.config_path), "datasets", cfg_ds["name"] + ".yaml")
        
        if not os.path.exists(os.path.dirname(metadata_file_path)):
            os.makedirs(os.path.dirname(metadata_file_path))

        metadata = {}
        if not os.path.exists(metadata_file_path):
            # get key and DBS instance of the dataset to perform DAS queries
            metadata["key"] = cfg_ds["key"]
            metadata["dbs_instance"] = cfg_ds.setdefault("aux", {}).get("dbs_instance", "prod/global")

            # get the logical file names of the dataset
            metadata["lfns"] = self._get_das_dataset_lfns(metadata["key"], metadata["dbs_instance"])

            # get additional metadata
            metadata["dbs_id"], metadata["n_events"], metadata["n_files"] = self._get_das_dataset_metadata(metadata["key"], metadata["dbs_instance"])

            with open(metadata_file_path, mode="w") as f:
                yaml.safe_dump(metadata, f)

        else:
            with open(metadata_file_path, mode="r") as f:
                metadata = yaml.safe_load(f)

        # construct the dictionary with auxiliary information
        aux_dict = cfg_ds.get("aux", {})
        aux_dict.update({
            "dbs_instance": metadata["dbs_instance"],
            "dbs_id": metadata["dbs_id"],
            "lfns": metadata["lfns"],
        })

        # create the dataset object
        dataset = Dataset(
            name=cfg_ds["name"],
            id="+",
            keys=[metadata["key"]],
            n_events=metadata["n_events"],
            n_files=metadata["n_files"],
            is_data=cfg_ds.get("is_data", None),
            aux=DotDict.wrap(aux_dict),
        )

        # add the process if it has been specified
        if process is not None:
            dataset.add_process(process)

        return dataset

    def _get_das_dataset_lfns(self, key: str, dbs_instance: str) -> Dataset:
        # execute the DAS query
        p = subprocess.Popen(
            [
                "dasgoclient",
                "-query",
                "file dataset={} instance={}".format(key, dbs_instance),
                "-json",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            env=os.environ,
        )
        out, _ = p.communicate()
        if p.returncode != 0:
            raise OSError("dasgoclient: command failed with exit code {}".format(p.returncode))

        # decode and parse the JSON response
        response = json.loads(out.decode("utf-8"))

        # get the logical filenames from the response
        lfns = []
        for item in response:
            lfns.append(item["file"][0]["name"])

        return lfns

    def _get_das_dataset_metadata(self, key: str, dbs_instance: str) -> Dataset:
        # execute the DAS query
        p = subprocess.Popen(
            [
                "dasgoclient",
                "-query",
                "dataset={} instance={}".format(key, dbs_instance),
                "-json",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            env=os.environ,
        )
        out, _ = p.communicate()
        if p.returncode != 0:
            raise OSError("dasgoclient: command failed with exit code {}".format(p.returncode))

        # decode and parse the JSON response
        response = json.loads(out.decode("utf-8"))

        # get number of files, number of events and the dataset ID
        n_files = 0
        n_events = 0
        dbs_id = 0
        for item in response:
            if "dbs3:filesummaries" in item["das"]["services"]:
                n_files = item["dataset"][0]["nfiles"]
                n_events = item["dataset"][0]["nevents"]
            if "dbs3:dataset_info" in item["das"]["services"]:
                dbs_id = item["dataset"][0]["dataset_id"]

        return dbs_id, n_events, n_files

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
