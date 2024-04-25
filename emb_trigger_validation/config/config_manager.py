# coding: utf-8

"""
Load YAML configuration and materialize it into configuration objects of classes from the :py:package:`order` package.
"""

from law.util import merge_dicts
import os
from order import Channel, Config, Dataset, Process, UniqueObjectIndex
from typing import Any, Dict, List, Optional
import yaml

from emb_trigger_validation.config.metadata_fetcher import BaseMetadataFetcher
from emb_trigger_validation.config.util import check_keys, create_campaign, create_channel, create_dataset, create_process
from emb_trigger_validation.paths import CONFIG_STORE_DIR


class ConfigManager():

    def __init__(self, config_file):
        self._config_file = os.path.abspath(config_file)
        self._campaign_configs = self._load_campaign_configs(self._config_file)

    def get_campaign_config(self, name: str) -> Config:
        for campaign_config in self._campaign_configs:
            if campaign_config.name == name:
                return campaign_config
        raise ValueError("campaign with name '{}' not found".format(name))

    def _get_config_cache_path(self) -> str:
        return CONFIG_STORE_DIR

    def _sanitize_paths(self, config_paths) -> List[str]:
        base_path = os.path.dirname(self._config_file)
        return [
            p if os.path.isabs(p) else os.path.join(base_path, p)
            for p in config_paths
        ]

    def _load_campaign_configs(self, config_file: str):
        # create the container for the campaign configs
        configs = []

        # load the YAML config
        with open(config_file, mode="r") as f:
            cfg_dict = yaml.safe_load(f)

        # get defaults
        default = cfg_dict.get("default", {})

        # load configuration of each campaign and merge it with default settings
        for cfg_item in cfg_dict["campaigns"]:

            # merge config of this campaign with default values
            # check the keys and values of campaign config dictionary
            # update the ID argument: use automatic increment
            campaign_dict = merge_dicts(default, cfg_item, deep=True)

            # obtain the configuration files for the processes, datasets and channels
            process_config_files = self._sanitize_paths(campaign_dict.pop("process_config_files", []))
            dataset_config_files = self._sanitize_paths(campaign_dict.pop("dataset_config_files", []))
            channel_config_files = self._sanitize_paths(campaign_dict.pop("channel_config_files", []))

            # check the keys and use automatic ID increment
            check_keys(
                campaign_dict,
                required={
                    ("name", str),
                    ("label", str),
                    ("cmssw", dict),
                },
                optional={
                    ("aux", dict),
                }
            )
            campaign_dict.update({"id": "+"})

            # load the processes from the declared configuration files
            processes = []
            for process_config_file in process_config_files:
                processes.extend(self._load_processes(process_config_file))
            processes = UniqueObjectIndex(Process, processes)

            # load the datasets from the declared configuration files
            datasets = []
            for dataset_config_file in dataset_config_files:
                datasets.extend(self._load_datasets(dataset_config_file, processes))
            datasets = UniqueObjectIndex(Dataset, datasets)

            # load the channels from the declared configuration files
            channels = []
            for channel_config_file in channel_config_files:
                channels.extend(self._load_channels(channel_config_file))
            channels = UniqueObjectIndex(Channel, channels)

            # create the campaign
            campaign = create_campaign(**campaign_dict, datasets=datasets)

            # create the config related to the campaign and append it to the config container
            config = Config(
                name=campaign.name,
                id="+",
                campaign=campaign,
                datasets=datasets,
                processes=processes,
                channels=channels,
            )
            configs.append(config)

        return configs

    def _load_datasets(self, config_file: str, processes: UniqueObjectIndex):
        # create the container for the datasets
        datasets = []

        # load the YAML config
        with open(config_file, mode="r") as f:
            cfg_dict = yaml.safe_load(f)

        # get defaults
        default = cfg_dict.get("default", {})

        for cfg_item in cfg_dict["datasets"]:

            # merge config of this dataset with default values
            dataset_dict = merge_dicts(default, cfg_item, deep=True)

            # check the required and optional keys and values of the dictionary
            check_keys(
                dataset_dict,
                required={
                    ("name", str),
                    ("process", str),
                    ("metadata_fetcher", dict),
                },
                optional={
                    ("is_data", bool),
                    ("tags", list),
                    ("aux", dict),
                },
            )

            # get the metadata fetcher config, check the keys
            metadata_fetcher_dict = dataset_dict.pop("metadata_fetcher")
            check_keys(
                metadata_fetcher_dict,
                required={
                    ("name", str),
                    ("kwargs", dict),
                },
            )

            # get the metadata
            name, kwargs = metadata_fetcher_dict["name"], metadata_fetcher_dict["kwargs"]
            fetcher = BaseMetadataFetcher.get_cls(name)(**kwargs)
            metadata = fetcher.get()

            # update the dataset dictionary
            dataset_dict.update(metadata)

            # get the process object
            process_name = dataset_dict["process"]
            dataset_dict.update({"process": processes.get(process_name)})

            # finally, check the keys of the dataset dict
            check_keys(
                dataset_dict,
                required={
                    ("name", str),
                    ("process", Process),
                    ("n_events", int),
                    ("n_files", int),
                    ("redirectors", list),
                    ("filelist", list),
                },
                optional={
                    ("is_data", bool),
                    ("tags", list),
                    ("aux", dict),
                },
            )

            # update the ID argument: use automatic increment
            dataset_dict.update(dict(id="+"))

            # create the dataset and add it to the collection
            datasets.append(create_dataset(**dataset_dict))

        return datasets

    def _load_processes(self, config_file: str) -> UniqueObjectIndex:
        # create the container for the processes
        processes = []

        # load the YAML config
        with open(config_file, mode="r") as f:
            cfg_dict = yaml.safe_load(f)

        # get defaults
        default = cfg_dict.get("default", {})

        for cfg_item in cfg_dict["processes"]:

            # merge config of this process with default values
            process_dict = merge_dicts(default, cfg_item, deep=True)

            # check the required and optional keys and values of the dictionary
            check_keys(
                process_dict,
                required={
                    ("name", str),
                    ("label", str),
                },
                optional={
                    ("color", list),
                },
            )

            # update the ID argument: use automatic increment
            process_dict.update({"id": "+"})

            # create the process and append it to the container
            processes.append(create_process(**process_dict))

        return processes

    def _load_channels(self, config_file: str) -> List[Channel]:
        # create the container for the channels
        channels = []

        # load the YAML config
        with open(config_file, mode="r") as f:
            cfg_dict = yaml.safe_load(f)

        # get defaults
        default = cfg_dict.get("default", {})

        for cfg_item in cfg_dict["channels"]:

            # merge config of this channel with default values
            channel_dict = merge_dicts(default, cfg_item, deep=True)

            # check the required and optional keys and values of the dictionary
            check_keys(
                channel_dict,
                required={
                    ("name", str),
                    ("label", str),
                    ("triggers", list),
                },
                optional={
                    ("aux", dict),
                },
            )

            # update the ID argument: use automatic increment
            channel_dict.update({"id": "+"})

            # create the channel and append it to the container
            channels.append(create_channel(**channel_dict))

        return channels
