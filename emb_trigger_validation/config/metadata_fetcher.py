# coding: utf-8

"""
Custom providers to retrieve the metadata of a dataset from different sources (YAML files, DAS and
gc_configs_for_embedding filelists).
"""

from abc import ABCMeta, abstractmethod
from configparser import ConfigParser
import glob
import json
from law.util import create_hash
#from order import Config, Dataset
import os
import tempfile
from typing import Any, Dict
import subprocess
import yaml

from emb_trigger_validation.paths import CONFIG_DATA_DIR


class MetadataFetcherMeta(ABCMeta):

    fetchers = {}

    def __new__(meta_cls, class_name, bases, class_dict):
        # create the class
        cls = super().__new__(meta_cls, class_name, bases, class_dict)

        # get the name of the metadata fetcher and register it
        name = cls.__name__
        if name in meta_cls.fetchers:
            raise ValueError("metadata fetcher '{}' has previously been registered".format(name))
        meta_cls.fetchers[name] = cls

    @classmethod
    def get_cls(meta_cls, name: str):
         if name in meta_cls.fetchers:
            raise ValueError("metadata fetcher '{}' does not exist".format(name))       


class MetadataStore():

    __shared_state = {}

    def __init__(self):
        super().__init__()

        # get the shared state
        self.__dict__ = self.__class__.__shared_state

        # initialize the persistent store
        self._config_data_dir = CONFIG_DATA_DIR
        if not os.path.exists(self._config_data_dir):
            os.makedirs(self._config_data_dir) 

        # initialize the store object
        if not hasattr(self, "_store"):
            self._store = self._init_store(self._config_data_dir)

    def _init_store(self, config_data_dir: str):
        store = {
            os.path.splitext(os.path.basename(store_file))[0]: (store_file, None)
            for store_file in glob.glob(config_data_dir, "*.json")
        }
        return store

    def _create_path(self, key):
        return os.path.join(self._config_data_dir, key + ".json")

    def _load(self, path):
        with open(path, mode="r") as f:
            obj = json.load(f)
        return obj

    def _dump(self, path, obj):
        with open(path, mode="w") as f:
            json.dump(obj, f)

    def has_key(self, key):
        return key in self._store

    def get(self, key):
        if not self.has_key(key):
            raise ValueError("object with key '{}' does not exist".format(key))
        
        # get path to the object file as well as the object itself
        path, obj = self._store[key]

        # load the object from the file if it has not been loaded before, update the store afterwards
        if obj is None:
            obj = self._load(path)
        self._store[key] = (path, obj)

        return obj

    def put(self, key, obj):
        if not self.has_key(key):
            raise ValueError("object with key '{}' already exists".format(key))

        # create a path for the new store item
        path = self._create_path(key)

        # dump the serialized object into a persistent file
        self._dump(path, obj)

        # update the store
        self._store[key] = (path, obj)

        return obj


class BaseMetadataFetcher(metaclass=MetadataFetcherMeta):

    _store = MetadataStore()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._kwargs = kwargs
        self._instance_key = self.create_instance_hash(self._kwargs)

    def create_instance_hash(self, kwargs: Dict[str, Any]):
        # create a hash for the metadata fetcher in combination with the used keyword arguments
        
        # convert the dictionary of keyword arguments into a sorted list of tuples
        kwargs_tuples = []
        for k in sorted(kwargs):
            v = kwargs[k]
            kwargs_tuples.append((k, self.create_instance_hash(v) if isinstance(v, dict) else v))
        
        # concatenate the class name with the keyword arguments and create the hasn
        instance_hash = create_hash(tuple([self.__class__.__name__] + kwargs_tuples))

        return instance_hash

    def get(self):
        # get the metadata store
        store = MetadataStore()

        # if the query has previously been performed, just load the metadata from the store
        if store.has_key(self._instance_key):
            return store.get(self._instance_key)
        
        # if the data is not available yet, fetch it from the source and put it into the store
        result = self.fetch(**self._kwargs)
        store.put(self._instance_key, result)

        return result

    @abstractmethod
    def fetch(self, /, **kwargs):
        return NotImplemented


class YAMLMetadataFetcher(BaseMetadataFetcher):

    def fetch(self, /, file_path: str) -> Dict[str, Any]:
        """
        Retrieve the metadata of a dataset from a YAML file.

        This function is mainly used for loading metadata, which has been downloaded before, again.

        The method receives a parameter `file_path`, which points to the location of the existing YAML file with the metadata.

        This function can also be used to provide metadata for custom datasets. The YAML file then has to be prepared
        beforehand.

        The function returns a dictionary with four items, which should of course also be used when preparing the YAML file
        of a custom dataset:

        - `filelist`: list of the logical filenames to access the individual files of the dataset

        - `n_events`: total number of events in the dataset, accumulated over all files

        - `n_files`: number of individual files, of which the dataset consists

        - `redirectors`: list of redirectors, from which the dataset files are accessible. The list is ordered by the
          priority of the redirectors, which means, that the first redirector is checked out first.

        :param file_path: path to the YAML file with metadata
        :type file_path: str

        :return: pulled metadata information for the dataset
        :rtype:  Dict[str, Any]
        """

        # load the file content
        with open(file_path, mode="r") as f:
            metadata = yaml.safe_load(f)

        return metadata


class DASMetadataFetcher(BaseMetadataFetcher):

    def fetch(self, /, dbs_key: str, dbs_instance: str = "prod/global"):
        """
        Retrieve the metadata of a dataset, which is registered at the CMS Data Aggregation System (DAS).

        The method receives two arguments: The `dbs_key` of the dataset as well as the `dbs_instance`, at which the
        dataset information is available.

        This function should be used to fetch the metadata for all official CMS datasets, which are registered at the DAS.

        The function returns a dictionary with four items:

        - `filelist`: list of the logical filenames to access the individual files of the dataset

        - `n_events`: total number of events in the dataset, accumulated over all files

        - `n_files`: number of individual files, of which the dataset consists

        - `redirectors`: list of redirectors, from which the dataset files are accessible. The list is ordered by the
          priority of the redirectors, which means, that the first redirector is checked out first. The default is:

          ```python
          [
            "root://xrootd-cms.infn.it//",
            "root://xrd-global.cern.ch//",
          ]
          ```

        :param dbs_key: key for querying the dataset
        :type dbs_key: str

        :param dbs_instance: name of the DBS instance, where the dataset information can be found; default: *prod/global*
        :type dbs_instance: str

        :return: pulled metadata information for the dataset
        :rtype:  Dict[str, Any]
        """

        # execute the DAS query
        p = subprocess.Popen(
            [
                "dasgoclient",
                "-query",
                "file dataset={} instance={}".format(dbs_key, dbs_instance),
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
        filelist = []
        for item in response:
            filelist.append(item["file"][0]["name"])

        # sort the filelist to make the outcome reproducible
        filelist.sort()

        # execute the DAS query
        p = subprocess.Popen(
            [
                "dasgoclient",
                "-query",
                "dataset={} instance={}".format(dbs_key, dbs_instance),
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
        for item in response:
            if "dbs3:filesummaries" in item["das"]["services"]:
                n_files = int(item["dataset"][0]["nfiles"])
                n_events = int(item["dataset"][0]["nevents"])

        # also add the possible redirectors for this dataset, ordered by their priority
        redirectors = [
            "root://xrootd-cms.infn.it//",
            "root://xrd-global.cern.ch//",
        ]

        return {
            "filelist": filelist,
            "n_events": n_events,
            "n_files": n_files,
            "redirectors": redirectors,
        }


class GCConfigsMetadataFetcher(BaseMetadataFetcher):

    def fetch(self, /, rel_dbs_file_path: str) -> Dict[str, Any]:
        """
        Custom function to retrieve dataset metadata for embedding samples from files in the repository
        https://github.com/KIT-CMS/gc_configs_for_embedding.

        The method receives one argument, which is the path to the `*.dbs` file for the dataset relative to the root
        directory of the *gc_configs_for_embedding* repository, e.g. *dbs/ul_embedding_rerun_puppi/Run2017B_ElTau.dbs* for
        the embedding dataset produced with data events from the 2017B run and with generator-level tau pair decays into one
        electron and one hadronic tau.

        The function returns a dictionary with four items:

        - `filelist`: list of the logical filenames to access the individual files of the dataset

        - `n_events`: total number of events in the dataset, accumulated over all files

        - `n_files`: number of individual files, of which the dataset consists

        - `redirectors`: list of redirectors, from which the dataset files are accessible. The list is ordered by the
          priority of the redirectors, which means, that the first redirector is checked out first. As this is mostly used
          for private productions at KIT, the redirectors list is here fixed to only contain the GridKA redirector:

          ```python
          [
            "root://cmsxrootd-kit-disk.gridka.de/",
          ]
          ```

        :param rel_dbs_file_path: path to the DBS file with the dataset file names, relative to the root of the repository
        :type rel_dbs_file_path: str

        :return: pulled metadata information for the dataset
        :rtype:  Dict[str, Any]
        """

        # clone the repository in a temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            gc_configs_path = os.path.join(tmpdir, "gc_configs_for_embedding")
            p = subprocess.Popen(
                [
                    "git",
                    "clone",
                    "https://github.com/KIT-CMS/gc_configs_for_embedding",
                    gc_configs_path,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                env=os.environ,
                cwd=tmpdir,
            )
            _, _ = p.communicate()

            if p.returncode != 0:
                raise OSError("git clone: command failed with exit code {}".format(p.returncode))

            # construct the absolute filelist path and check if it is present
            dbs_filelist_path = os.path.join(gc_configs_path, rel_dbs_file_path)
            if not os.path.exists(dbs_filelist_path):
                raise FileNotFoundError("file {} not found inside repository {}".format(rel_dbs_file_path, gc_configs_path))

            # fetch the metadata
            metadata = self._read_metadata_from_dbs_filelist(dbs_filelist_path)

        return metadata

    def _read_metadata_from_dbs_filelist(self, dbs_filelist_path: str) -> Dict[str, Any]:
        # load the file content
        parser = ConfigParser()
        parser.read(dbs_filelist_path)
        section = parser.sections()[0]

        # the prefix is the first part of the LFN and keys, which are the 
        lfn_base = parser.get(section, "prefix")

        # all entries, that end with '.root', are the filenames of the dataset; the value is the number of events contained in that file
        filelist = []
        for key, _ in parser.items(section):
            if key.endswith(".root"):
                filelist.append(os.path.join(lfn_base, key))

        # sort the filelist to make the outcome reproducible
        filelist.sort()

        # get the number of events and the number of files
        n_events = int(parser.get(section, "events"))
        n_files = len(filelist)

        # fix the redirector to GridKA as this is a private production
        redirectors = [
            "root://cmsxrootd-kit-disk.gridka.de/",
        ]

        return {
            "filelist": filelist,
            "n_events": n_events,
            "n_files": n_files,
            "redirectors": redirectors,
        }
