# coding: utf-8

"""
Custom functions to retrieve the metadata of a dataset for different scenarios (YAML files, DAS and
gc_configs_for_embedding filelists)
"""

from configparser import ConfigParser
import json
from order import Config, Dataset
import os
import tempfile
from typing import Any, Dict
import subprocess
import yaml


def get_dataset_metadata_from_yaml(dataset: Dataset) -> Dict[str, Any]:
    """
    Retrieve the metadata of a dataset from a YAML file.

    This function is mainly used for loading metadata which has been downloaded once again.

    The parameter `dataset` is a configuration object. The auxiliary data of this object has to contain an entry
    `metadata_path`, which points to the location of the existing YAML file with the metadata.

    This function can also be used to provide metadata for custom datasets. The YAML file then has to be prepared
    beforehand.

    The function returns a dictionary with four items, which should of course also be used when preparing the YAML file
    of a custom dataset:

    - `lfns`: list of the logical filenames to access the individual files of the dataset

    - `n_events`: total number of events in the dataset, accumulated over all files

    - `n_files`: number of individual files, of which the dataset consists

    - `redirectors`: list of redirectors, from which the dataset files are accessible. The list is ordered by the
      priority of the redirectors, which means, that the first redirector is checked out first.

    :param dataset: configuration object of the dataset
    :type dataset: Dataset

    :return: pulled metadata information for the dataset
    :rtype:  Dict[str, Any]
    """

    # get the metadata file path and check if it is present
    if not dataset.has_aux("metadata_path"):
        raise ValueError("dataset '{}' does not provide a value for 'metadata_path' in the member 'aux'".format(dataset.name))

    if not os.path.exists(dataset.x.metadata_path):
        raise FileNotFoundError("file {} not found".format(dataset.x.metadata_path))
    
    # load the file content
    with open(dataset.x.metadata_path, mode="r") as f:
        metadata = yaml.safe_load(f)

    return metadata


def get_dataset_metadata_from_das(dataset: Dataset, config: Config) -> Dict[str, Any]:
    """
    Retrieve the metadata of a dataset, which is registered at the CMS Data Aggregation System (DAS).

    The parameter `dataset` is a configuration object, which must hold exactly one value in the member `keys`, which
    is a list. This key is the query key, which is used to retrieve the dataset metadata from the DAS.

    If the dataset is only accessible at a certain DBS instance, the auxiliary data of `dataset` has to contain a key
    `dbs_instance`, which sets the specific value of the custom instance. Otherwise, the default instance `prod/global`
    is chosen for the query.

    This function should be used to fetch the metadata for all official CMS datasets, which are registered at the DAS.

    The function returns a dictionary with four items:

    - `lfns`: list of the logical filenames to access the individual files of the dataset

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

    :param dataset: configuration object of the dataset
    :type dataset: Dataset

    :return: pulled metadata information for the dataset
    :rtype:  Dict[str, Any]
    """
    # check if a key is provided
    if len(dataset.keys) == 0:
        raise ValueError("dataset '{}' does not provide any DAS dataset key, member 'keys' is empty".format(dataset.name))
    elif len(dataset.keys) >= 2:
        raise ValueError("dataset '{}' provides too many DAS dataset keys, member 'keys' has length {}".format(dataset.name, len(dataset.keys)))
    key = dataset.keys[0]

    # get the DBS instance; if no value is set in the 'auxiliary data member of the dataset, 'prod/global' is selected
    dbs_instance = dataset.x.get_aux("dbs_instance", "prod/global")

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
    for item in response:
        if "dbs3:filesummaries" in item["das"]["services"]:
            n_files = item["dataset"][0]["nfiles"]
            n_events = item["dataset"][0]["nevents"]

    # also add the possible redirectors for this dataset, ordered by their priority
    redirectors = [
        "root://xrootd-cms.infn.it//",
        "root://xrd-global.cern.ch//",
    ]

    return {
        "lfns": lfns,
        "n_events": n_events,
        "n_files": n_files,
        "redirectors": redirectors,
    }


def get_dataset_metadata_from_gc_configs(dataset: Dataset, config: Config):
    """
    Custom function to retrieve dataset metadata for embedding samples from files in the repository
    https://github.com/KIT-CMS/gc_configs_for_embedding.

    The parameter `dataset` is a configuration object, which must hold exactly one value in the member `keys`, which
    is a list. This key is interpreted as the path to the corresponding `*.dbs` file relative to the top-level
    directory of the *gc_configs_for_embedding* repository, e.g. *dbs/ul_embedding_rerun_puppi/Run2017B_ElTau.dbs* for
    the embedding dataset produced with data events from the 2017B run and with generator-level tau pair decays into one
    electron and one muon.

    Note that this function is only applicable to `dataset` objects, which have the entry `is_emb` set to `True` in the
    auxiliary data.

    The function returns a dictionary with four items:

    - `lfns`: list of the logical filenames to access the individual files of the dataset

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

    :param dataset: configuration object of the dataset
    :type dataset: Dataset

    :return: pulled metadata information for the dataset
    :rtype:  Dict[str, Any]
    """
    # if this is dataset not embedding, raise an exception
    if not dataset.x.get_aux("is_emb", False):
        raise ValueError("get_dataset_metadata_from_gc_configs is only useable for embedding samples")

    # check if a key is provided
    if len(dataset.keys) == 0:
        raise ValueError("dataset '{}' does not provide any DAS dataset key, member 'keys' is empty".format(dataset.name))
    elif len(dataset.keys) >= 2:
        raise ValueError("dataset '{}' provides too many DAS dataset keys, member 'keys' has length {}".format(dataset.name, len(dataset.keys)))
    key = dataset.keys[0]

    # clone the repository in a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        gc_configs_path = os.path.join(tmpdir, "gc_configs_for_embedding"),
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
        dbs_filelist_path = os.path.join(gc_configs_path, key)
        if not os.path.exists(dbs_filelist_path):
            raise FileNotFoundError("file {} not found inside repository {}".format(dataset.keys[0], gc_configs_path))

        # fetch the metadata
        metadata = _read_metadata_from_dbs_filelist(dbs_filelist_path)

    return metadata


def _read_metadata_from_dbs_filelist(dbs_filelist_path: str) -> Dict[str, Any]:
    # load the file content
    parser = ConfigParser()
    parser.read(dbs_filelist_path)
    section = parser.sections()[0]

    # the prefix is the first part of the LFN and keys, which are the 
    lfn_base = parser.get(section, "prefix")

    # all entries, that end with '.root', are the filenames of the dataset; the value is the number of events contained in that file
    lfns = []
    for key, _ in parser.items(section):
        if key.endswith(".root"):
            lfns.append(os.path.join(lfn_base, key))

    # get the number of events and the number of files
    n_events = parser.get(section, "events")
    n_files = len(lfns)

    # fix the redirector to GridKA as this is a private production
    redirectors = [
        "root://cmsxrootd-kit-disk.gridka.de/",
    ]

    return {
        "lfns": lfns,
        "n_events": n_events,
        "n_files": n_files,
        "redirectors": redirectors,
    }
