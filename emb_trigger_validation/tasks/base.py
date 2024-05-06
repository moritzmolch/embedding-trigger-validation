from collections import OrderedDict
import law
import law.contrib.wlcg
from law.util import iter_chunks
from law import Config
import luigi
from order import Dataset, Process, UniqueObjectIndex
import os
from typing import List, Union

from emb_trigger_validation.paths import LOCAL_STORE_DIR, WLCG_STORE_DIR, CONFIG_FILE
from emb_trigger_validation.config import config_manager


class BaseTask(law.Task):

    exclude_params_req_get = {
        "branches",
        "workflow",
        "tolerance",
    }

    version = luigi.Parameter(
        default="v1",
        description=(
            "version identifier of the output, is used for separating outputs of different versions of this project; "
            "default: 'v1'"
        ),
    )

    local_store_path = luigi.PathParameter(
        exists=False,
        description="path to the local file store for task outputs; default: '{}".format(LOCAL_STORE_DIR),
        default=LOCAL_STORE_DIR,
    )

    wlcg_store_name = luigi.Parameter(
        description=(
            "name of the remote file system for task outputs; corresponds to the name of a WLCG file system defined "
            "in law.cfg; default: '{}'".format(Config().instance().get("target", "default_wlcg_fs"))
        ),
        default=Config().instance().get("target", "default_wlcg_fs"),
    )

    wlcg_store_path = luigi.PathParameter(
        exists=False,
        description=(
            "path to the remote file store for task outputs; the path is relative to the base URI of the respective "
            "file system; default: '{}'".format(WLCG_STORE_DIR)
        ),
        default=WLCG_STORE_DIR,
    )

    _wlcg_stores = {}
    _wlcg_redirectors = {}

    @classmethod
    def get_remote_file_system(cls, name: str) -> law.contrib.wlcg.WLCGFileSystem:
        if name not in cls._wlcg_stores:
            if not law.Config.instance().has_section(name):
                raise RuntimeError("no config section for remote file system '{}' found".format(name))
            cls._wlcg_stores[name] = law.contrib.wlcg.WLCGFileSystem(name)
        return cls._wlcg_stores[name]

    def path(self, store, *parts, **kwargs) -> str:
        return os.path.join(store, self.version, self.__class__.__name__, *parts)

    def local_target(self, *parts, **kwargs):
        store = kwargs.pop("store", None) or self.local_store_path
        target_class = law.LocalDirectoryTarget if kwargs.pop("is_dir", False) else law.LocalFileTarget
        return target_class(self.path(store, *parts, **kwargs))

    def remote_target(self, *parts, **kwargs):
        fs_name = kwargs.pop("fs_name", None) or self.wlcg_store_name
        fs = self.__class__.get_remote_file_system(fs_name)
        store = kwargs.pop("store", None) or self.wlcg_store_path
        target_class = law.contrib.wlcg.WLCGDirectoryTarget if kwargs.pop("is_dir", False) else law.contrib.wlcg.WLCGFileTarget
        return target_class(self.path(store, *parts, **kwargs), fs=fs)


class CampaignConfigTask(BaseTask):

    campaign = luigi.Parameter(
        description=(
            "name of a campaign, e.g. for a specific data-taking era; choose a name of one of the campaigns defined "
            "in the config file {}".format(CONFIG_FILE)
        ),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._campaign_config = config_manager.get_campaign_config(self.campaign)

    def get_campaign_config(self) -> Config:
        return self._campaign_config

    def path(self, store, *parts, **kwargs) -> str:
        return os.path.join(store, self.version, self.__class__.__name__, self.get_campaign_config().name, *parts)


class DatasetTask(CampaignConfigTask):

    dataset = luigi.Parameter(
        description=(
            "name of the dataset within the declared campaign",
        ),
    )

    file_group_size = luigi.IntParameter(
        default=1,
        description=(
            "number of input files that are merged together into one task to produce one output file;"
            " default: '1'"
        ),
    )

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)
        self._dataset = self.get_campaign_config().get_dataset(self.dataset)

    def get_dataset(self):
        return self._dataset

    def create_branch_map(self):
        file_index_chunks = iter_chunks(range(self.get_dataset().n_files), size=self.file_group_size)
        return OrderedDict({
            i: {
                "file_index": file_index,
            }
            for i, file_index in enumerate(file_index_chunks)
        })

    def path(self, store, *parts, **kwargs) -> str:
        return os.path.join(store, self.__class__.__name__, self.get_campaign_config().name, self.get_dataset().name, self.version, *parts)


class ProcessCollectionTask(CampaignConfigTask):

    processes = law.CSVParameter(
        description=(
            "selection of the processes for processing datasets collectively; only datasets with a process which "
            "is a child of a given process are taken into account for triggering dependencies"
        ),
    )

    def __init__(self, *args, **kwargs):
        super(ProcessCollectionTask, self).__init__(*args, **kwargs)
        self._processes = UniqueObjectIndex(
            Process,
            [
                self.get_campaign_config().get_process(process)
                for process in self.processes
            ],
        )

    def get_processes(self) -> Union[UniqueObjectIndex, List[Process]]:
        return self._processes

    def get_datasets_with_process(self, process: Process) -> Union[UniqueObjectIndex, List[Dataset]]:
        datasets = []
        for dataset in self.get_campaign_config().datasets.values():
            if len(dataset.processes) == 0:
                continue
            dataset_process = dataset.processes.get_first()
            if dataset_process == process or dataset_process.has_parent_process(process):
                datasets.append(dataset)
        return UniqueObjectIndex(Dataset, datasets)
