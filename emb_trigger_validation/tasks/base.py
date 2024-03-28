from collections import OrderedDict
import law
import law.contrib.wlcg
from law.util import iter_chunks
import luigi
from order import Config, Dataset, Process, UniqueObjectIndex
import os

from emb_trigger_validation.config.config_manager import ConfigManager


class BaseTask(law.Task):

    version = luigi.Parameter(
        default="v1",
        description=(
            "version identifier of the output, is used for separating outputs of different versions of this project; "
            "default: 'v1'"
        ),
    )

    local_store_path = luigi.PathParameter(
        exists=False,
        description="path to the local file store for task outputs; default: '{}".format(os.environ["ETV_LOCAL_STORE_PATH"]),
        default=os.environ["ETV_LOCAL_STORE_PATH"],
    )

    wlcg_store_name = luigi.Parameter(
        description=(
            "name of the remote file system for task outputs; corresponds to the name of a WLCG file system defined "
            "in law.cfg; default: '{}'".format(os.environ["ETV_WLCG_STORE_NAME"])
        ),
        default=os.environ["ETV_WLCG_STORE_NAME"],
    )

    wlcg_store_path = luigi.PathParameter(
        exists=False,
        description=(
            "path to the remote file store for task outputs; the path is relative to the base URI of the respective "
            "file system; default: '{}'".format(os.environ["ETV_WLCG_STORE_PATH"])
        ),
        default=os.environ["ETV_WLCG_STORE_PATH"],
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


class ConfigTask(BaseTask):

    config = luigi.Parameter(
        description=(
            "name of the config for a specific data-taking era; the name corresponds to the name of a directory in "
            "'config/analysis', e.g. 'ul17_miniaod'"
        ),
    )

    def __init__(self, *args, **kwargs):
        super(ConfigTask, self).__init__(*args, **kwargs)
        self.config_inst = self.get_config(self.config)

    @staticmethod
    def get_config(name: str) -> Config:
        return ConfigManager().load_config(os.path.join(os.environ["ETV_CONFIG_PATH"], "analysis", name, "config.yaml"))

    def path(self, store, *parts, **kwargs) -> str:
        return os.path.join(store, self.version, self.__class__.__name__, self.config_inst.name, *parts)


class DatasetTask(ConfigTask):

    dataset = luigi.Parameter(
        description=(
            "name of the dataset; the available dataset names for a configuration are defined in the 'datasets' "
            "section in 'config/analysis/<NAME OF THE CONFIG>/config.yaml"
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
        self.dataset_inst = self.config_inst.get_dataset(self.dataset)

    def create_branch_map(self):
        file_index_chunks = iter_chunks(range(self.dataset_inst.n_files), size=self.file_group_size)
        return OrderedDict({
            i: {
                "file_index": file_index,
            }
            for i, file_index in enumerate(file_index_chunks)
        })

    def path(self, store, *parts, **kwargs) -> str:
        return os.path.join(store, self.version, self.__class__.__name__, self.config_inst.name, self.dataset_inst.name, *parts)


class RootProcessesTask(ConfigTask):

    root_processes = law.CSVParameter(
        description=(
            "selection of the root processes for processing datasets collectively; only datasets with a process which "
            "is a child of a given root process are taken into account for triggering dependencies"
        ),
    )

    def __init__(self, *args, **kwargs):
        super(RootProcessesTask, self).__init__(*args, **kwargs)
        self.process_insts = UniqueObjectIndex(
            Process,
            [
                self.config_inst.get_process(process)
                for process in self.root_processes
            ],
        )

    def get_datasets_from_root_processes(self) -> UniqueObjectIndex:
        datasets = []
        for process in self.process_insts.values():
            for dataset in self.config_inst.datasets.values():
                if len(dataset.processes) == 0:
                    continue
                dataset_root_process = dataset.processes.get_first().get_root_processes()[0]
                if dataset_root_process == process:
                    datasets.append(dataset)
        return UniqueObjectIndex(Dataset, datasets)
