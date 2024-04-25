import law
import law.contrib.cms
import law.contrib.git
import law.contrib.tasks
from law.util import human_bytes
import luigi
import os

from emb_trigger_validation.tasks.base import BaseTask
from emb_trigger_validation.paths import BASE_DIR, CONFIG_DATA_DIR


class BundleRepository(
    BaseTask,
    law.contrib.git.BundleGitRepository,
    law.contrib.tasks.TransferLocalFile,
):

    replicas = luigi.IntParameter(
        description="number of replica archives to generate; default: 1",
        default=1,
    )

    task_namespace = None

    exclude_files = [
        ".law",
        ".vscode",
        "cache",
        "jobs",
        "store",
        "software",
    ]

    def get_repo_path(self):
        return BASE_DIR

    def single_output(self):
        return self.remote_target(
            "{}.{}.tgz".format(os.path.basename(self.get_repo_path()), self.checksum),
        )

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.contrib.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):
        # bundle repository
        bundle = law.LocalFileTarget(is_tmp="tgz", tmp_dir=os.environ["TMPDIR"])
        self.bundle(bundle)

        # log the size
        self.publish_message(
            "bundled repository archive, size is {:.2f} {}".format(
                *human_bytes(bundle.stat().st_size)
            )
        )

        # transfer replica archives
        self.transfer(bundle)


class BundleConfigData(BaseTask, law.contrib.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        description="number of replica archives to generate; default: 1",
        default=1,
    )

    def get_config_data_path(self):
        return CONFIG_DATA_DIR

    def single_output(self):
        return self.remote_target(
            "{}.tgz".format(os.path.basename(self.get_config_data_path())),
        )

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.contrib.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):
        # bundle repository
        bundle = law.LocalFileTarget(is_tmp="tgz", tmp_dir=os.environ["TMPDIR"])
        config_data_target = law.LocalDirectoryTarget(self.get_config_data_path())

        with self.publish_step("bundle config data directory ..."):
            bundle.dump(config_data_target, formatter="tar")

        # log the size
        self.publish_message(
            "bundled repository archive, size is {:.2f} {}".format(
                *human_bytes(bundle.stat().st_size)
            )
        )

        # transfer replica archives
        self.transfer(bundle)


class BundleCMSSW(BaseTask, law.contrib.cms.BundleCMSSW, law.contrib.tasks.TransferLocalFile):

    task_namespace = None

    replicas = luigi.IntParameter(
        description="number of replica archives to generate; default: 1",
        default=1,
    )

    cmssw_path = luigi.Parameter(
        description="path to the CMSSW environment, which is going to be bundled",
    )

    def get_cmssw_path(self):
        return os.path.abspath(self.cmssw_path)

    def single_output(self):
        return self.remote_target("{}.{}.tgz".format(os.path.basename(os.path.dirname(self.get_cmssw_path())), self.checksum))

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.contrib.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):
        # bundle repository
        bundle = law.LocalFileTarget(is_tmp="tgz", tmp_dir=os.environ["TMPDIR"])
        self.bundle(bundle.path)

        # log the size
        self.publish_message(
            "bundled cmssw archive, size is {:.2f} {}".format(
                *human_bytes(bundle.stat().st_size)
            )
        )

        # transfer replica archives
        self.transfer(bundle)
