import law
from law.util import create_hash
import luigi
import os
from order import Dataset, Process
from typing import List, Union

from emb_trigger_validation.tasks.base import ConfigTask, DatasetTask
from emb_trigger_validation.tasks.cmssw import CMSSWCommandTask
from emb_trigger_validation.tasks.remote import BaseHTCondorWorkflow
from emb_trigger_validation.tasks.bundle import BundleCMSSW


class ProduceTauTriggerNtuples(CMSSWCommandTask, DatasetTask, BaseHTCondorWorkflow, law.LocalWorkflow):

    threads = luigi.IntParameter(
        description="number of threads used for executing the cmsRun command; default: 4",
        default=4,
    )

    # hard-code the file group size for now
    file_group_size = 10

    @classmethod
    def modify_param_values(cls, params):
        # align number of requested CPUs to number of threads that are assigned for the command execution
        params.update({
            "htcondor_request_cpus": params.get("threads", 1)
        })

        return params

    def create_branch_map(self):
        # TODO allow flexible base URI, fixed for now
        #base_uri = "root://cms-xrd-global.cern.ch//"
        #base_uri = "root://xrootd-cms.infn.it//"
        base_uri = "root://cmsxrootd-kit-disk.gridka.de//"

        # extend the branch map with additional information
        branch_map = DatasetTask.create_branch_map(self)
        for branch in branch_map:
            file_index = branch_map[branch]["file_index"]

            # get file URIs corresponding to the file index of the branch
            file_uris = [base_uri + self.dataset_inst.x.lfns[i] for i in file_index]

            # generate a unique output filename for a given list of files
            output_filename = "{}.root".format(create_hash(",".join([os.path.basename(uri) for uri in file_uris])))

            # update the branch map
            branch_map[branch].update({
                "file_uris": file_uris,
                "output_filename": output_filename,
            })

        # return the extended branch map
        return branch_map

    def htcondor_workflow_requires(self):
        reqs = super(ProduceTauTriggerNtuples, self).htcondor_workflow_requires()
        reqs["BundleCMSSW"] = BundleCMSSW.req(self, cmssw_path=self.cmssw_path())
        return reqs

    def modify_polling_status_line(self, status_line):
        return "{} - dataset: {}".format(status_line, law.util.colored(self.dataset_inst.name, color='light_cyan'))

    def cmssw_parent_dir(self) -> str:
        cmssw_hash = create_hash((self.cmssw_release(), self.cmssw_arch(), self.cmssw_custom_packages_script()))
        return os.path.join(os.environ["ETV_SOFTWARE_PATH"], "cmssw", "{}_{}".format(self.cmssw_release(), cmssw_hash))

    def cmssw_release(self) -> str:
        return self.config_inst.x.cmssw.release

    def cmssw_arch(self) -> str:
        return self.config_inst.x.cmssw.arch

    def cmssw_custom_packages_script(self) -> Union[str, None]:
        path = self.config_inst.x.cmssw.get("custom_packages_script", None)
        if path is not None:
            path = os.path.expandvars(path)
        return path

    def get_trigger_names(self):
        """
        Concatenate the names of the triggers from all considered channels to one list.

        :returns: list of all trigger names, for which information is collected during the ntuple processing
        :rtype:   List[str]
        """
        return [
            trigger["name"]
            for channel in self.config_inst.channels.values()
            for trigger in channel.x.triggers
        ]

    def output(self):
        return self.remote_target(self.branch_data["output_filename"])

    def run(self):
        output = self.output()

        # create a temporary, local output directory as well as a temporary target for the ntuple
        tmpdir_target = law.LocalDirectoryTarget(is_tmp=True)
        tmp_ntuple_target = law.LocalFileTarget(os.path.join(tmpdir_target.path, "ntuple.root"))
        tmpdir_target.touch()

        # translate the dataset type to the corresponding argument of the ntuplizer script
        dataset_type = ""
        if self.dataset_inst.is_data and self.dataset_inst.x.is_emb:
            dataset_type = "emb"
        elif self.dataset_inst.is_mc:
            dataset_type = "mc"
        else:
            raise NotImplementedError("ntuplizer for data not implemented yet")

        # construct the cmsRun command
        cmd = [
            "cmsRun",
            "-n", str(self.threads),
            os.path.join(
                self.cmssw_parent_dir(),
                self.cmssw_release(),
                "src/TauAnalysis/TauTriggerNtuples/python/TauTriggerNtuplizer_cfg.py"
            ),
            "datasetType={}".format(dataset_type),
            "hltPaths={}".format(",".join(self.get_trigger_names())),
            "inputFiles={}".format(",".join(self.branch_data["file_uris"])),
            "outputFile=file:ntuple.root",
        ]

        # run the command in a CMSSW sandbox
        self.run_command(cmd, popen_kwargs={"cwd": tmpdir_target.path})

        # copy the output file to its final destination
        if not self.output().parent.exists():
            self.output().parent.touch()
        output.copy_from_local(tmp_ntuple_target)


class ProduceTauTriggerNtuplesWrapper(ConfigTask, law.WrapperTask):

    threads = luigi.IntParameter(
        description="number of threads used for executing the cmsRun command; default: 1",
        default=1,
    )

    root_process = luigi.Parameter(
        description=(
            "selection of the root process for processing datasets collectively; only datasets with a process, which "
            "is a child of the given root process, are taken into account for constructing the requirements of this "
            "wrapper task"
        ),
    )

    def __init__(self, *args, **kwargs):
        super(ProduceTauTriggerNtuplesWrapper, self).__init__(*args, **kwargs)
        self.process_inst = self.config_inst.get_process(self.root_process)

    def get_datasets_from_root_process(self, root_process: Process) -> List[Dataset]:
        datasets = []
        for dataset in self.config_inst.datasets.values():
            if len(dataset.processes) == 0:
                continue
            dataset_root_process = dataset.processes.get_first().get_root_processes()[0]
            if dataset_root_process == root_process:
                datasets.append(dataset)
        return datasets

    def requires(self):
        reqs = []
        self.publish_message("wrapping tasks for datasets associated with the root process '{}'".format(self.process_inst.name))
        for dataset in self.get_datasets_from_root_process(self.process_inst):
            self.publish_message("adding dataset '{}' to wrapper".format(dataset.name))
            reqs.append(
                ProduceTauTriggerNtuples.req(
                    self,
                    dataset=dataset.name,
                )
            )
        return reqs
