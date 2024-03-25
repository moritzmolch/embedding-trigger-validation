import law
from law.util import create_hash
import luigi
import os
from typing import Union

from emb_trigger_validation.tasks.base import CMSSWCommandTask, DatasetTask
from emb_trigger_validation.tasks.remote import BaseHTCondorWorkflow
from emb_trigger_validation.tasks.bundle import BundleCMSSW


class ProduceTauTriggerNtuples(CMSSWCommandTask, DatasetTask, law.LocalWorkflow, BaseHTCondorWorkflow):

    threads = luigi.IntParameter(
        description="number of threads used for executing the cmsRun command; default: 1",
        default=1,
    )

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
