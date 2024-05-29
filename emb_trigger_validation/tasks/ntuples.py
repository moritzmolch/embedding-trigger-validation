import awkward as ak
import numpy as np
import law
from law.util import create_hash, human_bytes
import luigi
import os
import re
from typing import Union
import vector

from emb_trigger_validation.tasks.base import DatasetTask, ProcessCollectionTask
from emb_trigger_validation.tasks.cmssw import CMSSWCommandTask
from emb_trigger_validation.tasks.remote import BaseHTCondorWorkflow
from emb_trigger_validation.tasks.bundle import BundleCMSSW
from emb_trigger_validation.paths import SOFTWARE_DIR

# load additional packages
law.contrib.load("awkward", "root")


class ProduceTauTriggerNtuples(CMSSWCommandTask, DatasetTask, BaseHTCondorWorkflow, law.LocalWorkflow):

    threads = luigi.IntParameter(
        description="number of threads used for executing the cmsRun command; default: 1",
        default=1,
    )

    exclude_params_req_get = {
        "branches",
        "workflow",
        "file_group_size",
        "tolerance",
        "threads",
    }

    @classmethod
    def modify_param_values(cls, params):
        # align number of requested CPUs to number of threads that are assigned for the command execution
        params.update({
            "htcondor_request_cpus": params.get("threads", 1),
        })

        # this is a dirty fix ... for 2017, 4 files of the DY sample are consistently not reachable, so the acceptance
        # and tolerance for this sample are hard-coded here
        if params["dataset"] == "dy_ll_m50_madgraph" and params["campaign"] == "ul_2017":
            params["tolerance"] = 0.01
            params["acceptance"] = 0.99

        return params

    def create_branch_map(self):
        # pick the first redirector in the 'redirectors' list
        base_uri = self.get_dataset().x.redirectors[0]

        # extend the branch map with additional information
        branch_map = DatasetTask.create_branch_map(self)
        for branch in branch_map:
            file_index = branch_map[branch]["file_index"]

            # get file URIs corresponding to the file index of the branch
            file_uris = [base_uri + self.get_dataset().x.filelist[i] for i in file_index]

            # generate a unique output filename for a given list of files
            output_file_hash = create_hash(",".join([os.path.basename(uri) for uri in file_uris]))
            output_name = "{}.root".format(output_file_hash)

            # update the branch map
            branch_map[branch].update({
                "file_index": file_index,
                "file_uris": file_uris,
                "hash": output_file_hash,
                "output_name": output_name,
            })

        # return the extended branch map
        return branch_map

    def htcondor_workflow_requires(self):
        reqs = super(ProduceTauTriggerNtuples, self).htcondor_workflow_requires()
        reqs["BundleCMSSW"] = BundleCMSSW.req(self, cmssw_path=self.cmssw_path())
        return reqs

    def modify_polling_status_line(self, status_line):
        return "{} - dataset: {}".format(status_line, law.util.colored(self.get_dataset().name, color='light_cyan'))

    def cmssw_parent_dir(self) -> str:
        cmssw_hash = create_hash((self.cmssw_release(), self.cmssw_arch(), self.cmssw_custom_packages_script()))
        return os.path.join(SOFTWARE_DIR, "cmssw", "{}_{}".format(self.cmssw_release(), cmssw_hash))

    def cmssw_release(self) -> str:
        return self.get_campaign_config().campaign.x.cmssw.release

    def cmssw_arch(self) -> str:
        return self.get_campaign_config().campaign.x.cmssw.arch

    def cmssw_custom_packages_script(self) -> Union[str, None]:
        path = self.get_campaign_config().campaign.x.cmssw.get("custom_packages_script", None)
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
            for channel in self.get_campaign_config().channels.values()
            for trigger in channel.x.triggers
        ]

    def output(self):
        return self.remote_target(self.branch_data["output_name"])

    def run(self):
        # get the output target
        output = self.output()

        # create a temporary, local output directory as well as a temporary target for the ntuple
        tmpdir_target = law.LocalDirectoryTarget(is_tmp=True)
        tmp_ntuple_target = law.LocalFileTarget(os.path.join(tmpdir_target.path, "ntuple.root"))
        tmpdir_target.touch()

        # translate the dataset type to the corresponding argument of the ntuplizer script
        dataset_type = ""
        if self.get_dataset().is_data and self.get_dataset().has_tag("embedding"):
            dataset_type = "emb"
        elif self.get_dataset().is_mc:
            dataset_type = "mc"
        else:
            raise NotImplementedError("ntuplizer on data only implemented for embedding datasets")

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


class ProduceTauTriggerNtuplesWrapper(ProcessCollectionTask, law.WrapperTask):

    threads = luigi.IntParameter(
        description="number of threads used for executing the cmsRun command; default: 1",
        default=1,
    )

    def requires(self):
        reqs = []
        for process in self.get_processes().values():
            for dataset in self.get_datasets_with_process(process).values():
                reqs.append(
                    ProduceTauTriggerNtuples.req(
                        self,
                        dataset=dataset.name,
                        branches=":",
                    )
                )
        return reqs


class MergeTauTriggerNtuples(DatasetTask):

    def sanitize_triggers(self, trigger_table: ak.Array):

        triggers = {}

        for i in range(len(trigger_table)):
            trigger = trigger_table[i]

            # get the trigger name without version tag
            m = re.match(r"^(.*)?(_(v\d+))$",  trigger["hltPathName"])
            if m is None:
                raise Exception("trigger name could not be matched")
            trigger_name = m.group(1)

            # get information about the trigger
            trigger_index = trigger["hltPathIndex"]
            modules = ak.to_list(trigger["hltPathModules"])
            modules_save_tags = ak.to_list(trigger["hltPathModulesSaveTags"])

            if trigger_name not in triggers:
                # add the trigger to the dictionary of used triggers
                triggers[trigger_name] = {
                    "name": trigger_name,
                    "trigger_index": trigger_index,
                    "modules": modules,
                    "modules_save_tags": modules_save_tags,
                }

            else:
                # check if triggers with the same name are consistent
                if not all([
                    triggers[trigger_name]["name"] == trigger_name,
                    triggers[trigger_name]["trigger_index"] == trigger_index,
                    triggers[trigger_name]["modules"] == ak.to_list(modules),
                    triggers[trigger_name]["modules_save_tags"] == ak.to_list(modules_save_tags),
                ]):
                    raise Exception("trigger with same names do not have consistent specification")

        return list(triggers.values())

    def apply_gen_selection(self, events):
        # get the two generator-level taus
        tau_1 = events[["genParticlePt", "genParticleEta", "genParticlePhi", "genParticleMass"]][events["genParticlePdgId"] == -15][:, 0]
        tau_2 = events[["genParticlePt", "genParticleEta", "genParticlePhi", "genParticleMass"]][events["genParticlePdgId"] == 15][:, 0]

        # create four-vector objects and combine them
        tau_1 = vector.Array({
            "pt": tau_1["genParticlePt"],
            "eta": tau_1["genParticleEta"],
            "phi": tau_1["genParticlePhi"],
            "mass": tau_1["genParticleMass"],
        })
        tau_2 = vector.Array({
            "pt": tau_2["genParticlePt"],
            "eta": tau_2["genParticleEta"],
            "phi": tau_2["genParticlePhi"],
            "mass": tau_2["genParticleMass"],
        })
        tautau = tau_1 + tau_2

        # check if all individual taus as well as the tau pairs fulfill the generator cuts used for this analysis
        # the selection has two purposes:
        # - select MC events, which fulfill the cuts on the muons used for mu -> tau-embedding
        # - select mu -> tau-embedded events, which fulfill the invariant Z/gamma^* mass cut on 50 GeV
        pt_pairs = np.concatenate((np.reshape(ak.to_numpy(tau_1.pt), (-1, 1)), np.reshape(ak.to_numpy(tau_2.pt), (-1, 1))), axis=1)
        mask = (
            (np.max(pt_pairs, axis=1) > 17)
            & (np.min(pt_pairs, axis=1) > 8)
            & (abs(tau_1.eta) < 2.4)
            & (abs(tau_2.eta) < 2.4)
            & (tautau.mass > 50)
        )

        # select the events
        events_sel = events[mask]

        return events_sel

    def requires(self):
        reqs = {}
        reqs["ProduceTauTriggerNtuples"] = ProduceTauTriggerNtuples.req(self, branches=":")
        return reqs

    def output(self):
        return {
            "events": self.remote_target("events__{}.parquet".format(self.get_dataset().name)),
            "triggers": self.remote_target("triggers__{}.json".format(self.get_dataset().name)),
        }

    def run(self):
        # get the output target
        output = self.output()

        # get the input target
        input_ntuple_collection = self.input()["ProduceTauTriggerNtuples"]["collection"]

        # objects holding the full event and the trigger table
        events = []
        trigger_table = []

        # merge all targets, track progress
        targets = list(input_ntuple_collection.iter_existing())
        n_targets = len(targets)
        for i, target in enumerate(targets):

            # log progress and size of loaded files
            self.publish_message("load file {}/{} with size {:.2f}{}".format(i + 1, n_targets, *human_bytes(target.stat().st_size, unit="MB")))

            # load the ROOT file
            with target.load(formatter="uproot") as f:
                _events = f["tauTriggerNtuplizer/Events"].arrays(library="ak")
                _trigger_table = f["tauTriggerNtuplizer/HLT"].arrays(library="ak")

            # apply the generator-level selection
            _events = self.apply_gen_selection(_events)

            # check if the tables are empty, which means that we want to continue
            if len(_events) == 0 and len(_trigger_table) == 0:
                continue

            # merge the events and the trigger tables loaded from the current target with the full tables
            events.append(_events)
            trigger_table.append(_trigger_table)
        
        # convert the lists into arrays
        events = ak.concatenate(events, axis=0)
        trigger_table = ak.concatenate(trigger_table, axis=0)

        triggers = self.sanitize_triggers(trigger_table)

        # dump objects to the output targets
        with self.publish_step("create output files ..."):
            # dump the tables into parquet files
            output["events"].dump(events, formatter="awkward")
            output["triggers"].dump(triggers, formatter="json")

            # log the size of output files
            self.publish_message(
                "size of output files: {:.2f}{}".format(
                    *human_bytes(
                        output["events"].stat().st_size + output["triggers"].stat().st_size,
                        unit="MB",
                    )
                )
            )


class MergeTauTriggerNtuplesWrapper(ProcessCollectionTask, law.WrapperTask):

    def requires(self):
        reqs = []
        for process in self.get_processes().values():
            for dataset in self.get_datasets_with_process(process).values():
                reqs.append(
                    MergeTauTriggerNtuples.req(
                        self,
                        dataset=dataset.name,
                    )
                )
        return reqs
