import awkward as ak
import hist
import law
from law import localize_file_targets
from law.util import human_bytes
import luigi
import numpy as np
from order import Dataset, Process
import re
from typing import List
import vector

from emb_trigger_validation.tasks.base import ConfigTask, DatasetTask
from emb_trigger_validation.tasks.ntuples import ProduceTauTriggerNtuples
from emb_trigger_validation.tasks.remote import BaseHTCondorWorkflow

# load additional packages
law.contrib.load("root")


class HLTStore():

    def __init__(self):
        self._hlt_paths = []

    def put(self, hlt_path):
        is_member = False

        for member in self._hlt_paths:
            if (member.name == hlt_path.name):
                has_same_modules = (len(member.modules) == len(hlt_path.modules)) and any(member.modules[i] == hlt_path.modules[i] for i in range(len(hlt_path.modules)))
                has_same_modules_save_tags = (len(member.modules_save_tags) == len(hlt_path.modules_save_tags)) and any(member.modules_save_tags[i] == hlt_path.modules_save_tags[i] for i in range(len(hlt_path.modules_save_tags)))
                if has_same_modules and has_same_modules_save_tags:
                    is_member = True
                    break
                if not has_same_modules:
                    raise Exception("HLT path '{}' has inconsistent list of modules over runs".format(hlt_path.name))
                if not has_same_modules_save_tags:
                    raise Exception("HLT path '{}' has inconsistent list of modules with save tag over runs".format(hlt_path.name))

        if not is_member:
            self._hlt_paths.append(hlt_path)

    def get(self, name):
        for member in self._hlt_paths:
            if member.name == name:
                return member
        raise Exception("HLT path '{}' not found".format(name))

    def names(self):
        return [member.name for member in self._hlt_paths]

    def to_dict(self):
        return {
            name: self.get(name)
            for name in self.names()
        }


class HLT():
    
    def __init__(self, full_name, index, modules, modules_save_tags, run):
        self.full_name = full_name
        self.index = index
        self.name = self.full_name
        self.version = ""
        m = re.match("^(.*)_(v\d+)$", self.full_name)
        if m:
            self.name = m.group(1)
            self.version = m.group(2)
        self.modules = modules
        self.modules_save_tags = modules_save_tags
        self.run = run

    def get_module_index(self, module):
        for i, _module in enumerate(self.modules):
            if _module == module:
                return i
        raise RuntimeError("module '{}' not found in trigger path '{}'".format(module, self.name))


class CreateCutflowHistograms(DatasetTask, BaseHTCondorWorkflow, law.LocalWorkflow):

    def __init__(self, *args, **kwargs):
        super(CreateCutflowHistograms, self).__init__(*args, **kwargs)

    # depends on the branch map of the upstream task, hence this parameter is unset here
    file_group_size = None

    exclude_params_req_get = {"branches", "workflow"}

    def create_branch_map(self):
        # get the branch map from the upstream task
        branch_map = ProduceTauTriggerNtuples.req(self, branches=":").create_branch_map()

        # iterate over the branch map and update the payload
        for branch_data in branch_map.values():
            # get the hash from the upstream task and generate the output filename
            output_file_hash = branch_data["hash"]
            output_name = "hist_cutflow__{}__{}.root".format(self.dataset_inst.name, output_file_hash)

            # update the branch data
            branch_data.update({
                "output_name": output_name,
            })

        # return the extended branch map
        return branch_map

    def modify_polling_status_line(self, status_line):
        return "{} - dataset: {}".format(status_line, law.util.colored(self.dataset_inst.name, color='light_cyan'))

    def workflow_requires(self):
        reqs = dict(super(CreateCutflowHistograms, self).workflow_requires())
        reqs["ProduceTauTriggerNtuples"] = ProduceTauTriggerNtuples.req(self, branches=self.branches)

    def requires(self):
        reqs = dict(super(CreateCutflowHistograms, self).requires())
        reqs["ProduceTauTriggerNtuples"] = ProduceTauTriggerNtuples.req(self, branch=self.branch)
        return reqs

    def output(self):
        return self.remote_target(self.branch_data["output_name"])

    def get_hlt(self, hlt_tree):
        hlt_store = HLTStore()
        for run in np.unique(hlt_tree["run"]):
            paths = hlt_tree[hlt_tree["run"] == run][["hltPathName", "hltPathIndex", "hltPathModules", "hltPathModulesSaveTags"]]
            for i in range(len(paths)):
                hlt_store.put(
                    HLT(
                        paths[i]["hltPathName"],
                        paths[i]["hltPathIndex"],
                        paths[i]["hltPathModules"].to_list(),
                        paths[i]["hltPathModulesSaveTags"].to_list(),
                        run,
                    )
                )
        return hlt_store

    def get_gen_cut_mask(self, events):
        tau_1 = events[["genParticlePt", "genParticleEta", "genParticlePhi", "genParticleMass"]][events["genParticlePdgId"] == -15][:, 0]
        tau_2 = events[["genParticlePt", "genParticleEta", "genParticlePhi", "genParticleMass"]][events["genParticlePdgId"] == 15][:, 0]

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

        pt_pairs = np.concatenate((np.reshape(ak.to_numpy(tau_1.pt), (-1, 1)), np.reshape(ak.to_numpy(tau_2.pt), (-1, 1))), axis=1)
        mask = (
            (np.max(pt_pairs, axis=1) > 17)
            & (np.min(pt_pairs, axis=1) > 8)
            & (abs(tau_1.eta) < 2.4)
            & (abs(tau_2.eta) < 2.4)
            & (tautau.mass > 50)
        )

        return mask

    def create_cutflow_histogram(self, events: ak.Array, hlt_paths):
        cutflow = {}

        weight = events["genWeight"]

        # array with the final state string
        array_et = np.repeat("et", len(weight))
        array_mt = np.repeat("mt", len(weight))
        array_tt = np.repeat("tt", len(weight))
        array_none = np.repeat("none", len(weight))
        final_state = ak.where(
            events["isElTau"],
            array_et,
            ak.where(
                events["isMuTau"],
                array_mt,
                ak.where(
                    events["isTauTau"],
                    array_tt,
                    array_none,
                )
            )
        )

        for hlt_path_name, hlt_path in hlt_paths.items():            

            # create the cutflow histogram
            h = hist.Hist(
                hist.axis.StrCategory([hlt_path_name, ], name="hlt_path"),
                hist.axis.StrCategory([], growth=True, name="dataset"),
                hist.axis.StrCategory(["et", "mt", "tt", "none"], name="channel"),
                hist.axis.StrCategory(["n_events", ] + hlt_path.modules_save_tags, name="module"),
                storage=hist.storage.Weight(),
            )

            # check the index and the state of the last module for the considered HLT path
            hlt_module_info = events[["hltPathLastModule", "hltPathLastModuleState"]][(events["hltPathIndex"] == hlt_path.index)]

            n_pass = ak.ones_like(weight, dtype=int)
            h.fill(
                hlt_path=np.repeat(hlt_path_name, len(n_pass)),
                dataset=self.dataset_inst.name,
                channel=final_state,
                module=np.repeat("n_events", len(n_pass)),
                weight=n_pass * weight,
            )

            for module in hlt_path.modules_save_tags:
                module_index = hlt_path.get_module_index(module)

                # calculate number of events that have passed the filter
                # this number has to be multiplied by the 
                n_pass = ak.sum(
                    (
                        hlt_module_info["hltPathLastModule"] == module_index & (hlt_module_info["hltPathLastModuleState"] == 1)
                    ) | (
                        hlt_module_info["hltPathLastModule"] > module_index
                    ),
                    axis=1,
                )
                h.fill(
                    hlt_path=np.repeat(hlt_path_name, len(n_pass)),
                    dataset=self.dataset_inst.name,
                    channel=final_state,
                    module=np.repeat(module, len(n_pass)),
                    weight=n_pass * weight,
                )

            cutflow[hlt_path_name] = h

        return cutflow

    def run(self):
        # get the task's inputs and outputs
        input_ntuple = self.input()["ProduceTauTriggerNtuples"]
        output = self.output()

        # emerge a message that histogram production is running
        with self.publish_step("produce cutflow histogram for dataset '{}'".format(self.dataset_inst.name)):

            # load the tables
            with input_ntuple.localize(mode="r") as tmp_target:
                with tmp_target.load(formatter="uproot", mode="r") as f:
                    events = f["tauTriggerNtuplizer/Events"].arrays(library="ak")
                    trigger_table = f["tauTriggerNtuplizer/HLT"].arrays(library="ak")

            # apply the events selection and produce the cutflow histogram
            gen_cut_mask = self.get_gen_cut_mask(events)
            events = events[gen_cut_mask]
            hlt_paths = self.get_hlt(trigger_table).to_dict()
            cutflow = self.create_cutflow_histogram(events, hlt_paths)

        output.dump(cutflow, formatter="pickle")


class MergeCutflowHistograms(DatasetTask):

    def requires(self):
        reqs = dict(super(MergeCutflowHistograms, self).requires())
        reqs["CreateCutflowHistograms"] = CreateCutflowHistograms.req(self, branches=":")
        return reqs

    def output(self):
        return self.local_target("cutflow_histogram_merged__{}.pickle".format(self.dataset_inst.name))

    def run(self):
        # get the task's inputs and outputs
        input_cutflows = self.input()["CreateCutflowHistograms"]["collection"]
        output = self.output()

        # initialize variable for the merged cutflow
        cutflow = {}

        # load the input ntuples
        with localize_file_targets(input_cutflows.targets, mode="r") as local_targets:
            # number of files, needed for progress monitoring
            n_files = len(local_targets)

            # loop over local targets and invoke progress monitoring
            for i, (chunk, local_target) in enumerate(local_targets.items()):

                # load the file and concatenate events and trigger tables
                with self.publish_step(
                    "[{}/{}] loading file {} (size {} {})".format(
                        i + 1, n_files, local_target.basename, *human_bytes(local_target.stat().st_size, unit="MB"))
                ):
                    # add up the cutflow histograms for all HLT paths
                    _cutflow = local_target.load(formatter="pickle")
                    for trigger_name in _cutflow.keys():
                        if trigger_name not in cutflow:
                            cutflow[trigger_name] = _cutflow[trigger_name]
                        else:
                            cutflow[trigger_name] += _cutflow[trigger_name]
 
        # finally dump the merged histograms into the output file
        output.dump(cutflow, formatter="pickle")


class MergeCutflowHistogramsWrapper(ConfigTask, law.WrapperTask):

    root_process = luigi.Parameter(
        description=(
            "selection of the root process for processing datasets collectively; only datasets with a process, which "
            "is a child of the given root process, are taken into account for constructing the requirements of this "
            "wrapper task"
        ),
    )

    def __init__(self, *args, **kwargs):
        super(MergeCutflowHistogramsWrapper, self).__init__(*args, **kwargs)
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
        self.logger.info("wrapping tasks for datasets associated with the root process '{}'".format(self.process_inst.name))
        for dataset in self.get_datasets_from_root_process(self.process_inst):
            self.logger.info("adding dataset '{}' to wrapper".format(dataset.name))
            reqs.append(
                MergeCutflowHistograms.req(
                    self,
                    dataset=dataset.name,
                )
            )
        return reqs
