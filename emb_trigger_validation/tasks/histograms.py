import awkward as ak
import hist
import law
import numpy as np

from emb_trigger_validation.tasks.base import DatasetTask, ProcessCollectionTask
from emb_trigger_validation.tasks.ntuples import MergeTauTriggerNtuples
from emb_trigger_validation.tasks.remote import BaseHTCondorWorkflow

# load additional packages
law.contrib.load("awkward", "root")


class CreateCutflowHistogram(DatasetTask, BaseHTCondorWorkflow, law.LocalWorkflow):

    channel = law.Parameter(
        default=law.NO_STR,
        description="name of the channel",
    )

    trigger = law.Parameter(
        default=law.NO_STR,
        description="name of the trigger path, for which the cutflow histogram is created",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._channel =  None
        self._trigger =  None
        if self.channel != law.NO_STR:
            self._channel = self.get_campaign_config().get_channel(self.channel)
        if self.trigger != law.NO_STR:
            for trigger in self._channel.x.triggers:
                if trigger["name"] == self.trigger:
                    self._trigger = trigger
                    break
        #if self._trigger is None:
        #    raise ValueError(f"trigger with name '{self.trigger}' not found")

    def modify_polling_status_line(self, status_line):
        return "{} - dataset: {}".format(status_line, law.util.colored(self.get_dataset().name, color='light_cyan'))

    def create_branch_map(self):
        return {0: {}}

    def workflow_requires(self):
        return {
            "MergeTauTriggerNtuples": MergeTauTriggerNtuples.req(self),
        }

    def requires(self):
        return {
            "MergeTauTriggerNtuples": MergeTauTriggerNtuples.req(self),
        }

    def get_channel(self):
        return self._channel

    def get_trigger(self):
        return self._trigger

    def output(self):
        return self.remote_target("cutflow_histogram__{}__{}__{}.pickle".format(self.get_dataset().name, self.get_channel().name, self.get_trigger()["name"]))

    def get_final_state(self, events: ak.Array) -> ak.Array:
        # create auxiliary arrays for final states
        array_et = np.repeat("et", len(events))
        array_mt = np.repeat("mt", len(events))
        array_tt = np.repeat("tt", len(events))
        array_none = np.repeat("none", len(events))

        # create array with the final state string
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
        
        return final_state

    def get_cutflow_histogram(self, events: ak.Array, trigger_name: str, trigger_table: list) -> hist.Hist:
        # get the trigger name
        trigger_name = self.get_trigger()["name"]

        # get the trigger information
        trigger = list([t for t in trigger_table if t["name"] == trigger_name])[0]
        trigger_index = trigger["trigger_index"]
        modules = trigger["modules"]
        modules_save_tags = trigger["modules_save_tags"]

        # create the cutflow histogram
        h = hist.Hist(
            hist.axis.StrCategory([trigger_name, ], name="trigger"),
            hist.axis.StrCategory([], growth=True, name="dataset"),
            hist.axis.StrCategory(["et", "mt", "tt", "none"], name="channel"),
            hist.axis.StrCategory(["n_events", ] + modules_save_tags, name="module"),
            storage=hist.storage.Weight(),
        )

        # column with generator weights
        weight = events["genWeight"]

        # column with final state strings
        final_state = self.get_final_state(events)

        # get mask for rows, which correspond to the regarded trigger
        trigger_mask = events["hltPathIndex"] == trigger_index

        with self.publish_step(f"processing trigger {trigger_name} ..."):

            # get the number of events that have passed the selection, fill the histogram with it
            self.publish_message(f"[1/{len(modules_save_tags) + 1}] processing filter n_events")
            passed = ak.ones_like(events["event"], dtype=np.uint8)
            h.fill(
                trigger=trigger_name,
                dataset=self.get_dataset().name,
                channel=final_state,
                module="n_events",
                weight=passed * weight,
            )

            for i, module_save_tags in enumerate(modules_save_tags):
                self.publish_message(f"[{i + 2}/{len(modules_save_tags) + 1}] processing filter {module_save_tags} ...")
                module_index = modules.index(module_save_tags)

                # get the number of events that have passed the filter
                passed = ak.sum(
                    (
                        events["hltPathLastModule"][trigger_mask] == module_index & (events["hltPathLastModuleState"][trigger_mask] == 1)
                    ) | (
                        events["hltPathLastModule"][trigger_mask] > module_index
                    ),
                    axis=1,
                )
 
                # fill the histogram
                h.fill(
                    trigger=trigger_name,
                    dataset=self.get_dataset().name,
                    channel=final_state,
                    module=module_save_tags,
                    weight=passed * weight,
                )

        return h

    def run_branch(self, trigger: dict):
        # get the trigger name
        trigger_name = trigger["name"]

        # get the output target
        output = self.output()

        # get the input targets
        input_events = self.input()["MergeTauTriggerNtuples"]["events"]
        input_triggers = self.input()["MergeTauTriggerNtuples"]["triggers"]

        # load the content from the input files
        events = input_events.load(formatter="awkward")
        trigger_table = input_triggers.load(formatter="json")

        # create the cutflow histogram
        h = self.get_cutflow_histogram(events, trigger_name, trigger_table)

        # pickle the histogram
        output.dump(h, formatter="pickle")

    def run(self):
        self.run_branch(self.get_trigger())


class CreateCutflowHistogramsForDataset(CreateCutflowHistogram):

    def create_branch_map(self):
        branch_map = []
        for channel in self.get_campaign_config().channels:
            for trigger in channel.x.triggers:
                branch_map.append({
                    "channel": channel.name,
                    "trigger": trigger["name"],
                })
        return {i: branch for i, branch in enumerate(branch_map)}

    def get_channel(self):
        return self.get_campaign_config().get_channel(self.branch_data["channel"])

    def get_trigger(self):
        for trigger in self.get_channel().x.triggers:
            if trigger["name"] == self.branch_data["trigger"]:
                return trigger

    def run(self):
        self.run_branch(self.get_trigger())


class CreateCutflowHistogramsWrapper(ProcessCollectionTask, law.WrapperTask):

    def requires(self):
        reqs = []
        for process in self.get_processes().values():
            htcondor_request_memory = "2GB"
            if process.name.startswith("dyjets_ll"):
                htcondor_request_memory = "4GB"
            for dataset in self.get_datasets_with_process(process).values():
                reqs.append(
                    CreateCutflowHistogramsForDataset.req(
                        self,
                        dataset=dataset.name,
                        htcondor_request_memory=htcondor_request_memory,
                    )
                )
        return reqs
