import hist
import law
import numpy as np
import matplotlib as mpl
import matplotlib.gridspec as gs
import matplotlib.pyplot as plt
import mplhep
from order import Channel, Process, UniqueObjectIndex
import re
from typing import Iterable, Tuple

from emb_trigger_validation.tasks.base import ConfigTask
from emb_trigger_validation.tasks.histograms import MergeCutflowHistogramsWrapper

# load additional packages
law.contrib.load("matplotlib", "root")


class PlotAcceptance(ConfigTask, law.LocalWorkflow):

    root_processes = law.CSVParameter(
        description=(
            "root processes considered for creating the acceptance histograms; only datasets with a process, which "
            "is a child of the given process, are taken into account for constructing the requirements of this "
            "wrapper task"
        ),
    )

    exclude_params_req = {"branch", "branches"}

    def __init__(self, *args, **kwargs):
        super(PlotAcceptance, self).__init__(*args, **kwargs)
        self.process_insts = UniqueObjectIndex(
            Process,
            [
                self.config_inst.get_process(process_name) for process_name in self.root_processes
            ]
        )

    def create_branch_map(self):
        branch_map = []
        for channel in self.config_inst.channels.values():
            for trigger in channel.x.triggers:
                branch_map.append({
                    "channel": channel,
                    "trigger": trigger,
                })
        return {i: v for i, v in enumerate(branch_map)}

    def workflow_requires(self):
        reqs = dict(super(PlotAcceptance, self).workflow_requires())
        for process in self.process_insts:
            reqs["MergeCutflowHistogramsWrapper__root_process_{}".format(process.name)] = MergeCutflowHistogramsWrapper.req(
                self,
                root_process=process.name,
            )
        return reqs

    def requires(self):
        reqs = dict(super(PlotAcceptance, self).requires())
        for process_inst in self.process_insts:
            reqs["MergeCutflowHistogramsWrapper__root_process_{}".format(process_inst.name)] = MergeCutflowHistogramsWrapper.req(
                self,
                root_process=process_inst.name,
            )
        return reqs

    def output(self):
        process_string = "_".join(self.process_insts.names())
        return self.local_target(
            "plot_cutflow_acceptance__{}__{}__{}.pdf".format(
                process_string,
                self.branch_data["channel"].name,
                self.branch_data["trigger"]["name"],
            )
        )

    def plot_acceptance(
        self,
        hists_and_processes: Iterable[Tuple[hist.Hist, Process]],
        trigger_name: str,
        channel_inst: Channel,
    ):
        fig = plt.figure(figsize=(10, 10))

        grid = gs.GridSpec(1, 2, figure=fig, wspace=0, bottom=0.20, left=0.06, right=0.97)
        ax_acc = fig.add_subplot(grid[0, 0], zorder=2)
        ax_rej = fig.add_subplot(grid[0, 1], sharey=ax_acc, zorder=1)

        n_labels = len(hists_and_processes[0][0].axes[0]) - 1
        labels = list(hists_and_processes[0][0].axes[0])[1:]

        for cutflow_hist, process_inst in hists_and_processes:
            acceptance = cutflow_hist[:].values() / cutflow_hist[0].value
            rej_nom = acceptance[1:]
            rej_denom = acceptance[:-1]

            rejection = np.concatenate(
                (
                    [0],
                    np.where(
                        rej_denom > 0, 1 - rej_nom / rej_denom, np.zeros(len(rej_nom))
                    )
                ),
                axis=0,
            )
            edges = cutflow_hist.axes[0].edges - 0.5
            centers = (edges[1:] + edges[:-1]) / 2.

            common_args = (centers[1:], )
            common_kwargs = dict(
                bins=edges[1:],
                color=process_inst.color1,
                orientation="horizontal",
                histtype="step",
                linewidth=2,
            )
            ax_acc.hist(*common_args, weights=acceptance[1:], **common_kwargs, label=process_inst.label)
            ax_rej.hist(*common_args, weights=rejection[1:], **common_kwargs)
   
        ax_acc.set_xlabel("acceptance $\\times$ efficiency of filter $i$")
        ax_acc.set_ylabel("filter")
        ax_rej.set_xlabel("rejection rate of filter $i$")

        ax_acc.set_ylim(n_labels + 0.5, 0.5)
        ax_rej.set_ylim(n_labels + 0.5, 0.5)
        ax_acc.set_yticks(np.arange(1, n_labels + 1), labels, horizontalalignment="left", color="black", fontsize="x-small")
        ax_acc.tick_params(axis="y", direction="in", pad=-15)
        ax_rej.set_xticks(ax_rej.get_xticks()[1:], ax_rej.get_xticklabels()[1:])
        ax_rej.tick_params(axis="y", labelcolor="none")

        x_acc = ax_acc.get_xlim()
        x_rej = ax_rej.get_xlim()
        for i in range(1, n_labels + 1):
            if i % 2 == 0:
                continue
            ax_acc.fill_between(x_acc, i - 0.5, i + 0.5, color="gray", alpha=0.3, zorder=-1)
            ax_rej.fill_between(x_rej, i - 0.5, i + 0.5, color="gray", alpha=0.3, zorder=-1)
        ax_acc.set_xlim(x_acc)
        ax_rej.set_xlim(x_rej)

        ax_acc.tick_params(axis="y", which="both", left=False, right=False)
        ax_rej.tick_params(axis="y", which="both", left=False, right=False)

        mplhep.cms.text(ax=ax_acc, text="Work in progress")
        mplhep.cms.lumitext(ax=ax_rej, text="{} (13 TeV)".format(self.config_inst.campaign.x.era_name))

        ax_acc.text(0.06, 0.11, "Path " + trigger_name, horizontalalignment="left", verticalalignment="center", transform=fig.transFigure)
        ax_acc.text(0.06, 0.07, channel_inst.label + " channel", horizontalalignment="left", verticalalignment="center", transform=fig.transFigure)
        fig.legend(loc="outside lower right")

        return fig, ax_rej, ax_acc

    def run(self):
        # get the output target
        output = self.output()

        # get the input target collection of histograms
        input_hists = []
        for key in self.input():
            m = re.match(r"^.*__root_process_(.*)$", key)
            if m is None:
                raise ValueError("process name could not be parsed from requirement key '{}'".format(key))
            process_name = m.group(1)
            input_hists.append((
                self.input()[key],
                self.process_insts.get(process_name),
            ))

        # get parameters of this branch
        channel = self.branch_data["channel"]
        trigger_name = self.branch_data["trigger"]["name"]

        # load histograms from files and add them together for the same root process
        hists_and_processes = []
        for hist_targets, process in input_hists:
            cutflow_hist = None
            for hist_target in hist_targets:
                _cutflow_hist = hist_target.load(formatter="pickle")[trigger_name]
                if cutflow_hist is None:
                    cutflow_hist = _cutflow_hist
                else:
                    cutflow_hist += _cutflow_hist

            cutflow_hist = cutflow_hist[trigger_name, hist.sum, channel.name, :]
            hists_and_processes.append((cutflow_hist, process))

        # plot acceptance
        style = mplhep.style.CMS
        style.update({
            "font.size": 18,
            "axes.axisbelow": False,
        })
        with mpl.style.context(style):
            fig, ax_acc, ax_rej = self.plot_acceptance(hists_and_processes, trigger_name, channel)
            output.dump(fig, formatter="mpl")
