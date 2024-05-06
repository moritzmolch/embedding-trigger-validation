import hist
import law
import luigi
import numpy as np
import matplotlib as mpl
import matplotlib.gridspec as gs
import matplotlib.pyplot as plt
import mplhep
from order import Channel, Process
import re
from typing import Iterable, Tuple

from emb_trigger_validation.tasks.base import ProcessCollectionTask
from emb_trigger_validation.tasks.histograms import CreateCutflowHistogram

# load additional packages
law.contrib.load("matplotlib", "root")


class PlotEfficiencyComparison(ProcessCollectionTask):

    channel = law.Parameter(
        default=law.NO_STR,
        description="name of the channel",
    )

    trigger = law.Parameter(
        default=law.NO_STR,
        description="name of the trigger path, for which the cutflow histogram is created",
    )

    prefix = luigi.Parameter(
        description=(
            "tag, which is prepended to the names of all generated output files; optional"
        ),
        default=law.NO_STR,
    )

    postfix = luigi.Parameter(
        description=(
            "tag, which is appended to the names of all generated output files just in front of the file extension; "
            "optional"
        ),
        default=law.NO_STR,
    )

    extension = luigi.Parameter(
        description=(
            "file extension of the generated output files, e.g. '.png', '.pdf', '.svg'; all file formats that "
            "matplotlib is capable to handle may be used here; default: 'pdf'"
        ),
        default="pdf",
    )

    exclude_params_req = {"branch", "branches"}

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

    def requires(self):
        reqs = dict(super().requires())
        for process in self.get_processes().values():
            reqs[f"CreateCutflowHistogram__{process.name}"] = []
            for dataset in self.get_datasets_with_process(process).values():
                reqs[f"CreateCutflowHistogram__{process.name}"].append(CreateCutflowHistogram.req(
                self,
                dataset=dataset.name,
                channel=self.get_channel().name,
                trigger=self.get_trigger()["name"],
            ))
        return reqs

    def output(self):
        prefix = "__{}".format(self.prefix) if self.prefix != law.NO_STR else ""
        postfix = "__{}".format(self.postfix) if self.postfix != law.NO_STR else ""
        extension = self.extension
        process_string = "_".join(self.get_processes().names())
        return self.local_target(
            "plot_cutflow_efficiency{}__{}__{}__{}{}.{}".format(
                prefix,
                process_string,
                self.get_channel().name,
                self.get_trigger()["name"],
                postfix,
                extension,
            )
        )

    def get_channel(self):
        return self._channel

    def get_trigger(self):
        return self._trigger

    def complete(self):
        return False

    def plot_efficiency_comparison(
        self,
        hists_and_processes: Iterable[Tuple[hist.Hist, Process]],
        trigger_name: str,
        channel_inst: Channel,
    ):
        fig = plt.figure(figsize=(10, 10))

        grid = gs.GridSpec(1, 2, figure=fig, wspace=0, bottom=0.20, left=0.06, right=0.97)
        ax_eff = fig.add_subplot(grid[0, 0], zorder=2)
        ax_rej = fig.add_subplot(grid[0, 1], sharey=ax_eff, zorder=1)

        n_labels = len(hists_and_processes[0][0].axes[0]) - 1
        labels = list(hists_and_processes[0][0].axes[0])[1:]

        for cutflow_hist, process in hists_and_processes:
            eff = cutflow_hist[:].values() / cutflow_hist[0].value
            rej_nom = eff[1:]
            rej_denom = eff[:-1]

            rejection = np.concatenate(
                (
                    [0],
                    np.where(
                        rej_denom > 0, 1 - rej_nom / rej_denom, np.zeros(len(rej_denom))
                    )
                ),
                axis=0,
            )
            edges = cutflow_hist.axes[0].edges - 0.5
            centers = (edges[1:] + edges[:-1]) / 2.

            common_args = (centers[1:], )
            common_kwargs = dict(
                bins=edges[1:],
                color=process.color1,
                orientation="horizontal",
                histtype="step",
                linewidth=2,
            )

            ax_eff.hist(*common_args, weights=eff[1:], **common_kwargs, label=process.label)
            ax_rej.hist(*common_args, weights=rejection[1:], **common_kwargs)

        ax_eff.set_xlabel("acceptance $\\times$ efficiency of filter $i$")
        ax_eff.set_ylabel("filter")
        ax_rej.set_xlabel("rejection rate of filter $i$")

        ax_eff.set_ylim(n_labels + 0.5, 0.5)
        ax_rej.set_ylim(n_labels + 0.5, 0.5)
        ax_eff.set_yticks(np.arange(1, n_labels + 1), labels, horizontalalignment="left", color="black", fontsize="x-small")
        ax_eff.tick_params(axis="y", direction="in", pad=-15)
        ax_rej.set_xticks(ax_rej.get_xticks()[1:], ax_rej.get_xticklabels()[1:])
        ax_rej.tick_params(axis="y", labelcolor="none")

        x_acc = ax_eff.get_xlim()
        x_rej = ax_rej.get_xlim()
        for i in range(1, n_labels + 1):
            if i % 2 == 0:
                continue
            ax_eff.fill_between(x_acc, i - 0.5, i + 0.5, color="gray", alpha=0.3, zorder=-1)
            ax_rej.fill_between(x_rej, i - 0.5, i + 0.5, color="gray", alpha=0.3, zorder=-1)
        ax_eff.set_xlim(x_acc)
        ax_rej.set_xlim(x_rej)

        ax_eff.tick_params(axis="y", which="both", left=False, right=False)
        ax_rej.tick_params(axis="y", which="both", left=False, right=False)

        mplhep.cms.text(ax=ax_eff, text="Work in progress")
        mplhep.cms.lumitext(ax=ax_rej, text="{} (13 TeV)".format(self.get_campaign_config().campaign.x.label))

        ax_eff.text(0.06, 0.11, "Path " + trigger_name, horizontalalignment="left", verticalalignment="center", transform=fig.transFigure)
        ax_eff.text(0.06, 0.07, channel_inst.label + " channel", horizontalalignment="left", verticalalignment="center", transform=fig.transFigure)

        ax_eff.grid(True, axis="x", color="tab:gray", linewidth=1)
        ax_rej.grid(True, axis="x", color="tab:gray", linewidth=1)

        fig.legend(loc="outside lower right")

        return fig, ax_rej, ax_eff

    def run(self):
        # get the output target
        output = self.output()

        # get the input target collection of histograms
        input_hists = []
        for key in self.input():
            m = re.match(r"^.*__(.*)$", key)
            if m is None:
                raise ValueError("process name could not be parsed from requirement key '{}'".format(key))
            process_name = m.group(1)
            process = self.get_processes().get(process_name)
            hist_targets = []
            for targets in self.input()[key]:
                hist_targets.extend(list(targets["collection"].targets.values()))
            input_hists.append((hist_targets, process))

        # get channel and trigger
        channel = self.get_channel()
        trigger_name = self.get_trigger()["name"]

        # load histograms from files and add them together for the same root process
        hists_and_processes = []
        for hist_targets, process in input_hists:
            cutflow_hist = None
            for hist_target in hist_targets:
                _cutflow_hist = hist_target.load(formatter="pickle")
                if cutflow_hist is None:
                    cutflow_hist = _cutflow_hist[trigger_name, hist.sum, channel.name, :]
                else:
                    cutflow_hist += _cutflow_hist[trigger_name, hist.sum, channel.name, :]
            hists_and_processes.append((cutflow_hist, process))

        # plot acceptance
        style = mplhep.style.CMS
        style.update({
            "font.size": 18,
            "axes.axisbelow": False,
        })
        with mpl.style.context(style):
            fig, ax_acc, ax_rej = self.plot_efficiency_comparison(hists_and_processes, trigger_name, channel)
            output.dump(fig, formatter="mpl")


class PlotEfficiencyComparisonWrapper(ProcessCollectionTask, law.WrapperTask):

    def requires(self):
        reqs = []
        process_names = [p.name for p in self.get_processes().values()]
        for channel in self.get_campaign_config().channels:
            for trigger in channel.x.triggers:
                reqs.append(
                    PlotEfficiencyComparison.req(
                        self,
                        processes=process_names,
                        channel=channel.name,
                        trigger=trigger["name"],
                    )
                )
        return reqs
