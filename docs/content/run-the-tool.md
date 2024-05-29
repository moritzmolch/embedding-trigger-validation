# Run the tool

!!! note
    This tool must be run on a machine with CentOS 7 as its operating system. The software stack is set up with an LCG view compiled against CentOS 7. The grid setup, which is used, also only works on a CentOS 7 machine. The CMSSW version, which is used, is also only available for CentOS 7.


## Trigger the whole workflow

Some parts of the workflow produce output files on the WLCG. Therefore, make sure that you have a valid VOMS proxy file.

To steer the workflow and to ensure that all requirements of an element in the workflow chain have been processed, this tool uses [LAW](https://github.com/riga/law). In principal, only a few commands are needed to produce the results for a production campaign, e.g. `ul_2016preVFP`. Open your shell and type in the following commands:

```bash
# pull the repository (note the --recursive option)
git clone --recursive git@github.com:KIT-CMS/embedding-trigger-validation.git

# go to the root directory of this project
cd embedding-trigger-validation

# set up the shell environment
source setup.sh

# install CMSSW (only has to be done once per campaign)
law run SetupCMSSWForCampaign --campaign ul_2016preVFP --cmssw-threads 4

# run the whole workflow with one command
law run PlotEfficiencyComparisonWrapper --campaign ul_2016preVFP --processes emb,dyjets_ll --workers 5
```

You find the output plots of this command in `data/store/<version>/PlotEfficiencyComparison/<campaign>`. If you didn't further specify the version tag via the `--version` argument, `version` should have the value `v1`.


## Run parts of the workflow

You can also run parts of the workflow with custom wrappers.

The command
```bash
law run ProduceTauTriggerNtuplesWrapper --processes emb,dyjets_ll --campaign ul_2016preVFP --workers 5
```
triggers the production of the custom NTuples used for this evaluation. These jobs run remotely on a HTCondor cluster.

The command
```bash
law run MergeTauTriggerNtuplesWrapper --processes emb,dyjets_ll --campaign ul_2016preVFP --workers 5
```
merges the produced NTuples together. The trigger information stored in these NTuples is skimmed and the merged event tables are stored `.parquet` files.

The command
```bash
law run CreateCutflowHistogramsWrapper --processes emb,dyjets_ll --campaign ul_2016preVFP --workers 5
```
produces cutflow histograms for each trigger and for each dataset.

The command
```bash
law run PlotEfficiencyComparisonWrapper --processes emb,dyjets_ll --campaign ul_2016preVFP --workers 5
```
produces the final comparison plots. Here, you can specify different options for ...

- ... producing different filetypes with `--extension` (e.g. `pdf`, `png`, `jpg`, `svg`),

- ... defining pre- and postfixes with `--prefix` and `--postfix` for custom filenames.
