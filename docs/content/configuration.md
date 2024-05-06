# Configuration


## Central configuration file

The central configuration file can be found at `config/config.yaml`.

The `campaigns` section represents a list of definitions of a production campaign. For example, this could be a data-taking era like *2016preVFP* for Run2, or a test production campaign of embedding files. The configuration should look like this:

```yaml
campaigns:

- name: ul_2016preVFP
  label: "2016preVFP"
  process_config_files:
  - ./processes.yaml
  dataset_config_files:
  - ./2016preVFP/datasets_emb.yaml
  - ./2016preVFP/datasets_dyjets_ll.yaml
  channel_config_files:
  - ./2016preVFP/channels.yaml
  cmssw:
    release: CMSSW_10_6_28_patch1
    arch: slc7_amd64_gcc700
    custom_packages_script: ${ETV_BASE}/scripts/cmssw_packages_embedding.sh

- name: ul_2016postVFP
  label: "2016postVFP"
  process_config_files:
  - ./processes.yaml
  dataset_config_files:
  - ./2016postVFP/datasets_emb.yaml
  - ./2016postVFP/datasets_dyjets_ll.yaml
  channel_config_files:
  - ./2016preVFP/channels.yaml
  cmssw:
    release: CMSSW_10_6_28_patch1
    arch: slc7_amd64_gcc700
    custom_packages_script: ${ETV_BASE}/scripts/cmssw_packages_embedding.sh
```

Each list item has to contain the keys `name` and `label`. The name is used as identifier for the campaign, the label is shown in the final output plots. Moreover, one has to define paths to additional configuration files, which are provided as lists for the entries `process_config_files`, `dataset_config_files` and `channel_config_files`. These files contain information about the used dataset files (e.g. the locations of the Drell-Yan and embedding sample files), some configuration for the "processes" embedding and Drell-Yan production (which is mostly needed for plotting) and information about the channels (also needed for plotting and for declaring the trigger paths under investigation).

Note that a CMSSW configuration must be provided for each campaign as it is needed to produce the NTuple files. Do also not change the `custom_packages_script` path without a good reason since it pulls the `TauTriggerNtuplizer` packages, on which the NTuple production step relies.

To reduce redundancy, there is the possibility to define default values, which are propagated to all campaign objects in the list, if these values have not been set there. In the existing configuration, this is done for the declaration of the CMSSW release:

```yaml
default:
  cmssw:
    release: CMSSW_10_6_28_patch1
    arch: slc7_amd64_gcc700
    custom_packages_script: ${ETV_BASE}/scripts/cmssw_packages_embedding.sh

campaigns:
- name: ul_2016preVFP
  label: "2016preVFP"
  process_config_files:
  - ./processes.yaml
  dataset_config_files:
  - ./2016preVFP/datasets_emb.yaml
  - ./2016preVFP/datasets_dyjets_ll.yaml
  channel_config_files:
  - ./2016preVFP/channels.yaml

- ...
```


## Process configuration

The files declared in the list `process_config_files` contain the information about the physics process. This is mainly needed to define the the color and the label of the process in the final plots. The name of the process is also used to define that a dataset belongs to one or the other physics process. Colors are declared as lists of three integers, which define the RGB code of the used color.

```yaml
processes:

- name: emb
  label: $\mu \to \tau$-embedding
  color: [254, 198, 21]

- name: dyjets_ll
  label: Drell-Yan ($\to \tau\tau$)
  color: [61, 122, 253]
```


## Dataset configuration

The dataset configuration files are located at the paths defined in the entry `dataset_config_files`. Each dataset has to have a name and has to be assigned to a process, which is defined in the process configuration and identified by its name. Additionally, one has to provide a `metadata_fetcher`, which describes a method how the tool figures out the paths of the files, of which the dataset consists. These paths could for example be fetched from the [CMS Data Aggregation Service](https://cmsweb.cern.ch/das/) or from the files stored in the [repository managing the embedding production workflow at KIT](https://github.com/KIT-CMS/gc_configs_for_embedding). One can also declare if a sample contains data events and define tags for categorizing the sample.

```yaml
datasets:

- name: emb_2018A_et
  process: emb
  is_data: True
  tags:
  - embedding
  metadata_fetcher:
    name: DASMetadataFetcher
    kwargs:
      key: /EmbeddingRun2018A/ElTauFinalState-inputDoubleMu_106X_ULegacy_miniAOD-v1/USER
      dbs_instance: prod/phys03
```

Also here, you can define default values beforehand, which are propagated to all datasets in the list.


### Metadata fetchers

In general, the `metadata_fetcher` entry must have two subentries, which are its name (with the key `name`) and the values of the keyword arguments (with the key `kwargs`) being passed to the corresponding class. The following three approaches to get datasets are implemented

- The `DASMetadataFetcher` can be used to pull the filelists from the CMS Data Aggregation Service. Here, `kwargs` must contain the DBS key of the dataset as value of `key`. Optionally, one can also declare the DBS instance (key `dbs_instance`), on which the samples are available. For example, embedding samples have to be queried on `prod/phys03`. The default instance is `prod/global`.

- The `GCConfigsMetadataFetcher` can be used to pull the filelists from the repository, which manages the workflow of the embedding production. The `kwargs` entry must contain a value for `rel_dbs_file_path`, which is the path of the DBS file containing the individual file locations relative to the root directory of the repository.

- The `YAMLMetadataFetcher` can be used to provide the filelists from a separate YAML file. The path to this file is given to the fetcher as value of the key `file_path` in the `kwargs` entry. The YAML file must contain the keys `filelist`, `n_events` for the total number of events and `n_files` for the total number of files of the dataset as well as a key `redirectors`, which defines redirectors where the files are available.


## Channel configuration

The channel configuration is mainly needed to define labels for the $\tau\tau$ decay modes and to declare the trigger paths under investigation. The configuration should then look like this:

```yaml
channels:

- name: et
  label: $\mathrm{e}\tau_{\\mathrm{h}}$
  triggers:
  - name: HLT_Ele27_WPTight_Gsf
  - name: HLT_Ele32_WPTight_Gsf
  - name: HLT_Ele32_WPTight_Gsf_DoubleL1EG
  - name: HLT_Ele35_WPTight_Gsf
  - name: HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1

- name: mt
  label: $\mu\tau_{\\mathrm{h}}$
  triggers:
  - name: HLT_IsoMu24
  - name: HLT_IsoMu27
  - name: HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1

- name: tt
  label: $\tau_{\\mathrm{h}}\\tau_{\\mathrm{h}}$
  triggers:
  - name: HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg
  - name: HLT_DoubleTightChargedIsoPFTauHPS35_Trk1_TightID_eta2p1_Reg
  - name: HLT_DoubleMediumChargedIsoPFTauHPS40_Trk1_TightID_eta2p1_Reg
  - name: HLT_DoubleTightChargedIsoPFTauHPS40_Trk1_eta2p1_Reg
```
