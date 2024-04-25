#!/usr/bin/env bash


# bootstrap function for a generic HTCondor job
bootstrap_htcondor () {

    # entry point
    export HOME="${LAW_JOB_HOME}"
    export USER="{{etv_user}}"
    echo "home dir with bootstrap script:  ${LAW_JOB_HOME}"
    echo "user:                            ${USER}"
    echo "temporary directory:             ${TMPDIR}"

    export ORIG_CONDOR_TMPDIR="${TMPDIR}"

    # set project variables that are needed before sourcing the environment
    export ETV_REMOTE="1"
    export ETV_BASE="${LAW_JOB_HOME}/{{etv_repo_name}}"

    # patterns and URIs for external bundles that have to be pulled and unpacked
    export ETV_REPO_URIS="{{etv_repo_uris}}"
    export ETV_REPO_PATTERN="{{etv_repo_pattern}}"
    export ETV_CONFIG_DATA_URIS="{{etv_config_data_uris}}"
    export ETV_CONFIG_DATA_PATTERN="{{etv_config_data_pattern}}"
    export ETV_CMSSW_URIS="{{etv_cmssw_uris}}"
    export ETV_CMSSW_PATTERN="{{etv_cmssw_pattern}}"

    # paths for sourcing grid access and access to wlcg tools before the full environment has been loaded
    local grid_wn_script="/cvmfs/grid.cern.ch/centos7-wn-4.0.5-1_umd4v1/etc/profile.d/setup-c7-wn-example.sh"
    local wlcg_tools_script="${LAW_JOB_HOME}/{{wlcg_tools}}"

    # unpack the analysis repository in a sub-shell
    (
        mkdir -p "${ETV_BASE}" &&
        cd "${ETV_BASE}" &&
        source "${grid_wn_script}" "" &&
        source "${wlcg_tools_script}" "" &&
        law_wlcg_get_file "${ETV_REPO_URIS}" "${ETV_REPO_PATTERN}" "repo.tgz" &&
        tar -xzf "repo.tgz" &&
        rm "repo.tgz"
    ) || return "$?"
 
    # unpack the config data a sub-shell
    (
        mkdir -p "${ETV_BASE}/data/config" &&
        cd "${ETV_BASE}/data/config" &&
        source "${grid_wn_script}" "" &&
        source "${wlcg_tools_script}" "" &&
        law_wlcg_get_file "${ETV_CONFIG_DATA_URIS}" "${ETV_CONFIG_DATA_PATTERN}" "config_data.tgz" &&
        tar -xzf "config_data.tgz" &&
        rm "config_data.tgz"
    ) || return "$?"
 
    # source the default repo environment
    source "${ETV_BASE}/setup.sh" "" || return "$?"

    export TMPDIR="${ORIG_CONDOR_TMPDIR}"

    return 0
}


# job entry point
bootstrap_{{etv_bootstrap_name}} "$@"