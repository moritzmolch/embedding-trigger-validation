#!/usr/bin/env bash


# bootstrap function for a generic HTCondor job
bootstrap_htcondor_base () {

    # entry point
    export HOME="${LAW_JOB_HOME}"
    export USER="{{etv_user}}"
    echo "home dir with bootstrap script:  ${LAW_JOB_HOME}"
    echo "user:                            ${USER}"
    echo "temporary directory:             ${TMPDIR}"

    export ORIG_CONDOR_TMPDIR="${TMPDIR}"

    # set project variables that are needed before sourcing the environment
    export ETV_REMOTE="1"
    export ETV_BASE_PATH="${LAW_JOB_HOME}/{{etv_repo_name}}"

    # paths for sourcing grid access and access to wlcg tools before the full environment has been loaded
    local grid_wn_script="/cvmfs/grid.cern.ch/centos7-wn-4.0.5-1_umd4v1/etc/profile.d/setup-c7-wn-example.sh"
    local wlcg_tools_script="${LAW_JOB_HOME}/{{wlcg_tools}}"

    # unpack the analysis repository in a sub-shell
    (
        mkdir -p "${ETV_BASE_PATH}" &&
        cd "${ETV_BASE_PATH}" &&
        source "${grid_wn_script}" "" &&
        source "${wlcg_tools_script}" "" &&
        law_wlcg_get_file "{{ets_repo_uris}}" "{{ets_repo_pattern}}" "repo.tgz" &&
        tar -xzf "repo.tgz" &&
        rm "repo.tgz"
    ) || return "$?"
 
    # source the default repo environment
    source "${ETV_BASE_PATH}/setup.sh" "" || return "$?"

    export TMPDIR="${ORIG_CONDOR_TMPDIR}"

    return 0
}


# job entry point
bootstrap_htcondor_{{ets_bootstrap_name}} "$@"