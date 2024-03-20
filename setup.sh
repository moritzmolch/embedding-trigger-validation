#!/usr/bin/env bash


action () {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # base path of this project
    export ETV_BASE_PATH="${this_dir}"

    # auxiliary variables for special paths
    export ETV_CONFIG_PATH="${ETV_BASE_PATH}/config"
    export ETV_MODULES_PATH="${ETV_BASE_PATH}/modules"
    export ETV_JOBS_PATH="${ETV_BASE_PATH}/jobs"
    export ETV_LOCAL_STORE_PATH="${ETV_BASE_PATH}/store"

    # default values for the WLCG store location
    export ETV_WLCG_STORE_NAME="wlcg_fs_gridka"
    export ETV_WLCG_STORE_PATH="$(basename "${ETV_BASE_PATH}" )/store"

    # set up software stack
    source "/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh"

    # additional dependencies (law, order, scinum)
    export PYTHONPATH="${ETV_MODULES_PATH}:${ETV_MODULES_PATH}/law:${ETV_MODULES_PATH}/order:${ETV_MODULES_PATH}/scinum:${PYTHONPATH}"
    export PATH="${ETV_MODULES_PATH}/law/bin:${PATH}"

    # configuration and output paths for law
    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"

    law index --quiet
    source "$( law completion )" ""
}


action "$@"
