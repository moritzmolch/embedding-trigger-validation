#!/usr/bin/env bash


action () {

    # some convenient variable definitions
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # flags for the state of the environment
    export ETV_SETUP="${ETV_SETUP:-0}"
    export ETV_REMOTE="${ETV_REMOTE:-0}"

    # stage out if the setup has already been taking place
    if [[ "${ETV_SETUP}" = "1" ]]; then
        echo "project has already been set up, open a new shell to source this script again"
        return
    fi

    # important paths
    export ETV_BASE="${this_dir}"
    export ETV_MODULES="${ETV_BASE}/modules"
    export ETV_SOFTWARE="${ETV_BASE}/data/software"

    # set up software stack by using a recent LCG view
    source "/cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-centos7-gcc12-opt/setup.sh"

    # set up the GRID environment
    source "/cvmfs/grid.cern.ch/centos7-umd4-ui-211021/etc/profile.d/setup-c7-ui-python3-example.sh"
    #_setup_grid_variables || return "${?}"

    # additional dependencies (law, order, scinum)
    export PYTHONPATH="${ETV_BASE}:${ETV_MODULES}/law:${ETV_MODULES}/order:${ETV_MODULES}/scinum:${ETV_MODULES}/luigi:${ETV_SOFTWARE}/x86_64-centos7-gcc12-opt/lib/python3.9/site-packages:${PYTHONPATH}"
    export PATH="${ETV_BASE}/bin:${ETV_MODULES}/law/bin:${ETV_MODULES}/luigi/bin:${ETV_SOFTWARE}/x86_64-centos7-gcc12-opt/bin:${PATH}"

    # configuration and output paths for law
    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"

    if which law &> /dev/null; then
        law index --quiet
        source "$( law completion )" ""
    fi

    # set flag that environment has been set up
    export ETV_SETUP="1"

}


action "$@"
