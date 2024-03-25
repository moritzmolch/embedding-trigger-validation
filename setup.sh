#!/usr/bin/env bash


_setup_grid_variables () {
    # adapted from /cvmfs/grid.cern.ch/centos7-umd4-ui-211021/etc/profile.d/setup-c7-ui-python3-example.sh grid-setup.sh
    local base="/cvmfs/grid.cern.ch/centos7-umd4-ui-211021"

    # VOMS-related variables
    export GLOBUS_THREAD_MODEL="${GLOBUS_THREAD_MODEL:-none}"
    export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
    export X509_VOMS_DIR="${X509_VOMS_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/vomsdir}"
    export X509_VOMSES="${X509_VOMSES:-/cvmfs/grid.cern.ch/etc/grid-security/vomses}"
    export X509_USER_PROXY="${X509_USER_PROXY:-/tmp/x509up_u$(/usr/bin/id -u)}"
    export VOMS_USERCONF="${VOMS_USERCONF:-${X509_VOMSES}}"

    # gfal-related variables
    export GFAL_CONFIG_DIR="${GFAL_CONFIG_DIR:-${base}/etc/gfal2.d}"
    export GFAL_PLUGIN_DIR="${GFAL_PLUGIN_DIR:-${base}/usr/lib64/gfal2-plugins}"

    # append binaries and libraries to paths
    export PATH="${base}/bin:${base}/sbin:${base}/usr/bin:${base}/usr/sbin:${PATH}"
    export LD_LIBRARY_PATH="${base}/lib64:${base}/lib:${base}/usr/lib64:${base}/usr/lib:${LD_LIBRARY_PATH}"
    export PYTHONPATH="${base}/usr/lib64/python3.6/site-packages:${base}/usr/lib/python3.6/site-packages:${PYTHONPATH}"
}


action () {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # flags for the state of the environment
    export ETV_SETUP="${ETV_SETUP:-0}"
    export ETV_REMOTE="${ETV_REMOTE:-0}"

    # stage out if the setup has already been taking place
    if [[ "${ETV_SETUP}" = "1" ]]; then
        echo "project has already been set up"
        return
    fi

    # base path of this project
    export ETV_BASE_PATH="${this_dir}"

    # auxiliary variables for special paths
    export ETV_CACHE_PATH="${ETV_BASE_PATH}/cache"
    export ETV_CONFIG_PATH="${ETV_BASE_PATH}/config"
    export ETV_JOBS_PATH="${ETV_BASE_PATH}/jobs"
    export ETV_LOCAL_STORE_PATH="${ETV_BASE_PATH}/store"
    export ETV_MODULES_PATH="${ETV_BASE_PATH}/modules"
    export ETV_SOFTWARE_PATH="${ETV_BASE_PATH}/software"

    # default values for the WLCG store location
    export ETV_WLCG_STORE_NAME="wlcg_fs_gridka"
    export ETV_WLCG_STORE_PATH="$(basename "${ETV_BASE_PATH}" )/store"

    # set up software stack
    source "/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh"

    _setup_grid_variables || return "${?}"

    # additional dependencies (law, order, scinum)
    export PYTHONPATH="${ETV_BASE_PATH}:${ETV_MODULES_PATH}/law:${ETV_MODULES_PATH}/order:${ETV_MODULES_PATH}/scinum:${ETV_MODULES_PATH}/luigi:${PYTHONPATH}"
    export PATH="${ETV_MODULES_PATH}/law/bin:${ETV_MODULES_PATH}/luigi/bin:${PATH}"

    # configuration and output paths for law
    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"

    if which law &> /dev/null; then
        law index --quiet
        source "$( law completion )" ""
    fi

}


action "$@"
