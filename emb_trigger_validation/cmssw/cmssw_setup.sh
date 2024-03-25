#!/usr/bin/env bash

# The structure of this setup script is inspired by https://github.com/riga/law/blob/master/law/contrib/cms/scripts/setup_cmssw.sh [Copyright (c) 2018-2024, Marcel Rieger]

# TODO add documentation
# TODO handle relative paths correctly


# global variables of the script
_CMSSW_BASE_PATH=""
_CMSSW_RELEASE=""
_CMSSW_ARCH=""
_CMSSW_THREADS=""
_CMSSW_HAS_CUSTOM_PACKAGES_SCRIPT=""
_CMSSW_CUSTOM_PACKAGES_SCRIPT=""
_CMSSW_IS_REMOTE=""
_CMSSW_TARBALL_URIS=""
_CMSSW_TARBALL_PATTERN=""

_info () {
    2>&1 echo "INFO    ${1}"
}


_warning () {
    2>&1 echo "WARNING ${1}"
}


_error () {
    2>&1 echo "ERROR   ${1}"
}


_setup_cmssw () {
    # set some local variables for shell type and paths to this file/directory
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local current_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local current_dir="$( cd "$( dirname "${current_file}" )" && pwd )"

    # internal function for setting up CMS defaults
    _cmsset_default () {
        export VO_CMS_SW_DIR="/cvmfs/cms.cern.ch"
        source "${VO_CMS_SW_DIR}/cmsset_default.sh"
        return "${?}"
    }

    # internal function for setting up WLCG dependencies
    _wlcg_setup () {
        source "/cvmfs/grid.cern.ch/centos7-umd4-ui-4.0.3-1_191004/etc/profile.d/setup-c7-ui-example.sh"
        return "${?}"
    }

    # define local variables with paths
    local base_path="${_CMSSW_BASE_PATH}"
    local cmssw_path="${_CMSSW_BASE_PATH}/${_CMSSW_RELEASE}"
    local cmssw_src_path="${cmssw_path}/src"
    local cmssw_lock_file="${base_path}/.cmssw-lock"
    local cmssw_install_file="${base_path}/.cmssw-install"

    if [[ "${_CMSSW_IS_REMOTE}" == "0" ]]; then

        # the release is installed if
        # - the src directory of the CMSSW installation exists
        # - the install file exists
        # - the lock file does not exist
        local is_installed;
        is_installed="$( [[ -d "${cmssw_src_path}" ]] && [[ -f "${cmssw_install_file}" ]] && [[ ! -f ${cmssw_lock_file} ]] && echo "1" || echo "0" )"

        # if the lockfile exists, wait a certain amount of time and occasionally check if it has been removed
        while [[ -f "${cmssw_lock_file}" ]]; do 
            local timestamp_lockfile; local timestamp_current;

            # read the timestamp from the logfile
            { read -r timestamp_lockfile < "${cmssw_lock_file}"; } 3< "${cmssw_lock_file}"

            # get the current timestamp
            timestamp_current="$(date +%s)"

            _warning "lockfile ${cmssw_lock_file} exists, waiting ..."

            # if the timestamp from the lockfile is older than 30 minutes, abort
            local diff="$(( timestamp_current - timestamp_lockfile ))"
            if [[ ${diff} -gt 1800  ]]; then
                _error "lockfile is older than 30 minutes, you are probably dealing with broken installation"
                _error "pleace resolve this issue manually"
                return "4"
            fi

            # sleep for 30 seconds and try it again
            _info "sleeping for 30 seconds ..."
            sleep 30
        done

        # start the installation if the release has not been installed yet
        if [[ "${is_installed}" = "0" ]]; then

            # if the parent of the release directory does not exist, create it
            if [[ ! -d "${base_path}" ]]; then
                _info "creating directory ${base_path}"
                mkdir -p "${base_path}"
            fi

            # create the lockfile with a timestamp
            _info "creating lockfile ${cmssw_lock_file}"
            date +%s > "${cmssw_lock_file}"

            # define variable for return code of installation subshells
            local ret_code;

            # install CMSSW release
            (
                _cmsset_default &&
                _wlcg_setup &&
                export SCRAM_ARCH="${_CMSSW_ARCH}" &&
                cd "${base_path}" &&
                _info "pulling release '${_CMSSW_RELEASE}'" &&
                scramv1 project CMSSW "${_CMSSW_RELEASE}" &&
                cd "${cmssw_src_path}" &&
                eval "$(scramv1 runtime -sh)" &&
                _info "building CMSSW with command 'scramv1 build -j ${_CMSSW_THREADS}'" &&
                scramv1 build -j "${_CMSSW_THREADS}" &&
                scramv1 build python
            ) && ret_code="${?}"

            # check if custom packages need to be installed
            if [[ ${_CMSSW_HAS_CUSTOM_PACKAGES_SCRIPT} = "1" && "${ret_code}" = "0" ]]; then

                # install CMSSW release
                (
                    _cmsset_default &&
                    _wlcg_setup &&
                    export SCRAM_ARCH="${_CMSSW_ARCH}" &&
                    cd "${cmssw_src_path}" &&
                    eval "$(scramv1 runtime -sh)" &&
                    _info "installing custom packages with script '${_CMSSW_CUSTOM_PACKAGES_SCRIPT}'" &&
                    source "${_CMSSW_CUSTOM_PACKAGES_SCRIPT}" &&
                    _info "building CMSSW with command 'scramv1 build -j ${_CMSSW_THREADS}'" &&
                    scramv1 build -j "${_CMSSW_THREADS}" &&
                    scramv1 build python
                ) && ret_code="${?}"

            fi

            # remove the lockfile
            rm -rf "${cmssw_lock_file}"

            # clean up if the installation has failed
            if [[ "${ret_code}" != 0 ]]; then
                _error "installation of release '${_CMSSW_RELEASE}' failed with exit code ${ret_code}"
                rm -rf "${cmssw_path}"
                return "5"
            fi

            date +%s > "${cmssw_install_file}"

            _info "successfully installed release '${_CMSSW_RELEASE}' under destination '${cmssw_path}'"

        fi

    else
        # in remote jobs, pull the tarball and unpack it

        # paths for sourcing grid access and access to wlcg tools before the full environment has been loaded
        local grid_wn_script="/cvmfs/grid.cern.ch/centos7-wn-4.0.5-1_umd4v1/etc/profile.d/setup-c7-wn-example.sh"
        local wlcg_tools_script="${ETV_MODULES_PATH}/law/law/contrib/wlcg/scripts/wlcg_tools.sh"

        # ensure that needed environment variables are set
        if [[ -z "${ETV_MODULES_PATH}" ]]; then
            _error "environment variable \$ETV_MODULES_PATH must be set"
            return "6"
        fi

        # ensure that the directory of the CMSSW release exists
        if [[ ! -d "${cmssw_path}" ]]; then
            mkdir -p "${cmssw_path}"
        fi

        # unpack the analysis repository in a sub-shell
        (
            cd "${cmssw_path}" &&
            source "${grid_wn_script}" "" &&
            source "${wlcg_tools_script}" "" &&
            law_wlcg_get_file "${_CMSSW_TARBALL_URIS}" "${_CMSSW_TARBALL_PATTERN}" "cmssw.tgz" &&
            tar -xzf "cmssw.tgz" &&
            rm "cmssw.tgz"
        ) || return "$?"

    fi

    # set up CMSSW defaults and WLCG environment
    _cmsset_default || return "${?}"
    _wlcg_setup || return "${?}"

    # source the CMSSW environment
    export SCRAM_ARCH="${_CMSSW_ARCH}"

    local ret_code;
    _info "set up CMSSW release '${_CMSSW_RELEASE}'"
    cd "${cmssw_src_path}" || (ret_code="${?}" && _error "directory ${cmssw_src_path} does not exist" && cd "${current_dir}" && return "${ret_code}" )
    eval "$(scramv1 runtime -sh)"
    cd "${current_dir}"

    return "0"
}


_parse_args () {

    # define local variables to fetch the command-line arguments
    local base_path
    local release
    local arch
    local threads
    local has_custom_packages_script
    local custom_packages_script
    local tarball
    local is_remote

    # loop over command-line inputs
    while [[ ${#} -gt 0 ]]; do

        case "${1}" in

            --base-path)
                base_path="${2}"
                shift
                shift
                ;;

            --release)
                release="${2}"
                shift
                shift
                ;;

            --arch)
                arch="${2}"
                shift
                shift
                ;;

            --threads)
                threads="${2}"
                shift
                shift
                ;;

            --custom-packages-script)
                custom_packages_script="${2}"
                shift
                shift
                ;;

            --remote)
                is_remote="1"
                shift
                ;;

            --tarball-uris)
                tarball_uris="${2}"
                shift
                shift
                ;;

            --tarball-pattern)
                tarball_pattern="${2}"
                shift
                shift
                ;;


            -*)
                _error "unknown command-line-argument ${1}"
                return "1"
                ;;
        esac
    done

    # required arguments: check if base path, release and arch have been set

    if [[ -z "${base_path}" ]]; then
        _error "no value obtained for option '--base-path'"
        return "2"
    fi

    if [[ -z "${release}" ]]; then
        _error "no value obtained for option '--release'"
        return "3"
    fi

    if [[ -z "${arch}" ]]; then
        _error "no value obtained for option '--arch'"
        return "4"
    fi

    if [[ "${is_remote}" = "1" ]]; then
        if [[ -z "${tarball_uris}" ]]; then
            _error "no value obtained for option '--tarball-uris' despite '--remote' has been declared"
            return "5"
        fi
        if [[ -z "${tarball_pattern}" ]]; then
            _error "no value obtained for option '--tarball-pattern' despite '--remote' has been declared"
            return "6"
        fi
    fi

    # optional arguments: set default values

    threads="${threads:-1}"

    if [[ -z "${custom_packages_script}" ]]; then
        has_custom_packages_script="0"
        custom_packages_script=""
    else
        has_custom_packages_script="1"
    fi

    # fill global variables with parsed arguments
    _CMSSW_BASE_PATH="${base_path}"
    _CMSSW_ARCH="${arch}"
    _CMSSW_RELEASE="${release}"
    _CMSSW_THREADS="${threads}"
    _CMSSW_HAS_CUSTOM_PACKAGES_SCRIPT="${has_custom_packages_script}"
    _CMSSW_CUSTOM_PACKAGES_SCRIPT="${custom_packages_script}"
    _CMSSW_IS_REMOTE="${is_remote:-0}"
    _CMSSW_TARBALL_URIS="${tarball_uris}"
    _CMSSW_TARBALL_PATTERN="${tarball_pattern}"

    return "0"
}


action () {

    # parse the command-line arguments and set global variables of this script
    _parse_args "$@" || return "${?}"

    # set up the CMSSW release
    _setup_cmssw || return "${?}"

}

action "$@"