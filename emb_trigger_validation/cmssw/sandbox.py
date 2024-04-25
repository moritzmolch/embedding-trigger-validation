# coding: utf-8

"""
Sandbox for handling CMSSW environments
"""

import json
import os
import shlex
import shutil
from typing import Dict, List, Optional

from law import Task
from law.logger import get_logger
from law.util import create_hash, readable_popen

from emb_trigger_validation.paths import CACHE_DIR

logger = get_logger(__name__)


class CMSSWSandbox():
    """
    Interface between the python codebase of this package and a clean CMSSW environment.

    Instances of this class are used to setup CMSSW releases, fetch the shell environment being set up by
    `scramv1 runtime -sh`, and cache it in a file. The environment is evaluated in a completely clean environment
    independent of the software setup used for executing the code of this package. By that, it is also possible to
    execute commands in the CMSSW environment for releases, which are based on `python2`. For now, this sandbox
    just provides an environment in the form of a dictionary, which can be used in the context of executing commands
    with the tools from the :py:class:`subprocess` package.

    :note: Despite this class is called a "sandbox", it is not a sandbox in the sense of :py:class:`law.SandboxTask`.
    This sandbox is not meant to provide an environment, in which a `law ...` command can be executed. Instead, the
    environment created by this sandbox should be used when executing commands with the :py:package:`subprocess`
    package for the `env` parameter.

    :param task: the task which uses this sandbox
    :type task: law.Task

    :param parent_dir: path to the parent directory of the CMSSW release
    :type parent_dir: str

    :param release: name of the CMSSW release
    :type release: str

    :param arch: software architecture which this release will be compiled for
    :type arch:

    :param custom_packages_script: path to a custom shell script, which includes directives to install additional
                                   packages; optional
    :type custom_packages_script: str | None

    :param threads: number of threads which are used for compiling the release
    :type threads: int
    """

    def __init__(
        self,
        task: Task,
        parent_dir: str,
        release: str,
        arch: str,
        custom_packages_script: Optional[str] = None,
        threads: int = 1,
    ):
        # tasks to which this sandbox is connected
        self._task = task

        # base attributes of the CMSSW release
        self._parent_dir = parent_dir
        self._release = release
        self._arch = arch
        self._custom_packages_script = custom_packages_script
        self._threads = threads

        # remote task arguments
        self._is_remote = os.environ.get("ETV_REMOTE", "0") == "1"
        self._tarball_uris = os.environ.get("ETV_CMSSW_URIS", None)
        self._tarball_pattern = os.environ.get("ETV_CMSSW_PATTERN", None)

        # check that tarball information is available if this is a remote job
        if self._is_remote:
            if self._tarball_uris is None or self._tarball_pattern is None:
                raise OSError("obtained values '{}' for tarball URIs and '{}' for tarball patterns despite being inside a remote job".format(self._tarball_uris, self._tarball_pattern))

        # set the path to the CMSSW setup executable
        self._cmssw_setup_exec = os.path.join(os.path.abspath(os.path.dirname(__file__)), "cmssw_setup.sh")        

        # set the path to the directory for caching the CMSSW environment
        self._env_cache_dir = os.path.join(CACHE_DIR, "cmssw", "envs")

        # calculate a unique hash for caching the environment of the CMSSW release
        self._hash = create_hash((self._parent_dir, self._release, self._arch, self._custom_packages_script))
        self._env_cache_file = os.path.join(self._env_cache_dir, "{}.json".format(self._hash))

    def _build_bash_command_in_clean_env(self, cmd: str) -> List[str]:
        # get the correct env command
        env_cmd = shutil.which("env")

        # embed the bash commands in a clean environment
        cmd_clean = [
            env_cmd,
            "-i",
            "HOME={}".format(os.environ["HOME"]),
            "USER={}".format(os.environ["USER"]),
            "X509_USER_PROXY={}".format(os.environ.get("X509_USER_PROXY", "")),
        ]

        # add all ETV_* environment variables
        cmd_clean.extend([
            "{}={}".format(variable, os.environ[variable]) for variable in os.environ if variable.startswith("ETV_")
        ])

        # append the bash command
        cmd_clean.extend([
            "bash",
            "-c",
            cmd,
        ])

        logger.debug("{}.{}: set up command {}".format(self.__class__.__name__, self._build_bash_command_in_clean_env.__name__, cmd_clean))

        return cmd_clean

    def _build_cmssw_setup_command(self) -> str:

        # command for execution inside bash to set up CMSSW
        cmd_cmssw = [
            "source",
            self._cmssw_setup_exec,
            "--base-path",
            self._parent_dir,
            "--release",
            self._release,
            "--arch",
            self._arch,
            "--threads",
            str(self._threads),
        ]
        if self._custom_packages_script is not None:
            cmd_cmssw.extend([
                "--custom-packages-script",
                self._custom_packages_script,
            ])

        # add tarball information for remote jobs
        if self._is_remote:
            cmd_cmssw.extend([
                "--remote",
                "--tarball-uris",
                self._tarball_uris,
                "--tarball-pattern",
                self._tarball_pattern,
            ])

        # convert the command list into a properly quoted string
        cmd_cmssw = shlex.join(cmd_cmssw)
        
        logger.debug("{}.{}: set up command {}".format(self.__class__.__name__, self._build_cmssw_setup_command.__name__, cmd_cmssw))

        return cmd_cmssw

    def _build_dump_env_command(self, env_file: str) -> str:
        # python command for execution inside bash to capture the environment and dump it to a file
        cmd_env = shlex.join([
            "python",
            "-c",
            "; ".join([
                "import json",
                "import os",
                "f = open(\"{}\", mode=\"w\")".format(env_file),
                "json.dump(dict(os.environ), f)",
                "f.close()",
            ])
        ])

        logger.debug("{}.{}: set up command {}".format(self.__class__.__name__, self._build_dump_env_command.__name__, cmd_env))

        return cmd_env

    def get_env(self) -> Dict[str, str]:
        """
        TODO add documentation
        """
        # set the path to the environment file

        if not os.path.exists(self._env_cache_file):
            # dump the environment into a cache file
            
            # ensure that the parent directory of the cache file exists
            if not os.path.exists(self._env_cache_dir):
                os.makedirs(self._env_cache_dir)

            # construct the command for sourcing the CMSSW environment
            # - source profile scripts to set the base paths
            # - set up the CMSSW release
            # - dump the environment into the cache file
            # - execute the bash command in a clean environment
            cmd = self._build_bash_command_in_clean_env(
                " && ".join([
                    "source /etc/profile",
                    self._build_cmssw_setup_command(),
                    self._build_dump_env_command(self._env_cache_file),
                ])
            )

            logger.debug("{}.{}: run command {}".format(self.__class__.__name__, self.get_env.__name__, cmd))

            # execute the command to dump the environment
            with self._task.publish_step("setup CMSSW environment"):
                p, lines = readable_popen(cmd, shell=False)

                while True:
                    for line in lines:
                        self._task.publish_message(line)
                    if not p.poll() is None:
                        break

                if p.returncode != 0:
                    raise RuntimeError("command failed with exit code {}".format(p.returncode))

        # load the environment from the cache file
        with open(self._env_cache_file, mode="r") as f:
            env = json.load(f)

        return env
