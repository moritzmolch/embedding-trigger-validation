from abc import ABCMeta, abstractmethod
import law
import law.contrib.wlcg
from law.util import create_hash, readable_popen
import luigi
import os
import shlex
from typing import Any, Dict, List, Optional, Union

from emb_trigger_validation.cmssw import CMSSWSandbox
from emb_trigger_validation.tasks.base import BaseTask, ConfigTask


class CMSSWCommandTask(BaseTask, metaclass=ABCMeta):

    cmssw_threads = luigi.IntParameter(
        description="number of threads which are used to compile the CMSSW release; default: 1",
        default=1,
    )

    def __init__(self, *args, **kwargs):
        super(CMSSWCommandTask, self).__init__(*args, **kwargs)
        self._sandbox = CMSSWSandbox(
            self,
            self.cmssw_parent_dir(),
            self.cmssw_release(),
            self.cmssw_arch(),
            custom_packages_script=self.cmssw_custom_packages_script(),
        )

    @abstractmethod
    def cmssw_parent_dir(self) -> str:
        """
        Path to the directory, where CMSSW is installed within.

        Abstract method, that must be implemented by subclasses.

        :returns: path that contains the CMSSW release
        :rtype:   str
        """
        return NotImplemented

    @abstractmethod
    def cmssw_release(self) -> str:
        """
        Name of the CMSSW release.

        Abstract method, that must be implemented by subclasses.

        :returns: name of the CMSSW release
        :rtype:   str
        """
        return NotImplemented

    @abstractmethod
    def cmssw_arch(self) -> str:
        """
        Architecture that the CMSSW release will be compiled for.

        Abstract method, that must be implemented by subclasses.

        :returns: architecture for compiling CMSSW
        :rtype:   str
        """
        return NotImplemented

    def cmssw_path(self) -> str:
        """
        TODO add documentation
        """
        return os.path.join(self.cmssw_parent_dir(), self.cmssw_release())

    def cmssw_custom_packages_script(self) -> Union[str, None]:
        """
        Path to a custom script for installing additional packages in the CMSSW release.

        The script is executed after the installation of the base release and the release is compiled once again
        after having pulled the additional packages.

        By default, this method returns `None`. Subclasses can change the implementation of this method, so that it
        returns a path to a custom install script.

        :returns: path to install script for custom CMSSW packages or `None` if no custom script is provided
        :rtype:   str | None
        """
        return None

    def run_command(self, cmd: List[str], popen_kwargs: Optional[Dict[str, Any]] = None):
        """
        Run a shell command in a CMSSW environment, which has been set up with `cmsenv`.

        The command has to be passed as a list of of its parts, which are meant to belong together, so that the command
        can be properly quoted by :py:class:`subprocess.Popen`.

        The parameter `popen_kwargs` obtains a dictionary with values for keyword arguments of
        :py:class:`subprocess.Popen`. For a full list of available options, look at 
        https://docs.python.org/3/library/subprocess.html#popen-constructor. Note that the values `shell=False`,
        `executable=None` and `env=env` (with `env` being the environment provided by the sandbox) are going to be
        fixed regardless of the settings passed to `popen_kwargs`.

        :param cmd: command that is being executed
        :type cmd: List[str]

        :param popen_kwargs: explicit values for keyword arguments of `Popen`, optional
        :type popen_kwargs: Dict[str, Any] | None

        :raises RuntimeError: when the command has a non-zero exit code
        """

        # get the environment
        env = self._sandbox.get_env()

        # fix some values of the keyword arguments of Popen
        popen_kwargs = popen_kwargs or {}
        popen_kwargs.update({
            "shell": False,
            "executable": None,
            "env": env,
        })

        # run the command
        with self.publish_step("run command {}".format(shlex.join(cmd))):
            p, lines = readable_popen(cmd, **popen_kwargs)

            while True:
                for line in lines:
                    self.publish_message(line)
                if p.poll() is not None:
                    break

            if p.returncode != 0:
                raise RuntimeError("command failed with exit code {}".format(p.returncode))


class SetupCMSSWForConfig(CMSSWCommandTask, ConfigTask):

    def cmssw_parent_dir(self) -> str:
        cmssw_hash = create_hash((self.cmssw_release(), self.cmssw_arch(), self.cmssw_custom_packages_script()))
        return os.path.join(os.environ["ETV_SOFTWARE_PATH"], "cmssw", "{}_{}".format(self.cmssw_release(), cmssw_hash))

    def cmssw_release(self) -> str:
        return self.config_inst.x.cmssw.release

    def cmssw_arch(self) -> str:
        return self.config_inst.x.cmssw.arch

    def cmssw_custom_packages_script(self) -> Union[str, None]:
        path = self.config_inst.x.cmssw.get("custom_packages_script", None)
        if path is not None:
            path = os.path.expandvars(path)
        return path

    def output(self):
        return law.LocalDirectoryTarget(os.path.join(self.cmssw_parent_dir(), self.cmssw_release()))

    def run(self):
        # trigger setup by requesting to create the sandbox environment
        self._sandbox.get_env()
