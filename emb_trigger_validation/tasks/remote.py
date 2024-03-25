from collections import OrderedDict
import law
import law.contrib.git
import law.contrib.htcondor
import law.contrib.tasks
import law.contrib.wlcg
from law.util import law_src_path, merge_dicts
import luigi
import os

from emb_trigger_studies.tasks.framework.base import BaseTask
from emb_trigger_studies.tasks.framework.bundle import BundleCMSSW, BundleMamba, BundleRepository


class BaseHTCondorWorkflow(BaseTask, law.contrib.htcondor.HTCondorWorkflow):

    htcondor_universe = luigi.Parameter(
        default="docker",
        description="universe of the job, can be for example 'docker' or 'vanilla'; default: 'docker'",
        significant=False,
    )

    htcondor_docker_image = luigi.Parameter(
        default="mschnepf/slc7-condocker",
        description=(
            "docker image that the job runs inside, must be chosen if 'htcondor_universe' is set to 'docker'; default: 'mschnepf/slc7-condocker'"
        ),
        significant=False,
    )

    htcondor_request_cpus = luigi.IntParameter(
        default=1,
        description="number of CPU cores to use for this job; default: 1",
        significant=False,
    )

    htcondor_request_gpus = luigi.IntParameter(
        default=0,
        description="number of GPU instances to use for this job; default: 0",
        significant=False,
    )

    htcondor_request_memory = law.BytesParameter(
        default="4GB",
        description="allocated memory for this job, the value can be accompained by the unit, e.g. '4 GB'; default: '4 GB'",
        unit="MB",
        significant=False,
    )

    htcondor_request_disk = law.BytesParameter(
        default="15GB",
        description="allocated hard drive storage for this job, the value can be accompained by the unit, e.g. '4 GB'; default: '15 GB'",
        unit="kB",
        significant=False,
    )

    htcondor_request_walltime = luigi.IntParameter(
        default=86400,
        description="maximal wall time of the job (duration of the actual job) in s; default: 86400",
        significant=False,
    )

    htcondor_remote_job = luigi.BoolParameter(
        default=False,
        description="remote job that can be run outside the local cluster; default: False",
        significant=False,
    )

    htcondor_requirements = luigi.Parameter(
        default="(Target.ProvidesIO =?= True) && (Target.ProvidesCPU =?= True)",
        description=(
            "custom requirements for the job that have to be fulfilled by machines that this job is submitted to; default: '(Target.ProvidesIO =?= True) && (Target.ProvidesCPU =?= True)'"
        ),
        significant=False,
    )

    htcondor_accounting_group = luigi.Parameter(
        default="cms.higgs",
        description="accounting group to which the person who is submitting the job belongs to; default: 'cms.higgs'",
        significant=False,
    )

    htcondor_run_as_owner = luigi.BoolParameter(
        default=True,
        description="job is run by the user account; default: True",
        significant=False,
    )

    # get the proxy file directly with law wlcg package
    htcondor_x509userproxy = law.contrib.wlcg.get_vomsproxy_file()

    # htcondor_* parameters have to be ignored when branches of the workflow are executed, otherwise the execution of
    # the task inside the batch job likely fails
    exclude_params_branch = {
        "htcondor_universe",
        "htcondor_docker_image",
        "htcondor_requirements",
        "htcondor_request_cpus",
        "htcondor_request_gpus",
        "htcondor_request_memory",
        "htcondor_request_walltime",
        "htcondor_request_disk",
        "htcondor_remote_job",
        "htcondor_accounting_group",
        "htcondor_run_as_owner",
        "htcondor_x509userproxy",
    }

    def htcondor_workflow_requires(self):
        reqs = OrderedDict(law.contrib.htcondor.HTCondorWorkflow.htcondor_workflow_requires(self))
        reqs["BundleRepository"] = BundleRepository.req(self)
        reqs["BundleCMSSW"] = BundleCMSSW.req(self)
        return reqs

    def htcondor_output_directory(self):
        return self.local_target("jobs", is_dir=True)

    def htcondor_create_job_file_factory(self, **kwargs):
        kwargs = merge_dicts(
            self.htcondor_job_file_factory_defaults, {"universe": self.htcondor_universe}, kwargs
        )
        factory = super(BaseHTCondorWorkflow, self).htcondor_create_job_file_factory(**kwargs)
        factory.is_tmp = False  # TODO for testing purposes, remove later
        return factory

    def htcondor_bootstrap_file(self):
        return os.path.join(os.path.expandvars("${ETS_BOOTSTRAP_PATH}"), "htcondor_bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):

        # create directory for log files if it doesn't exist
        log_dir = os.path.join(
            self.htcondor_output_directory(),
            "logs",
        )
        log_target = law.LocalDirectoryTarget(log_dir)
        if not log_target.exists():
            log_target.touch()

        # append law's wlcg tool script to the collection of input files
        # needed for setting up the software environment
        config.input_files["wlcg_tools"] = law_src_path(
            "contrib/wlcg/scripts/law_wlcg_tools.sh"
        )

        # contents of the HTCondor submission file

        # job environment: docker image and requirements
        # config.custom_content.append(("universe", self.htcondor_universe))
        config.custom_content.append(("docker_image", self.htcondor_docker_image))
        if self.htcondor_requirements != law.NO_STR:
            config.custom_content.append(("requirements", self.htcondor_requirements))

        # set paths for log files
        # enforce that STDOUT and STDERR are not streamed to the submission machine
        # while the job is running
        config.custom_content.append(
            (
                "log",
                os.path.join(
                    log_dir, "{0:d}_{1:d}To{2:d}_log.txt".format(job_num, branches[0], branches[-1])
                ),
            )
        )
        config.custom_content.append(
            (
                "output",
                os.path.join(
                    log_dir,
                    "{0:d}_{1:d}To{2:d}_output.txt".format(job_num, branches[0], branches[-1]),
                ),
            )
        )
        config.custom_content.append(
            (
                "error",
                os.path.join(
                    log_dir,
                    "{0:d}_{1:d}To{2:d}_error.txt".format(job_num, branches[0], branches[-1]),
                ),
            )
        )

        # whether to stream the output and the error of the remote job directly to the submitting machine
        # set this to 'True' for testing purposes only
        config.custom_content.append(("stream_output", False))
        config.custom_content.append(("stream_error", False))

        # resources and runtime
        config.custom_content.append(("request_cpus", self.htcondor_request_cpus))
        if self.htcondor_request_gpus > 0:
            config.custom_content.append(("request_gpus", self.htcondor_request_gpus))
        config.custom_content.append(("request_memory", self.htcondor_request_memory))
        config.custom_content.append(("request_disk", self.htcondor_request_disk))
        config.custom_content.append(("+RequestWalltime", self.htcondor_request_walltime))
        if self.htcondor_remote_job:
            config.custom_content.append(("+RemoteJob", self.htcondor_remote_job))

        # user information: accounting group and VOMS proxy
        config.custom_content.append(("accounting_group", self.htcondor_accounting_group))
        if self.htcondor_run_as_owner:
            config.custom_content.append(("run_as_owner", self.htcondor_run_as_owner))
        config.custom_content.append(("x509userproxy", self.htcondor_x509userproxy))

        # get the URIs of the bundles
        reqs = self.htcondor_workflow_requires()

        def get_bundle_info(task):
            uris = task.output().dir.uri(cmd="filecopy", return_all=True)
            pattern = os.path.basename(task.get_file_pattern())
            return ",".join(uris), pattern

        # render variables in bootstrap script

        config.render_variables["etv_user"] = os.environ["USER"]
        config.render_variables["etv_bootstrap_name"] = "base"

        uris, pattern = get_bundle_info(reqs["BundleRepository"])
        config.render_variables["etv_repo_name"] = os.path.basename(os.environ["ETV_BASE_PATH"])
        config.render_variables["etv_repo_uris"] = uris
        config.render_variables["etv_repo_pattern"] = pattern

        return config

    def htcondor_use_local_scheduler(self):
        # always use a local scheduler in remote jobs
        return True
