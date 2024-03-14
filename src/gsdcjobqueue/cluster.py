import asyncio
import logging
import os
import random
import shutil
import socket
import sys
import tempfile
import weakref
import pwd

import dask
import yaml
from dask_jobqueue.htcondor import (
    HTCondorCluster,
    HTCondorJob,
    quote_arguments,
    quote_environment,
)
from .schedd import SCHEDD, SCHEDD_POOL, htcondor



logger = logging.getLogger(__name__)
fn = os.path.join(os.path.dirname(__file__), "config.yaml")

with open(fn) as f:
    defaults = yaml.safe_load(f)

dask.config.update(dask.config.config, defaults, priority="new")


def is_venv():
    return hasattr(sys, "real_prefix") or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix)


class GSDCCondorJob(HTCondorJob):
    executable = os.path.dirname(os.path.abspath(__file__)) + '/condor_exec.exe'
    config_name = "gsdccondor"
    known_jobs = set()
    env_name = os.path.basename(os.getenv('VIRTUAL_ENV', '.env'))

    def __init__(
        self,
        scheduler=None,
        name=None,
        *,
        ship_env,
        image,
        **base_class_kwargs,
    ):
        if ship_env:
            base_class_kwargs["python"] = f"{self.env_name}/bin/python"
            base_class_kwargs.setdefault(
                "worker_extra_args", list(dask.config.get("jobqueue.%s.worker_extra_args" % self.config_name))
            )
            base_class_kwargs["worker_extra_args"].extend(["--preload", "gsdcjobqueue.patch"])
        else:
            base_class_kwargs["python"] = "python"
        super().__init__(scheduler=scheduler, name=name, **base_class_kwargs)
        if self.log_directory:
            if not any(
                os.path.commonprefix([self.log_directory, p]) == p
                for p in GSDCCondorCluster.schedd_safe_paths
            ):
                raise ValueError(
                    f"log_directory must be a subpath of one of {GSDCCondorCluster.schedd_safe_paths} or else the schedd cannot write our logs back to the container"
                )

        self.job_header_dict.update(
            {
                "when_to_transfer_output": "ON_EXIT_OR_EVICT",
                "transfer_output_files": "",
                "container_image": f"{image}",
                "universe":"container",
                "accounting_group": "group_cms",
                "should_transfer_files": "YES",
            }
        )

class GSDCCondorCluster(HTCondorCluster):
    __doc__ = (
        HTCondorCluster.__doc__
        + """

    Additional GSDC parameters:
    ship_env: bool
        If True (default False), ship the ``/srv/.env`` virtualenv with the job and
        run workers from that environent. This allows user-installed packages
        to be available on the worker
    image: str
        Name of the singularity image to use (default: $COFFEA_IMAGE)
    transfer_input_files: str, List[str]
        Files to be shipped along with the job. They will be placed in the
        working directory of the workers, as usual for HTCondor. Any paths
        not accessible from the GSDC schedds (because of restrictions placed
        on remote job submission) will be copied to a temporary directory
        under ``/cms_scratch/$USER``.
    """
    )
    job_cls = GSDCCondorJob
    config_name = "gsdccondor"
    schedd_safe_paths = [
        os.path.expanduser("~"),
        "/cms_scratch",
    ]

    def __init__(self, **kwargs):
        hostname = socket.gethostname()
        self._port = random.randint(10000, 10100)
        kwargs.setdefault("scheduler_options", {})
        kwargs["scheduler_options"].setdefault("host", f"{hostname}:{self._port}")
        kwargs.setdefault("ship_env", False)
        kwargs.setdefault(
            "image", os.environ.get("COFFEA_IMAGE", "/cms/container_images/coffea-dask-cc7.sif")
        )
        self._ship_env = kwargs["ship_env"]
        infiles = kwargs.pop("transfer_input_files", [])
        if not isinstance(infiles, list):
            infiles = [infiles]
        self._transfer_input_files = infiles
        self.scratch_area = None
        super().__init__(**kwargs)

    def _build_scratch(self):
        # Depending on the size of the inputs this may take a long time
        tmproot = f"/cms_scratch/{pwd.getpwuid(os.geteuid())[0]}/"
        os.makedirs(tmproot, exist_ok=True)
        self.scratch_area = tempfile.TemporaryDirectory(dir=tmproot)
        infiles = []
        if self._ship_env:
            env_path = os.getenv('VIRTUAL_ENV', '/srv/.env')
            shutil.copytree(env_path, os.path.join(self.scratch_area.name, os.path.basename(env_path)))
            infiles.append(os.path.basename(env_path))
        for fn in self._transfer_input_files:
            fn = os.path.abspath(fn)
            if any(os.path.commonprefix([fn, p]) == p for p in self.schedd_safe_paths):
                # no need to copy these
                infiles.append(fn)
                continue
            basename = os.path.basename(fn)
            try:
                shutil.copy(fn, self.scratch_area.name)
            except IsADirectoryError:
                shutil.copytree(fn, os.path.join(self.scratch_area.name, basename))
            infiles.append(basename)
        return infiles

    def _clean_scratch(self):
        if self.scratch_area is not None:
            self.scratch_area.cleanup()

    async def _start(self):
        try:
            await super()._start()
        except OSError:
            raise RuntimeError(
                f"Likely failed to bind to local port {self._port}, try rerunning"
            )

        prepared_input_files = await self.loop.run_in_executor(
            None, self._build_scratch
        )
        self._job_kwargs.setdefault("job_extra_directives", {})
        self._job_kwargs["job_extra_directives"]["initialdir"] = self.scratch_area.name
        self._job_kwargs["job_extra_directives"]["transfer_input_files"] = ",".join(
            prepared_input_files
        )

    async def _close(self):
        await super()._close()
        await self.loop.run_in_executor(None, self._clean_scratch)