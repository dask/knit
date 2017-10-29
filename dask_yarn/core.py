import os
import re
import socket
import atexit
from hashlib import sha1

from tornado import gen
from toolz import unique

from knit import Knit, CondaCreator, zip_path
from distributed import LocalCluster

global_packages = ['dask>=0.14', 'distributed>=1.16']

prog = re.compile('\w+')


def first_word(s):
    return prog.match(s).group()


class DaskYARNCluster(object):
    """
    Implements a dask cluster with YARN containers running the worker processes.
    A dask scheduler is started locally upon instantiation, but you must call
    ``start()`` to initiate the building of containers by YARN.
    
    Parameters
    ----------
    nn, nn_port, rm, rm_port, user, autodetect, lang: see knit.Knit
    env: str or None
        If provided, the path of a zipped conda env to put in containers. This
        can be a local zip file to upload, a zip file already on HDFS (hdfs://)
        or a directory to zip and upload. If not provided, a default environment
        will be built, containing dask.
    packages: list of str
        Packages to install in the env to provide to containers *if* env is 
        None. Uses conda spec for pinning versions. dask and distributed will
        always be included.
    channels: list of str
        If building an environment, pass these extra channels to conda using
        ``-c`` (i.e., in addition but of superior priority to any system
        default channels).
    conda_pars: dict
        Things to pass to CondaCreator
    ip: IP-like string or None
        Address for the scheduler to listen on. If not given, uses the system
        IP.
    """

    def __init__(self, autodetect=True, packages=None, ip=None, env=None,
                 channels=None, conda_pars=None, **kwargs):

        ip = ip or socket.gethostbyname(socket.gethostname())

        self.env = env
        self.application_master_container = None
        self.app_id = None
        self.channels = channels or []
        self.conda_pars = conda_pars

        try:
            self.local_cluster = LocalCluster(n_workers=0, ip=ip)
        except (OSError, IOError):
            self.local_cluster = LocalCluster(n_workers=0, scheduler_port=0,
                                              ip=ip)

        self.packages = list(
            sorted(unique((packages or []) + global_packages, key=first_word)))

        self.knit = Knit(autodetect=autodetect, **kwargs)

        atexit.register(self.stop)

    @property
    def scheduler_address(self):
        return self.local_cluster.scheduler_address

    def start(self, n_workers=1, cpus=1, memory=2048, checks=True,
              **kwargs):
        """
        Initiate workers. If required, environment is first built and uploaded
        to HDFS, and then a YARN application with the required number of
        containers is created.
        
        Parameters
        ----------
        n_workers: int
            How many containers to create
        cpus: int=1
            How many CPU cores is available in each container
        memory: int=2048
            Memory available to each dask worker (in MB)
        checks: bool=True
            Whether to run pre-flight checks before submitting app to YARN
        kwargs: passed to ``Knit.start()``
        
        Returns
        -------
        YARN application ID.
        """
        if self.env is None:
            c = CondaCreator(channels=self.channels, **(self.conda_pars or {}))
            env_name = 'dask-' + sha1(
                '-'.join(self.packages + self.channels).encode()).hexdigest()
            env_path = os.path.join(c.conda_envs, env_name)
            if os.path.exists(env_path + '.zip'):
                # zipfile exists, ready to upload
                self.env = env_path + '.zip'
            elif os.path.exists(env_path):
                # environment exists, can zip and upload
                self.env = zip_path(env_path)
            else:
                # create env from scratch
                self.env = c.create_env(env_name=env_name,
                                        packages=self.packages)
        elif (not self.env.endswith('.zip') and
              not self.env.startswith('hdfs://')):
            # given env directory, so zip it
            self.env = zip_path(self.env)

        # TODO: memory should not be total available?
        bn = os.path.basename(self.env)
        pathsep = '/'  # assume execution is always on posix
        pref = pathsep.join([bn, os.path.splitext(bn)[0]])  # like myenv.zip/myenv
        command = ('{pref}/bin/python {pref}/bin/dask-worker --nprocs=1 '
                   '--nthreads={cpus} --memory-limit={mem} --no-bokeh {addr} '
                   ''.format(cpus=cpus, mem=memory * 1e6, pref=pref,
                             addr=self.local_cluster.scheduler.address))

        files = [self.env] + kwargs.pop('files', [])
        app_id = self.knit.start(command, files=files,
                                 num_containers=n_workers, virtual_cores=cpus,
                                 memory=memory, checks=checks, **kwargs)
        self.app_id = app_id
        return app_id

    def remove_worker(self, container_id):
        """
        Stop worker and remove container

        Parameters
        ----------
        container_id

        Returns
        -------
        None
        """
        self.knit.remove_containers(container_id)

    @property
    def workers(self):
        """
        list of running container ids
        """

        # remove container ...00001 -- this is applicationMaster's container and
        # should not be removed or counted as a worker

        containers = list(self.knit.get_container_statuses())
        containers.sort()
        self.application_master_container = containers.pop(0)
        return containers

    @gen.coroutine
    def _start(self):
        pass

    def stop(self):
        """Kill the YARN application and all workers"""
        if self.knit:
            self.knit.kill()

    def add_workers(self, n_workers=1, cpus=1, memory=2048):
        """
        Non-blocking function to ask Yarn for more containers/dask-workers

        Parameters
        ----------
        n_workers: int
            number of containers to add (default: 1)

        cpus: int
            number of cpus (default: 1)
        memory: int
            amount of memory to allocate per container

        Returns
        -------
        None
        """

        self.knit.add_containers(num_containers=n_workers, virtual_cores=cpus,
                                 memory=memory)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        return "<Dask on YARN cluster {}:{}>".format(
            self.knit.conf['rm'], self.knit.conf['rm_port'])

    def close(self):
        """Stop the scheduler and workers"""
        self.stop()
        self.local_cluster.close()
