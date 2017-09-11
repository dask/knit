import os
import re
import socket
import atexit
from hashlib import sha1

from tornado import gen
from toolz import unique

from knit import Knit, CondaCreator
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
    nn, nn_port, rm, rm_port, user, autodetect: see knit.Knit
    env: str or None
        If provided, the path of a zipped conda env to put in containers
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
        self.channels = channels
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

    def start(self, n_workers, cpus=1, memory=2048):
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
        memory: int=4000
            Memory available to each dask worker (in MB)
        
        Returns
        -------
        YARN application ID.
        """
        c = CondaCreator(channels=self.channels, **(self.conda_pars or {}))
        if self.env is None:
            env_name = 'dask-' + sha1(
                '-'.join(self.packages).encode()).hexdigest()
            env_path = os.path.join(c.conda_envs, env_name)
            if os.path.exists(env_path + '.zip'):
                # zipfile exists, ready to upload
                self.env = env_path + '.zip'
            elif os.path.exists(env_path):
                # environment exists, can zip and upload
                c.zip_env(env_path)
                self.env = env_path + '.zip'
            else:
                # create env from scratch
                self.env = c.create_env(env_name=env_name,
                                        packages=self.packages)
        elif not self.env.endswith('.zip'):
            # given env directory, so zip it
            c.zip_env(self.env)
            self.env = self.env + '.zip'

        # TODO: memory should not be total available?
        command = '$PYTHON_BIN $CONDA_PREFIX/bin/dask-worker --nprocs=1 ' \
                  '--nthreads=%d --memory-limit=%d %s > ' \
                  '/tmp/worker-log.out 2> /tmp/worker-log.err' % (
                      cpus, memory * 1e6,
                      self.local_cluster.scheduler.address)

        app_id = self.knit.start(command, env=self.env,
                                 num_containers=n_workers,
                                 virtual_cores=cpus, memory=memory)
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
        # should not be remove or counted as a worker

        containers = self.knit.get_containers()
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

    def close(self):
        """Stop the scheduler and workers"""
        self.stop()
        self.local_cluster.close()
