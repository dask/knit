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
    nn = "localhost"
    nn_port = 8020
    rm = "localhost"
    rm_port = 8088

    def __init__(self, nn=None, nn_port=None, rm=None,
                 rm_port=None, autodetect=True, validate=False,
                 packages=None, ip=None, env=None):

        ip = ip or socket.gethostbyname(socket.gethostname())

        self.env = env or None

        try:
            self.local_cluster = LocalCluster(n_workers=0, ip=ip)
        except (OSError, IOError):
            self.local_cluster = LocalCluster(n_workers=0, scheduler_port=0,
                                              ip=ip)

        if not self.env:
            if not packages:
                packages = []

            self.packages = list(
                unique(packages + global_packages, key=first_word))

        # if any hdfs/yarn settings are used don't use autodetect
        if autodetect or any([nn, nn_port, rm, rm_port]):
            self.knit = Knit(autodetect=True, validate=validate)
        else:
            nn = nn or self.nn
            nn_port = nn_port or self.nn_port
            rm = rm or self.rm
            rm_port = rm_port or self.rm_port
            self.knit = Knit(nn=nn, nn_port=nn_port, rm=rm, rm_port=rm_port,
                             validate=validate)

        atexit.register(self.stop)

    @property
    def scheduler_address(self):
        return self.local_cluster.scheduler_address

    def start(self, n_workers, cpus=1, memory=4000):
        if not self.env:
            env_name = 'dask-' + sha1(
                '-'.join(self.packages).encode()).hexdigest()
            if os.path.exists(
                    os.path.join(CondaCreator().conda_envs, env_name + '.zip')):
                self.env = os.path.join(CondaCreator().conda_envs,
                                        env_name + '.zip')
            else:
                self.env = self.knit.create_env(env_name=env_name,
                                                packages=self.packages)

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
        Update current worker ids

        Returns
        -------
        list: list of container ids
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
        try:
            self.knit.kill()
        except AttributeError:
            pass

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
        self.stop()
        self.local_cluster.close()
