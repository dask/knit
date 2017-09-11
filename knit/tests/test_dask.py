from __future__ import print_function

import os
import sys
import errno
import pytest
import shutil
import signal
import subprocess
import time
from functools import wraps

pytest.importorskip('dask')
import dask.distributed
from knit.dask_yarn import DaskYARNCluster
from knit import CondaCreator, Knit
from knit.conf import conf, guess_config
from dask.distributed import Client
from distributed.utils_test import loop


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def test_knit_config():
    cluster = DaskYARNCluster(nn="pi", nn_port=31415, rm="e", rm_port=27182,
                              autodetect=False, replication_factor=1)
    str(cluster) == 'Knit<NN=pi:31415;RM=e:27182>'
    cluster = DaskYARNCluster(nn="pi", nn_port=31415, rm="e", rm_port=27182,
                              autodetect=True, replication_factor=1)
    str(cluster) == 'Knit<NN=pi:31415;RM=e:27182>'

    try:
        conf['nn'] = 'nothost'
        d = DaskYARNCluster(autodetect=True, replication_factor=1)
        assert d.knit.conf['nn'] == 'nothost'

        d = DaskYARNCluster(autodetect=True, nn='oi', replication_factor=1)
        assert d.knit.conf['nn'] == 'oi'

    finally:
        guess_config()

python_version = '%d.%d' % (sys.version_info.major, sys.version_info.minor)
python_pkg = 'python=%s' % python_version
pkgs = [python_pkg, 'nomkl']


@pytest.yield_fixture
def clear():
    c = CondaCreator()
    try:
        yield
    finally:
        shutil.rmtree(c.conda_envs)
        try:
            k = Knit()
            import hdfs3
            hdfs = hdfs3.HDFileSystem()
            hdfs.rm(k.knit_home, recursive=True)
        except ImportError:
            pass


def test_yarn_cluster(loop, clear):
    with DaskYARNCluster(packages=pkgs, replication_factor=1) as cluster:

        @timeout(600)
        def start_dask():
            cluster.start(2, cpus=1, memory=128)
        try:    
            start_dask()
        except Exception as e:
            cluster.knit.kill()
            print("Fetching logs from failed test...")
            print(subprocess.check_output(['free', '-m']).decode())
            print(subprocess.check_output(['df', '-h']).decode())
            print(cluster.knit.yarn_api.cluster_metrics())
            time.sleep(5)
            print(cluster.knit.logs())

            sys.exit(1)

        @timeout(300)
        def do_work():
            with Client(cluster, loop=loop) as client:
                print(client)
                future = client.submit(lambda x: x + 1, 10)
                assert future.result() == 11
                print(client)
                print(future)

        time.sleep(2)
        try:
            do_work()
        except Exception as e:
            print(subprocess.check_output(['free', '-m']))
            cluster.knit.kill()
            print("Fetching logs from failed test...")
            time.sleep(5)
            print(subprocess.check_output(['free', '-m']))
            print(subprocess.check_output(['df', '-h']))
            print(cluster.knit.logs())


def test_yarn_cluster_add_stop(loop):
    with DaskYARNCluster(packages=pkgs, replication_factor=1) as _cluster:
        _cluster.start(1, cpus=1, memory=128)

        assert len(_cluster.workers) == 0

    cluster = DaskYARNCluster(env=_cluster.env, replication_factor=1)
    cluster.start(1, cpus=1, memory=128)

    client = Client(cluster)
    future = client.submit(lambda x: x + 1, 10)
    assert future.result() == 11

    info = client.scheduler_info()
    workers = info['workers']
    assert len(workers) == 1

    status = cluster.knit.status()
    num_containers = status['runningContainers']
    assert num_containers == 2  # 1 container for the worker and 1 for the RM

    cluster.add_workers(n_workers=1, cpus=1, memory=128)

    while num_containers != 3:
        status = cluster.knit.status()
        num_containers = status['runningContainers']

    # wait a tad to let workers connect to scheduler

    start = time.time()
    while len(client.scheduler_info()['workers']) < 2:
        time.sleep(0.1)
        assert time.time() < start + 10

    assert num_containers == 3
    info = client.scheduler_info()
    workers = info['workers']
    assert len(workers) == 2

    assert len(cluster.workers) == 2

    cluster.remove_worker(cluster.workers[1])
    while num_containers != 2:
        status = cluster.knit.status()
        num_containers = status['runningContainers']

    assert len(cluster.workers) == 1

    # STOP ALL WORKERS!
    cluster.stop()
    time.sleep(2)

    workers = client.scheduler_info()['workers']
    assert len(workers) == 0
