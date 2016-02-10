import os
import time
import pytest
import socket

from knit import Knit
from knit.exceptions import HDFSConfigException, KnitException


def check_docker():
    """check if inside docker container"""
    return os.path.exists('/.dockerenv')

inside_docker = check_docker


@pytest.yield_fixture
def k():
    knitter = Knit()
    yield knitter


def test_port(k):
    with pytest.raises(HDFSConfigException):
        Knit(nn_port=90000, rm_port=90000)
    with pytest.raises(HDFSConfigException):
        Knit(nn_port=90000, rm_port=8088)
    with pytest.raises(HDFSConfigException):
        Knit(nn_port=9000, rm_port=5000)

    if inside_docker:
        # should pass without incident
        Knit(nn_port=9000, rm_port=8088)


def test_hostname(k):
    with pytest.raises(HDFSConfigException):
        Knit(nn="foobarbiz")
    with pytest.raises(HDFSConfigException):
        Knit(rm="foobarbiz")

    if inside_docker:
        # should pass without incident
        Knit(nn="localhost")
        k = Knit(autodetect=True)
        str(k) == 'Knit<NN=localhost:9000;RM=localhost:8088>'


def test_cmd(k):
    cmd = "/opt/anaconda/bin/python -c 'import socket; print(socket.gethostname()*2)'"
    appId = k.start(cmd, memory='128')

    status = k.status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = k.status(appId)
        print(status['app']['finalStatus'])
        time.sleep(2)

    hostname = socket.gethostname() * 2
    logs = k.logs(appId, shell=True)
    print(logs)

    assert hostname in logs


def test_multiple_containers(k):
    cmd = "sleep 10"
    appId = k.start(cmd, num_containers=2)

    status = k.status(appId)
    while status['app']['state'] != 'RUNNING':
        status = k.status(appId)
        time.sleep(2)

    time.sleep(2)
    status = k.status(appId)
    # containers = num_containers + 1 for ApplicationMaster
    assert status['app']['runningContainers'] == 3

    # wait for job to finsih
    status = k.status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = k.status(appId)
        time.sleep(2)


def test_memory(k):
    cmd = "sleep 10"
    appId = k.start(cmd, num_containers=2, memory=300)
    status = k.status(appId)
    while status['app']['state'] != 'RUNNING':
        status = k.status(appId)
        time.sleep(2)

    time.sleep(2)
    status = k.status(appId)

    # not exactly sure on getting an exact number
    # 300*2+128(AM)
    assert status['app']['allocatedMB'] >= 728

    # wait for job to finsih
    status = k.status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = k.status(appId)
        time.sleep(2)


def test_cmd_w_conda_env(k):
    env_zip = k.create_env(env_name='dev', packages=['python=2.6'], remove=True)
    cmd = '$PYTHON_BIN -c "import sys; print(sys.version_info); import random; print(str(random.random()))"'
    appId = k.start(cmd, env=env_zip)

    status = k.status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = k.status(appId)
        print(status['app']['finalStatus'])
        time.sleep(2)

    logs = k.logs(appId, shell=True)
    assert "(2, 6, 9, 'final', 0)" in logs

cur_dir = os.path.dirname(__file__)
txt_file = os.path.join(cur_dir, 'files', 'test_upload_file.txt')
py_file = os.path.join(cur_dir, 'files', 'read_uploaded.py')


def test_file_uploading(k):
    cmd = 'python ./read_uploaded.py'
    with pytest.raises(KnitException):
        k.start(cmd, files='a,b,c')

    appId = k.start(cmd, files=[txt_file, py_file])

    status = k.status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = k.status(appId)
        print(status['app']['finalStatus'])
        time.sleep(2)

    logs = k.logs(appId, shell=True)

    assert "rambling on" in logs

# temporarily removing test until vCore handling is better resolved in the core
# def test_vcores(k):
#     cmd = "sleep 10"
#     appId = k.start(cmd, num_containers=1, memory=300, virtual_cores=2)
#
#     time.sleep(2)
#     status = k.status(appId)
#
#     while status['app']['state'] != 'RUNNING':
#         status = k.status(appId)
#         time.sleep(2)
#
#     time.sleep(2)
#     status = k.status(appId)
#
#     assert status['app']['allocatedVCores'] == 3
#
