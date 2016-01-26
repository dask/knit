import time
import pytest
import socket

from knit import Knit
from knit.exceptions import HDFSConfigException


@pytest.yield_fixture
def k():
    knitter = Knit()
    yield knitter


def test_port(k):
    with pytest.raises(HDFSConfigException):
        k = Knit(nn_port=90000, rm_port=90000)
    with pytest.raises(HDFSConfigException):
        k = Knit(nn_port=90000, rm_port=8088)
    with pytest.raises(HDFSConfigException):
        k = Knit(nn_port=9000, rm_port=5000)

    if socket.gethostname() == "inside_docker":
        # should pass without incident
        k = Knit(nn_port=8020, rm_port=8088)


def test_hotname(k):
    with pytest.raises(HDFSConfigException):
        k = Knit(nn="foobarbiz")

    if socket.gethostname() == "inside_docker":
        # should pass without incident
        k = Knit(nn="inside_docker")


def test_cmd(k):
    cmd = "/opt/anaconda/bin/python -c 'import socket; print(socket.gethostname()*2)'"
    appId = k.start(cmd)

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


## temporarily removing test until vCore handling is better resolved in the core
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
