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


def test_hosntname(k):
    with pytest.raises(HDFSConfigException):
        k = Knit(nn="foobarbiz")

    if socket.gethostname() == "inside_docker":
        # should pass without incident
        k = Knit(nn="inside_docker")

def test_multiple_containers(k):
    cmd = "sleep 10"
    appId = k.start(cmd, num_containers=2)

    status = k.status(appId)
    
    while status['app']['state'] != 'RUNNING':
        status = k.status(appId)
        time.sleep(2)

    # containers = num_containers + 1 for ApplicationMaster
    assert status['app']['runningContainers'] == 3

    appId = k.start(cmd, num_containers=2, memory=300)
    status = k.status(appId)
    while status['app']['state'] != 'RUNNING':
        status = k.status(appId)
        time.sleep(2)

    # not exactly sure how we get this number
    # 300*2+128(AM)+256 (JAVA PROCESS FOR CLIENT)
    assert status['app']['allocatedMB'] == 1024


def test_cmd(k):
    cmd = "/opt/anaconda/bin/python -c 'import socket; print(socket.gethostname()*2)'"
    appId = k.start(cmd)

    status = k.status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = k.status(appId)
        time.sleep(2)

    hostname = socket.gethostname() * 2
    logs = k.logs(appId, shell=True)
    print(logs)

    assert hostname in logs
