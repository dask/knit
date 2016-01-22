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


def test_cmd(k):
    cmd = "/opt/anaconda/bin/python -c 'import socket; print(socket.gethostname()*2)'"
    appId = k.start(cmd)

    status = k.get_application_status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = k.get_application_status(appId)
        time.sleep(2)

    hostname = socket.gethostname() * 2
    logs = k.get_application_logs(appId, shell=True)
    print(logs)

    assert hostname in logs
