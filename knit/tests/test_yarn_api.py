import os
import time
import pytest
import socket

from knit import Knit
from knit.yarn_api import check_app_id
from knit.exceptions import YARNException


def check_docker():
    """check if inside docker container"""
    return os.path.exists('/.dockerenv')

inside_docker = check_docker


@pytest.yield_fixture
def k():
    knitter = Knit()
    yield knitter


def test_decorator(k):
    with pytest.raises(YARNException):
        def func(cls, app_id):
            return "TEST"

        wrapped = check_app_id(func)
        wrapped(k, "invalid_app_id_555")


def test_logs(k):
    with pytest.raises(YARNException):
        k.logs("invalid_app_id_555")

    apps = k.apps
    appId = apps[-1]
    with pytest.raises(YARNException):
        k.logs(appId)

    assert k.logs(appId, shell=True)


def test_kill_status(k):
    cmd = "sleep 10"
    app_id = k.start(cmd, num_containers=1)

    status = k.status(app_id)
    while status['app']['state'] != 'RUNNING':
        status = k.status(app_id)
        time.sleep(1)
    assert k.kill(app_id)

    status = k.status(app_id)
    assert status['app']['state'] == 'KILLED'

