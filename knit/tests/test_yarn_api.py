import os
import time
import pytest
import socket

from knit.yarn_api import YARNAPI, check_app_id
from knit.exceptions import YARNException


def check_docker():
    """check if inside docker container"""
    return os.path.exists('/.dockerenv')

inside_docker = check_docker


@pytest.yield_fixture
def y():
    yarnapi = YARNAPI("localhost", 8088)
    yield yarnapi


def test_decorator(y):
    with pytest.raises(YARNException):
        def func(cls, app_id):
            return "TEST"

        wrapped = check_app_id(func)
        wrapped(y, "invalid_app_id_555")


def test_logs(y):
    with pytest.raises(YARNException):
        y.logs("invalid_app_id_555")

    apps = y.apps
    appId = apps[-1]

    y.logs(appId)
