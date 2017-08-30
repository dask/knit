import os
import pytest

from knit.yarn_api import YARNAPI
from knit.exceptions import YARNException


def check_docker():
    """check if inside docker container"""
    return os.path.exists('/.dockerenv')

inside_docker = check_docker


@pytest.yield_fixture
def y():
    yarnapi = YARNAPI("localhost", 8088)
    yield yarnapi


def test_logs(y):
    with pytest.raises(YARNException):
        y.logs("invalid_app_id_555")
