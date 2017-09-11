import os
import pytest

from knit.yarn_api import YARNAPI
from knit.exceptions import YARNException


@pytest.yield_fixture
def y():
    yarnapi = YARNAPI("localhost", 8088)
    yield yarnapi


def test_info(y):
    # this test should run first to get YARN info
    #
    print(y.cluster_info())
    print(y.cluster_metrics())
    assert isinstance(y.apps, list)   # empty list on vanilla new cluster
    assert isinstance(y.apps_info(), list)
    assert y.nodes()
    assert y.scheduler()
    with pytest.raises(YARNException):
        y.app_attempts('noapp')
    with pytest.raises(YARNException):
        y.state('noapp')
    with pytest.raises(YARNException):
        y.status('noapp')


def test_logs(y):
    with pytest.raises(YARNException):
        y.logs("invalid_app_id_555")
