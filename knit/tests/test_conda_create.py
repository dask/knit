import os
import time
import pytest
import socket

from knit import Knit
from knit.env import CondaCreator

def check_docker():
    """check if inside docker container"""
    return os.path.exists('/.dockerenv')

inside_docker = check_docker

@pytest.yield_fixture
def c():
    c = CondaCreator()
    yield c

def test_miniconda_install(c):
    assert c._install()

def test_create(c):
    with pytest.raises(TypeError):
        c.create_env('test_env', packages='numpy')
