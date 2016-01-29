import os
import time
import pytest
import socket
import uuid

from knit.exceptions import CondaException
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
        env_name = str(uuid.uuid4())
        c.create_env(env_name, packages='numpy')

    env_name = 'test_env'
    env_path = os.path.join(c.conda_root, 'envs', env_name)
    assert env_path == c._create_env(env_name, packages=['python=3', 'numpy'], remove=True)

    with pytest.raises(CondaException):
        c._create_env(env_name, packages=['numpy'])


def test_full_create(c):
    env_name = 'test_env'
    env_zip = os.path.join(c.conda_root, 'envs', env_name+'.zip')
    assert env_zip == c.create_env(env_name, packages=['python=3', 'numpy'], remove=True)
    assert os.path.getsize(env_zip) > 500000 # ensures zipfile has non-0 size


