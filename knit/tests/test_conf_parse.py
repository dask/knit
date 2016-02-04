import os
import time
import pytest
import socket
from lxml import etree

from knit.utils import conf_find
from knit.exceptions import HDFSConfigException

cur_dir = os.path.dirname(__file__)
core_site = os.path.join(cur_dir, 'files', 'core-site.xml')

def check_docker():
    """check if inside docker container"""
    return os.path.exists('/.dockerenv')

inside_docker = check_docker


def test_conf_parse():
    assert 'hdfs://knit-host:9000' == conf_find(core_site, 'fs.defaultFS')
    assert '' == conf_find(core_site, 'FOO/BAR')
