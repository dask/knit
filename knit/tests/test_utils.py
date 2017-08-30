import os
import logging

from knit.utils import conf_find, set_logging, parse_xml

cur_dir = os.path.dirname(__file__)
core_site = os.path.join(cur_dir, 'files', 'core-site.xml')
yarn_site = os.path.join(cur_dir, 'files', 'yarn-site.xml')


def check_docker():
    """check if inside docker container"""
    return os.path.exists('/.dockerenv')

inside_docker = check_docker


def test_conf_parse():
    assert '' == conf_find(core_site, 'FOO/BAR')

    assert 'hdfs://knit-host:9000' == conf_find(core_site, 'fs.defaultFS')
    conf = parse_xml(core_site, 'fs.defaultFS')
    assert conf == {'port': 9000, 'host': 'knit-host'}

    assert 'knit-host:8088' == conf_find(yarn_site, 'yarn.resourcemanager.webapp.address')
    conf = parse_xml(yarn_site, 'yarn.resourcemanager.webapp.address')
    assert conf == {'port': 8088, 'host': 'knit-host'}


def test_set_logging():
    logger = logging.getLogger('knit')
    set_logging(logging.DEBUG)
    assert logger.level == logging.DEBUG
    set_logging(logging.INFO)
    assert logger.level == logging.INFO
