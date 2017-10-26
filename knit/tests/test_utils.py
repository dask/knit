import logging

from knit.utils import set_logging, triple_slash


def test_set_logging():
    logger = logging.getLogger('knit')
    set_logging(logging.DEBUG)
    assert logger.level == logging.DEBUG
    set_logging(logging.INFO)
    assert logger.level == logging.INFO


def test_slashes():
    assert triple_slash('hdfs://hello/path') == 'hdfs:///hello/path'
    assert triple_slash('hdfs:///hello/path') == 'hdfs:///hello/path'
    assert triple_slash('hdfs:////hello/path') == 'hdfs:////hello/path'
