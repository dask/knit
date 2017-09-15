import logging

from knit.utils import set_logging


def test_set_logging():
    logger = logging.getLogger('knit')
    set_logging(logging.DEBUG)
    assert logger.level == logging.DEBUG
    set_logging(logging.INFO)
    assert logger.level == logging.INFO
