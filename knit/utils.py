from __future__ import print_function, division, absolute_import

import os
import logging

from lxml import etree

from .compatibility import urlparse, check_output

format = ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(format=format, level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)


def set_logging(level):
    logger = logging.getLogger('knit')
    logger.setLevel(level)


def parse_xml(f, search_string=''):
    conf = {}
    url = conf_find(f, search_string)
    if url:
        u = urlparse(url)

        # if we have a hostname and port, lets use that
        if u.hostname and u.port:
            conf['host'] = u.hostname
            conf['port'] = u.port

        # if not, assume bad things
        else:
            if ":" in url:
                conf['host'], conf['port'] = url.split(':')
                conf['port'] = int(conf['port'])
            else:
                conf['host'] = url
                conf['port'] = 80

    return conf


def conf_find(fp='', name=''):
    """
    Utility function to help parse hadoop configuration files.

    Parameters
    ----------
    fp : string
        file path
    name : string
        name to search

    Returns
    -------
    value : string

    Examples
    --------

    with the following xml
    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://knit-host:9000</value>
    </property>

    >>> conf_find('fs.defaultFS')

    """
    tree = etree.parse(fp)
    elem = tree.xpath("./property[descendant::text()='{0}']".format(name))
    try:
        hdfs_url = elem[0]
        return hdfs_url.find('value').text
    except IndexError:
        return ''


def shell_out(cmd=None, **kwargs):
    """
    Thin layer on check_output to return data as strings

    Parameters
    ----------
    cmd : list
        command to run
    kwargs:
        passed directly to check_output

    Returns
    -------
    result : str
        result of shell command
    """
    return check_output(cmd, **kwargs).decode('utf-8')


def get_log_content(s):
    if 'Cannot find this log' in s:
        return ''
    st = """<td class="content">"""
    ind0 = s.find(st) + len(st)
    ind1 = s[ind0:].find("</td>")
    return s[ind0:ind0+ind1]
