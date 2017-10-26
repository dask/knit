from __future__ import print_function, division, absolute_import

import logging

from .compatibility import check_output

format = ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(format=format, level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)


def set_logging(level):
    logger = logging.getLogger('knit')
    logger.setLevel(level)

set_logging('INFO')


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
    out = s[ind0:ind0+ind1]
    return out.lstrip('\n          <pre>').rstrip('</pre>\n        ')


def triple_slash(s):
    if s.startswith('hdfs://') and not s.startswith('hdfs:///'):
        return 'hdfs:///' + s[7:]
    else:
        return s
