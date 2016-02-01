from __future__ import print_function, division, absolute_import

import re
import logging
import subprocess

format = ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(format=format, level=logging.DEBUG)


def conf_to_dict(fname):
    name_match = re.compile("<name>(.*?)</name>")
    val_match = re.compile("<value>(.*?)</value>")
    conf = {}
    for line in open(fname):
        name = name_match.search(line)
        if name:
            key = name.groups()[0]
        val = val_match.search(line)
        if val:
            val = val.groups()[0]
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    pass
            if val == 'false':
                val = False
            if val == 'true':
                val = True
            conf[key] = val
    return conf


def shell_out(cmd=None):
    """
    Thin layer on check_output to return data as strings

    Parameters
    ----------
    cmd : list
        command to run

    Returns
    -------
    result : str
        result of shell command
    """
    return subprocess.check_output(cmd).decode('utf-8')
