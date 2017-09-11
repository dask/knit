"""
Configuration from system files and environment variables.

Warning: much of this code is copied from hdf3.conf
"""

from __future__ import absolute_import

import os
import re
import warnings


def current_user():
    try:
        import pwd
        return pwd.getpwuid(os.geteuid()).pw_name
    except ImportError:
        import getpass
        return getpass.getuser()

DEFAULT_USER = current_user()
java_lib_dir = os.path.join(os.path.dirname(__file__), "java_libs")
DEFAULT_KNIT_HOME = os.environ.get('KNIT_HOME') or java_lib_dir

# standard defaults
conf_defaults = {'nn': 'localhost', 'nn_port': 8020, 'rm': 'localhost',
                 'rm_port': 8088, 'rm_port_https': 8090,
                 'replication_factor': 3}
conf = conf_defaults.copy()


def hdfs_conf(confd, more_files=None):
    """ Load HDFS config from default locations. 

    Parameters
    ----------
    confd: str
        Directory location to search in
    more_files: list of str or None
        If given, additional filenames to query
    """
    files = ['core-site.xml', 'hdfs-site.xml']
    if more_files:
        files.extend(more_files)
    c = {'user': DEFAULT_USER}
    for afile in files:
        try:
            c.update(conf_to_dict(os.sep.join([confd, afile])))
        except (IOError, OSError):
            pass
    if not c:
        # no config files here
        return
    if 'fs.defaultFS' in c and c['fs.defaultFS'].startswith('hdfs'):
        # default FS in 'core'
        text = c['fs.defaultFS'].strip('hdfs://')
        host = text.split(':', 1)[0]
        port = text.split(':', 1)[1:]
        if host:
            c['nn'] = host
        if port:
            c['nn_port'] = int(port[0])
    if 'dfs.namenode.rpc-address' in c:
        # name node address
        text = c['dfs.namenode.rpc-address']
        host = text.split(':', 1)[0]
        port = text.split(':', 1)[1:]
        if host:
            c['nn'] = host
        if port:
            c['nn_port'] = int(port[0])
    if c.get("dfs.nameservices", None):
        # HA override
        c['nn'] = c["dfs.nameservices"].split(',', 1)[0]
        c['nn_port'] = None
    if 'nn' not in c:
        # no host found at all, use defaults
        c['nn'] = conf_defaults['nn']
        c['nn_port'] = conf_defaults['nn_port']
    if 'dfs.replication' in c:
        c['replication_factor'] = c['dfs.replication']
    else:
        c['replication_factor'] = conf_defaults['replication_factor']
    if 'yarn.resourcemanager.webapp.address' in c:
        c['rm'], c['rm_port'] = c[
            'yarn.resourcemanager.webapp.address'].split(':')
    elif 'yarn.resourcemanager.hostname' in c:
        c['rm'] = c['yarn.resourcemanager.hostname']
        c['rm_port'] = conf_defaults['rm_port']
    else:
        c['rm'] = conf_defaults['rm']
        c['rm_port'] = conf_defaults['rm_port']
    if 'yarn.resourcemanager.webapp.https.address' in c:
        c['rm'], c['rm_port_https'] = c[
            'yarn.resourcemanager.webapp.https.address'].split(':')
    else:
        c['rm_port_https'] = conf_defaults['rm_port_https']
    conf.clear()
    conf.update(c)


def reset_to_defaults():
    conf.clear()
    conf.update(conf_defaults)


def conf_to_dict(fname):
    """ Read a hdfs-site.xml style conf file, produces dictionary """
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
            conf[key] = val
    return conf


def guess_config():
    """ Look for config files in common places """
    d = None
    if 'LIBHDFS3_CONF' in os.environ:
        fdir, fn = os.path.split(os.environ['LIBHDFS3_CONF'])
        hdfs_conf(fdir, more_files=[fn, 'yarn-site.xml'])
        if not os.path.exists(os.environ['LIBHDFS3_CONF']):
            del os.environ['LIBHDFS3_CONF']
        return
    elif 'HADOOP_CONF_DIR' in os.environ:
        d = os.environ['HADOOP_CONF_DIR']
    elif 'HADOOP_INSTALL' in os.environ:
        d = os.environ['HADOOP_INSTALL'] + '/hadoop/conf'
    if d is None:
        # list of potential typical system locations
        for loc in ['/etc/hadoop/conf']:
            if os.path.exists(loc):
                fns = os.listdir(loc)
                if 'hdfs-site.xml' in fns:
                    d = loc
                    break
    if d is None:
        # fallback: local dir
        d = os.getcwd()
    hdfs_conf(d, more_files=['yarn-site.xml'])
    if os.path.exists(os.path.join(d, 'hdfs-site.xml')):
        os.environ['LIBHDFS3_CONF'] = os.path.join(d, 'hdfs-site.xml')


guess_config()
