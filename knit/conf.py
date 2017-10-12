"""
Configuration from system files and environment variables.

Warning: much of this code is copied from hdf3.conf
"""

from __future__ import absolute_import

import os
import re
try:
    from urllib.parse import urlsplit
except ImportError:
    from urlparse import urlsplit


__all__ = ('get_config', 'load_config', 'reset_config_cache')


def get_config(autodetect=True, pars=None, **kwargs):
    """Get configuration dictionary.

    Build up a configuration dictionary based on configuration files and
    overrides.

    Parameters
    ----------
    autodetect : bool, optional
        If True (default) configuration is read from files on disk. Otherwise
        only specified configuration parameters are used.
    pars : dict, optional
        Extra parameters to use for overriding
    **kwargs
        Extra parameters to use for overriding
    """
    config = load_config() if autodetect else {}
    if pars:
        config.update(**pars)
    config.update(**kwargs)
    return config


def current_user():
    try:
        import pwd
        return pwd.getpwuid(os.geteuid()).pw_name
    except ImportError:
        import getpass
        return getpass.getuser()


java_lib_dir = os.path.join(os.path.dirname(__file__), "java_libs")
DEFAULT_KNIT_HOME = os.environ.get('KNIT_HOME') or java_lib_dir

# standard defaults
DEFAULTS = {'rm': 'localhost',
            'rm_port': 8088,
            'rm_port_https': 8090,
            'replication_factor': 3,
            'user': current_user()}


def get_host_port(addr):
    """Infer a host & port from a given addr"""
    parsed = urlsplit(addr)
    if parsed.hostname:
        host = parsed.hostname
        port = parsed.port
    elif ':' in parsed.path:
        host, port = parsed.path.split(':', 1)
        port = int(port)
    else:
        host = parsed.path
        port = None
    return host, port


def infer_extra_params(config):
    """Given a config dict, infer the values of some extra fields"""
    user = current_user()

    replication_factor = config.get('dfs.replication',
                                    DEFAULTS['replication_factor'])

    # resourcemanager and port
    if 'yarn.resourcemanager.webapp.address' in config:
        rm_addr = config['yarn.resourcemanager.webapp.address']
        rm, rm_port = get_host_port(rm_addr)
    else:
        rm = config.get('yarn.resourcemanager.hostname', DEFAULTS['rm'])
        rm_port = DEFAULTS['rm_port']

    # resourcemanager https port
    if 'yarn.resourcemanager.webapp.https.address' in config:
        rm_https_addr = config['yarn.resourcemanager.webapp.https.address']
        rm_port_https = get_host_port(rm_https_addr)[1]
    else:
        rm_port_https = DEFAULTS['rm_port_https']

    return {'user': user,
            'replication_factor': replication_factor,
            'rm': rm,
            'rm_port': rm_port,
            'rm_port_https': rm_port_https}


def config_to_dict(fname):
    """ Read a *-site.xml style conf file, produces dictionary """
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


def error_if_path_missing(envvar):
    """Raise a ValueError if the path specified by an ENVVAR is missing"""
    path = os.environ[envvar]
    if not os.path.exists(path):
        raise ValueError("Environment variable `%s` points to "
                         "non-existant location" % envvar)


def find_config_files():
    """Look for config files in common places"""

    # Find the configuration directory
    if 'LIBHDFS3_CONF' in os.environ:
        error_if_path_missing('LIBHDFS3_CONF')
        confd, hdfs_site = os.path.split(os.environ['LIBHDFS3_CONF'])
    elif 'HADOOP_CONF_DIR' in os.environ:
        error_if_path_missing('HADOOP_CONF_DIR')
        confd = os.environ['HADOOP_CONF_DIR']
    elif 'HADOOP_INSTALL' in os.environ:
        error_if_path_missing('HADOOP_INSTALL')
        confd = os.environ['HADOOP_INSTALL'] + '/hadoop/conf'
    else:
        # list of potential typical system locations
        for loc in ['/etc/hadoop/conf']:
            if os.path.exists(loc):
                fns = os.listdir(loc)
                if 'core-site.xml' in fns:
                    confd = loc
                    break
        else:
            # fallback: local dir
            confd = os.getcwd()

    configs = {'yarn': 'yarn-site.xml',
               'core': 'core-site.xml'}
    paths = {}

    for name, f in configs.items():
        full_path = os.path.join(confd, f)
        if not os.path.exists(full_path):
            raise ValueError("Failed to load `%s` configuration file at "
                             "`%s`, file not found" % (name, full_path))
        paths[name] = full_path

    return paths


# The cached configuration
_cached_config = None


def load_config():
    """Load configuration from config files.

    Will error if configuration files aren't found.

    Returns
    -------
    config : dict
        The configuration dictionary.
    """
    global _cached_config

    if _cached_config is None:
        # Find the configuration files
        try:
            paths = find_config_files()
        except ValueError:
            _cached_config = DEFAULTS.copy()
            return DEFAULTS.copy()

        # Load and merge all config files
        config = {}
        for path in paths.values():
            config.update(config_to_dict(path))

        # Determine extra parameters
        config.update(infer_extra_params(config))

        _cached_config = config

    return _cached_config.copy()


def reset_config_cache():
    """Reset the configuration cache.

    The configuration will be recomputed on the next call to `load_config`."""
    global _cached_config
    _cached_config = None
