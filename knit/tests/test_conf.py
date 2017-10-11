from __future__ import absolute_import, print_function

from knit.conf import get_host_port, infer_extra_params, DEFAULTS, get_config


def test_get_host_port():
    host, port = get_host_port('hdfs://foo.bar.com:8080')
    assert host == 'foo.bar.com'
    assert port == 8080

    host, port = get_host_port('foo.bar.com:8080')
    assert host == 'foo.bar.com'
    assert port == 8080

    host, port = get_host_port('foo.bar.com')
    assert host == 'foo.bar.com'
    assert port is None


def test_infer_extra_params():
    # == defaults ==
    extra = infer_extra_params({})
    assert extra == DEFAULTS

    # == replication_factor ==
    extra = infer_extra_params({'dfs.replication': 10})
    assert extra['replication_factor'] == 10

    # == resourcemanager and port ==
    # take port from webapp.address if provided
    config = {'yarn.resourcemanager.webapp.address': 'priority1.com:1111',
              'yarn.resourcemanager.hostname': 'priority2.com'}
    extra = infer_extra_params(config)
    assert extra['rm'] == 'priority1.com'
    assert extra['rm_port'] == 1111
    # Fallback to hostname and default port
    config = {'yarn.resourcemanager.hostname': 'priority2.com'}
    extra = infer_extra_params(config)
    assert extra['rm'] == 'priority2.com'
    assert extra['rm_port'] == DEFAULTS['rm_port']

    # == resourcemanager https port ==
    config = {'yarn.resourcemanager.webapp.https.address': 'address.com:1111'}
    extra = infer_extra_params(config)
    assert extra['rm_port_https'] == 1111


def test_get_config():
    kwargs = dict(rm="e", rm_port=27182, replication_factor=1)
    config = get_config(autodetect=False, **kwargs)
    for k, v in kwargs.items():
        assert config[k] == v
    # Just specified kwargs
    assert 'user' not in config

    config = get_config(**kwargs)
    for k, v in kwargs.items():
        assert config[k] == v
    # Not just specified kwargs
    assert 'user' in config
