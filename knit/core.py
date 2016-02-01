from __future__ import absolute_import, division, print_function

import os
import re
import requests
import logging
from subprocess import Popen, PIPE

from .utils import conf_to_dict
from .env import CondaCreator
from .compatibility import FileNotFoundError, urlparse
from .exceptions import HDFSConfigException


logger = logging.getLogger(__name__)

JAR_FILE = "knit-1.0-SNAPSHOT.jar"
JAVA_APP = "io.continuum.knit.Client"


class Knit(object):
    """
    Connection to HDFS/YARN

    Parameters
    ----------
    nn: str
        Namenode hostname/ip
    nn: str
        Namenode hostname/ip
    nn_port: str
        Namenode Port (default: 9000)
    rm: str
        Resource Manager hostname
    rm_port: str
        Resource Manager port (default: 8088)
    autodetect: bool
        Autodetect NN/RM IP/Ports

    Examples
    --------

    >>> k = Knit()
    >>> app_id = k.start('sleep 100', num_containers=5, memory=1024)
    """
    def __init__(self, nn="localhost", nn_port=9000,
                 rm="localhost", rm_port=8088, autodetect=False):

        self.nn = nn
        self.nn_port = str(nn_port)

        self.rm = rm
        self.rm_port = str(rm_port)

        if autodetect:
            self.nn,  self.nn_port = self._hdfs_conf(autodetect)
            self.rm,  self.rm_port = self._yarn_conf(autodetect)
        else:
            # validates IP/Port is correct
            self._hdfs_conf()
            self._yarn_conf()

        self.java_lib_dir = os.path.join(os.path.dirname(__file__), "java_libs")
        self.KNIT_HOME = os.environ.get('KNIT_HOME') or self.java_lib_dir

        # must set KNIT_HOME ENV for YARN App
        os.environ['KNIT_HOME'] = self.KNIT_HOME

    @property
    def JAR_FILE_PATH(self):
        return os.path.join(self.KNIT_HOME, JAR_FILE)

    def _yarn_conf(self, autodetect=False):
        """
        Load YARN config from default locations.
        Parameters
        ----------
        autodetect: bool

        Returns
        -------
            tuple (ip, port)

        """
        confd = os.environ.get('HADOOP_CONF_DIR', os.environ.get('HADOOP_INSTALL',
                               '') + '/hadoop/conf')
        afile = 'yarn-site.xml'
        conf = {}

        try:
            conf.update(conf_to_dict(os.sep.join([confd, afile])))
        except FileNotFoundError:
            pass
        if 'yarn.resourcemanager.webapp.address' in conf:
            url = conf['yarn.resourcemanager.webapp.address']
            u = urlparse(url)

            # handle host:port with no :// preabmle
            if u.path == url:
                conf['host'], conf['port'] = url.split(':')
            else:
                conf['host'] = u.hostname
                conf['port'] = u.port
        else:
            conf['host'] = "localhost"
            conf['port'] = "8088"

        if autodetect:
            return (conf['host'], conf['port'])

        if self.rm != conf['host']:
            msg = "Possible Resource Manager hostname mismatch.  Detected {}".format(conf['host'])
            raise HDFSConfigException(msg)
        if str(self.rm_port) != str(conf['port']):
            msg = "Possible Resource Manager port mismatch.  Detected {}".format(conf['port'])
            raise HDFSConfigException(msg)

        return conf

    def _hdfs_conf(self, autodetect=False):
        """"
        Parameters
        ----------
        autodetect: bool

        Returns
        -------
            tuple (ip, port)

        """

        confd = os.environ.get('HADOOP_CONF_DIR', os.environ.get('HADOOP_INSTALL',
                               '') + '/hadoop/conf')
        afile = 'core-site.xml'
        conf = {}
        try:
            conf.update(conf_to_dict(os.sep.join([confd, afile])))
        except FileNotFoundError:
            pass
        
        if 'fs.defaultFS' in conf:
            u = urlparse(conf['fs.defaultFS'])
            conf['host'] = u.hostname
            conf['port'] = u.port
        else:
            conf['host'] = "localhost"
            conf['port'] = "9000"

        if autodetect:
            return (conf['host'], conf['port'])

        if self.nn != conf['host']:
            msg = "Possible Namenode hostname mismatch.  Detected {}".format(conf['host'])
            raise HDFSConfigException(msg)

        if str(self.nn_port) != str(conf['port']):
            msg = "Possible Namenode port mismatch.  Detected {}".format(conf['port'])
            raise HDFSConfigException(msg)

        return conf

    def start(self, cmd, num_containers=1, virtual_cores=1, memory=128, env=""):
        """
        Method to start a yarn app with a distributed shell

        Parameters
        ----------
        cmd: str
            command to run in each yarn container
        num_containers: int
            Number of containers YARN should request (default: 1)
            * A container should be requested with the number of cores it can
              saturate, i.e.
            * the average number of threads it expects to have runnable at a
              time.
        virtual_cores: int
            Number of virtual cores per container (default: 1)
            * A node's capacity should be configured with virtual cores equal to
            * its number of physical cores.
        memory: int
            Memory per container (default: 128)
            * The unit for memory is megabytes.
        env: string
            Full Path to zipped Python environment

        Returns
        -------
        applicationId: str
            A yarn application ID string
        """

        args = ["hadoop", "jar", self.JAR_FILE_PATH, JAVA_APP, "--numContainers", str(num_containers),
                "--command", cmd, "--virtualCores", str(virtual_cores), "--memory", str(memory)]

        if env:
            args = args + ["--pythonEnv", str(env)]

        logger.debug("Running Command: {}".format(' '.join(args)))
        proc = Popen(args, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()

        logger.debug(out)
        logger.debug(err)

        # last string in out is applicationId
        # TODO Better JAVA Python communcation: appId, Resources, Yarn, etc.

        appId = out.split()[-1].decode("utf-8")
        appId = re.sub('id', '', appId)
        return appId

    def logs(self, app_id, shell=False):
        """
        Collect logs from each container

        Parameters
        ----------
        app_id: str
             A yarn application ID string
        shell: bool
             Shell out to yarn CLI (default False)

        Returns
        -------
        log: dictionary
            logs from each container
        """

        if shell:

            args = ["yarn", "logs", "-applicationId", app_id]

            proc = Popen(args, stdout=PIPE, stderr=PIPE)
            out, err = proc.communicate()

            logger.debug(out)
            logger.debug(err)
            return str(out)

        host_port = "{}:{}".format(self.rm, self.rm_port)
        url = "http://{}/ws/v1/cluster/apps/{}".format(host_port, app_id)
        logger.debug("Getting Resource Manager Info: {}".format(url))
        r = requests.get(url)
        data = r.json()
        logger.debug(data)

        try:
            amHostHttpAddress = data['app']['amHostHttpAddress']
        except KeyError:
            msg = "Local logs unavailable. State: {} finalStatus: {} Possibly check logs " \
                  "with `yarn logs -applicationId`".format(data['app']['state'],
                                                           data['app']['finalStatus'])
            raise Exception(msg)

        url = "http://{}/ws/v1/node/containers".format(amHostHttpAddress)
        r = requests.get(url)
        data = r.json()['containers']['container']
        logger.debug(data)

        # container_1452274436693_0001_01_000001
        def get_app_id_num(x):
            return "_".join(x.split("_")[1:3])

        app_id_num = get_app_id_num(app_id)
        containers = [d for d in data if get_app_id_num(d['id']) == app_id_num]

        logs = {}
        for c in containers:
            log = {}
            log['nodeId'] = c['nodeId']

            # grab stdout
            url = "{}/stdout/?start=0".format(c['containerLogsLink'])
            logger.debug("Gather stdout/stderr data from {}: {}".format(c['nodeId'], url))
            r = requests.get(url)
            log['stdout'] = r.text

            # grab stderr
            url = "{}/stderr/?start=0".format(c['containerLogsLink'])
            r = requests.get(url)
            log['stderr'] = r.text

            logs[c['id']] = log

        return logs

    def status(self, app_id):
        """ Get status of an application

        Parameters
        ----------
        app_id: str
             A yarn application ID string

        Returns
        -------
        log: dictionary
            status of application
        """
        host_port = "{}:{}".format(self.rm, self.rm_port)
        url = "http://{}/ws/v1/cluster/apps/{}".format(host_port, app_id)
        logger.debug("Getting Application Info: {}".format(url))
        r = requests.get(url)
        data = r.json()

        return data

    def kill(self, application_id):
        """
        Method to kill a yarn application

        Parameters
        ----------
        application_id: str
            YARN applicaiton id

        Returns
        -------
        bool:
            True if successful, False otherwise.
        """

        args = ["yarn", "application", "-kill", application_id]

        proc = Popen(args, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()

        logger.debug(out)
        logger.debug(err)

        return any("Killed application" in s for s in [str(out), str(err)])

    @staticmethod
    def create_env(env_name, packages=None, conda_root=None, remove=False):
        """
        Create zipped directory of a conda environment

        Parameters
        ----------
        env_name : str
        packages : list
        conda_root : str, optional
        remove : bool
            remove possible conda environment before creating

        Returns
        -------
        path: str
            path to zipped conda environment

        Examples
        --------

        >>> k = Knit()
        >>> pkg_path = k.create_env(env_name='dev',
        ...                         packages=['distributed', 'dask', 'pandas'])
        """

        c = CondaCreator(conda_root=conda_root)
        path = c.create_env(env_name, packages=packages, remove=remove)

        return path
