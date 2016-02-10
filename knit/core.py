from __future__ import absolute_import, division, print_function

import os
import re
import requests
import logging
from subprocess import Popen, PIPE

from .utils import parse_xml
from .env import CondaCreator
from .compatibility import FileNotFoundError, urlparse
from .exceptions import HDFSConfigException, KnitException
from .yarn_api import YARNAPI

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

        self.yarn_api = YARNAPI(self.rm, self.rm_port)

        self.java_lib_dir = os.path.join(os.path.dirname(__file__), "java_libs")
        self.KNIT_HOME = os.environ.get('KNIT_HOME') or self.java_lib_dir

        # must set KNIT_HOME ENV for YARN App
        os.environ['KNIT_HOME'] = self.KNIT_HOME

    def __str__(self):
        return "Knit<NN={}:{};RM={}:{}>".format(self.nn, self.nn_port,
                                                     self.rm, self.rm_port)

    __repr__ = __str__

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
        conf = {}
        yarn_site = os.path.join(confd, 'yarn-site.xml')
        try:
            with open(yarn_site, 'r') as f:
                conf = parse_xml(f, 'yarn.resourcemanager.webapp.address')
        except FileNotFoundError:
            pass
        finally:
            if not conf:
                conf['host'] = "localhost"
                conf['port'] = "8088"

        if autodetect:
            return conf['host'], conf['port']

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
        conf = {}
        core_site = os.path.join(confd, 'core-site.xml')

        try:
            with open(core_site, 'r') as f:
                conf = parse_xml(core_site, 'fs.defaultFS')
        except FileNotFoundError:
            pass

        finally:
            if not conf:
                conf['host'] = "localhost"
                conf['port'] = "9000"

        if autodetect:
            return conf['host'], conf['port']

        if self.nn != conf['host']:
            msg = "Possible Namenode hostname mismatch.  Detected {}".format(conf['host'])
            raise HDFSConfigException(msg)

        if str(self.nn_port) != str(conf['port']):
            msg = "Possible Namenode port mismatch.  Detected {}".format(conf['port'])
            raise HDFSConfigException(msg)

        return conf

    def start(self, cmd, num_containers=1, virtual_cores=1, memory=128, env="", files=None):
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
        files: list
            list of files to be include in each container

        Returns
        -------
        applicationId: str
            A yarn application ID string
        """

        args = ["hadoop", "jar", self.JAR_FILE_PATH, JAVA_APP, "--numContainers", str(num_containers),
                "--command", cmd, "--virtualCores", str(virtual_cores), "--memory", str(memory)]

        if env:
            args = args + ["--pythonEnv", str(env)]
        if files:
            if not isinstance(files, list):
                raise KnitException("File argument must be a list of strings")
            f = ','.join(files)
            args = args + ["--files", str(f)]

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

    def logs(self, app_id, shell=False):
        """
        Collect logs from RM (if running)
        With shell=True, collect logs from HDFS after job completion

        Parameters
        ----------
        app_id: str
             A yarn application ID string
        shell: bool
             Shell out to yarn CLI (default False)

        Returns
        -------
        log: dictionary
            logs from each container (when possible)
        """
        return self.yarn_api.logs(app_id, shell=shell)

    def kill(self, app_id):
        """
        Method to kill a yarn application

        Parameters
        ----------
        app_id: str
            YARN application id

        Returns
        -------
        bool:
            True if successful, False otherwise.
        """
        return self.yarn_api.kill(app_id)

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
        return self.yarn_api.status(app_id)

    @property
    def apps(self):
        return self.yarn_api.apps
