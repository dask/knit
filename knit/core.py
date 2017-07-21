from __future__ import absolute_import, division, print_function

import os
import re
import requests
import logging
import socket
import atexit
import select
import signal
import platform
from subprocess import Popen, PIPE
import struct
import time

from .utils import parse_xml, shell_out
from .env import CondaCreator
from .compatibility import FileNotFoundError, urlparse
from .exceptions import HDFSConfigException, KnitException
from .yarn_api import YARNAPI

from py4j.protocol import Py4JError
from py4j.java_gateway import JavaGateway, GatewayClient

logger = logging.getLogger(__name__)

JAR_FILE = "knit-1.0-SNAPSHOT.jar"
JAVA_APP = "io.continuum.knit.Client"


def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]

defaults = dict(nn='localhost', nn_port='8020', rm='localhost', rm_port='8088')
confd = os.environ.get('HADOOP_CONF_DIR', os.environ.get(
    'HADOOP_INSTALL', '') + '/hadoop/conf')


class Knit(object):
    """
    Connection to HDFS/YARN. Launches a single "application" master with a
    number of worker containers.
    
    Parameter definition (nn, nn_port, rm, rm_port): those parameters given
    to __init__ take priority. If autodetect=True, Knit will attempt to fill
    out the others from system configuration files; fallback values are provided
    if this fails.

    Parameters
    ----------
    nn: str
        Namenode hostname/ip
    nn_port: int
        Namenode Port (default: 9000)
    rm: str
        Resource Manager hostname
    rm_port: int
        Resource Manager port (default: 8088)
    user: str ('root')
        The user name from point of view of HDFS. This is only used when
        checking for the existence of knit files on HDFS, since they are stored
        in the user's home directory.
    replication_factor: int (3)
        replication factor for files upload to HDFS (default: 3)
    autodetect: bool
        Autodetect NN/RM IP/Ports
    validate: bool
        Validate entered NN/RM IP/Ports match detected config file
    upload_always: bool(=False)
        If True, will upload conda environment zip always; otherwise will
        attempt to check for the file's existence in HDFS (using the hdfs3
        library, if present) and not upload if that matches the existing local
        file in size and is newer.

    Examples
    --------

    >>> k = Knit()
    >>> app_id = k.start('sleep 100', num_containers=5, memory=1024)
    """

    def __init__(self, nn=None, nn_port=None,  rm=None, rm_port=None,
                 user='root', replication_factor=3, autodetect=False,
                 validate=True, upload_always=False):

        self.user = user
        self.nn = nn
        self.nn_port = str(nn_port) if nn_port is not None else None

        self.rm = rm
        self.rm_port = str(rm_port) if nn_port is not None else None
        self.replication_factor = replication_factor

        self._hdfs_conf(autodetect, validate)
        self._yarn_conf(autodetect, validate)

        self.yarn_api = YARNAPI(self.rm, self.rm_port)

        self.java_lib_dir = os.path.join(os.path.dirname(__file__), "java_libs")
        self.KNIT_HOME = os.environ.get('KNIT_HOME') or self.java_lib_dir
        self.upload_always = upload_always

        # must set KNIT_HOME ENV for YARN App
        os.environ['KNIT_HOME'] = self.KNIT_HOME
        os.environ['REPLICATION_FACTOR'] = str(self.replication_factor)
        os.environ['REPLICATION_FACTOR'] = str(self.replication_factor)

        self.client = None
        self.master = None
        self.app_id = None

    def __str__(self):
        return "Knit<NN={0}:{1};RM={2}:{3}>".format(self.nn, self.nn_port,
                                                    self.rm, self.rm_port)

    __repr__ = __str__

    @property
    def JAR_FILE_PATH(self):
        return os.path.join(self.KNIT_HOME, JAR_FILE)

    def _yarn_conf(self, autodetect=False, validate=False):
        """
        Load YARN config from default locations.
        
        Parameters
        ----------
        autodetect: bool
            Find and use system config file
        validate: bool
            Assure that any provided parameters match system config file
        """
        yarn_site = os.path.join(confd, 'yarn-site.xml')
        try:
            if autodetect:
                with open(yarn_site, 'r') as f:
                    conf = parse_xml(f, 'yarn.resourcemanager.webapp.address')
                host, port = conf['host'], conf['port']
            else:
                host, port = defaults['rm'], defaults['rm_port']
        except (FileNotFoundError, KeyError):
            host, port = defaults['rm'], defaults['rm_port']

        if self.rm and self.rm != host and validate:
            msg = ("Possible Resource Manager hostname mismatch.  "
                   "Detected {0}").format(host)
            raise HDFSConfigException(msg)

        if self.rm_port and str(self.rm_port) != port and validate:
            msg = ("Possible Resource Manager port mismatch.  "
                   "Detected {0}").format(port)
            raise HDFSConfigException(msg)

        self.rm = self.rm or host
        self.rm_port = self.rm_port or port

    def _hdfs_conf(self, autodetect=False, validate=False):
        """
        Load HDFS config from default locations.
        
        Parameters
        ----------
        autodetect: bool
            Find and use system config file
        validate: bool
            Assure that any provided parameters match system config file
        """
        core_site = os.path.join(confd, 'core-site.xml')
        try:
            if autodetect:
                with open(core_site, 'r') as f:
                    conf = parse_xml(f, 'fs.defaultFS')
                host, port = conf['host'], conf['port']
            else:
                host, port = defaults['nn'], defaults['nn_port']
        except (FileNotFoundError, KeyError):
            host, port = defaults['nn'], defaults['nn_port']

        if self.nn and self.nn != host and validate:
            msg = ("Possible Resource Manager hostname mismatch.  "
                   "Detected {0}").format(host)
            raise HDFSConfigException(msg)

        if self.nn_port and str(self.nn_port) != port and validate:
            msg = ("Possible Resource Manager port mismatch.  "
                   "Detected {0}").format(port)
            raise HDFSConfigException(msg)

        self.nn = self.nn or host
        self.nn_port = self.nn_port or port

    def start(self, cmd, num_containers=1, virtual_cores=1, memory=128, env="",
              files=[], app_name="knit", queue="default"):
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
        app_name: String
            Application name shown in YARN (default: "knit")
        queue: String
            RM Queue to use while scheduling (default: "default")

        Returns
        -------
        applicationId: str
            A yarn application ID string
        """
        if not isinstance(memory, int):
            raise KnitException("Memory argument must be an integer")
        if files:
            if not isinstance(files, list):
                raise KnitException("File argument must be a list of strings")

        # From https://github.com/apache/spark/blob/d83c2f9f0b08d6d5d369d9fae04cdb15448e7f0d/python/pyspark/java_gateway.py
        # thank you spark

        # Start a socket that will be used by PythonGatewayServer to communicate its port to us
        callback_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        callback_socket.bind(('127.0.0.1', 0))
        callback_socket.listen(1)
        callback_host, callback_port = callback_socket.getsockname()

        args = ["hadoop", "jar", self.JAR_FILE_PATH, JAVA_APP, "--callbackHost",
                str(callback_host), "--callbackPort", str(callback_port)]
        
        on_windows = platform.system() == "Windows"

        # Launch the Java gateway.
        # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
        if not on_windows:
            # Don't send ctrl-c / SIGINT to the Java gateway:
            def preexec_func():
                signal.signal(signal.SIGINT, signal.SIG_IGN)
            proc = Popen(args, stdin=PIPE, preexec_fn=preexec_func)
        else:
            # preexec_fn not supported on Windows
            proc = Popen(args, stdin=PIPE)

        gateway_port = None
        # We use select() here in order to avoid blocking indefinitely if the
        # subprocess dies before connecting
        long_timeout = 60
        while gateway_port is None and proc.poll() is None and long_timeout > 0:
            timeout = 1  # (seconds)
            readable, _, _ = select.select([callback_socket], [], [], timeout)
            if callback_socket in readable:
                gateway_connection = callback_socket.accept()[0]
                # Determine which ephemeral port the server started on:
                gateway_port = read_int(gateway_connection.makefile(mode="rb"))
                gateway_connection.close()
                callback_socket.close()
            long_timeout -= 1

        if gateway_port is None:
            raise Exception("Java gateway process exited before sending the driver its port number")

        # In Windows, ensure the Java child processes do not linger after Python has exited.
        # In UNIX-based systems, the child process can kill itself on broken pipe (i.e. when
        # the parent process' stdin sends an EOF). In Windows, however, this is not possible
        # because java.lang.Process reads directly from the parent process' stdin, contending
        # with any opportunity to read an EOF from the parent. Note that this is only best
        # effort and will not take effect if the python process is violently terminated.
        if on_windows:
            # In Windows, the child process here is "spark-submit.cmd", not the JVM itself
            # (because the UNIX "exec" command is not available). This means we cannot simply
            # call proc.kill(), which kills only the "spark-submit.cmd" process but not the
            # JVMs. Instead, we use "taskkill" with the tree-kill option "/t" to terminate all
            # child processes in the tree (http://technet.microsoft.com/en-us/library/bb491009.aspx)
            def killChild():
                Popen(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(proc.pid)])
            atexit.register(killChild)

        gateway = JavaGateway(GatewayClient(port=gateway_port), auto_convert=True)
        self.client = gateway.entry_point
        upload = self.check_env_needs_upload(env)
        self.app_id = self.client.start(env, ','.join(files), app_name, queue, str(upload))

        long_timeout = 100
        master_rpcport = -1
        while master_rpcport == -1:
            master_rpcport = self.client.masterRPCPort()
            time.sleep(0.2)
            long_timeout -= 0.2
            if long_timeout < 0:
                break

        if master_rpcport == -1:
            raise Exception("YARN master container did not report back")
        master_rpchost = self.client.masterRPCHost()

        gateway = JavaGateway(GatewayClient(address=master_rpchost, port=master_rpcport), auto_convert=True)
        self.master = gateway.entry_point
        self.master.init(env, ','.join(files), cmd, num_containers, virtual_cores, memory)

        return self.app_id

    def add_containers(self, num_containers=1, virtual_cores=1, memory=128):
        """
        Method to add containers to an already running yarn app

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
        """
        self.master.addContainers(num_containers, virtual_cores, memory)

    def get_containers(self):
        """
        Method to return active containers

            Calls getContainers in Client.scala which returns comma delimited
            containerIds

        Returns
        -------
        container_list: List
            List of str

        """
        return self.client.getContainers().split(',')

    def get_container_statuses(self):
        """Get status info for each container via CLI
        
        Returns dict where the values are the raw text output.
        """
        return {c: self.yarn_api.container_status(c)
                for c in self.get_containers()}

    def remove_containers(self, container_id):
        """
        Method to remove containers from a running yarn app

            Calls removeContainers in ApplicationMaster.scala

        Be careful removing the ...0001 container.  This is where the
        applicationMaster is running

        Parameters
        ----------
        container_id: str

        Returns
        -------
        None

        """
        self.master.removeContainer(str(container_id))

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
        return c.create_env(env_name, packages=packages, remove=remove)

    def logs(self, shell=False):
        """
        Collect logs from RM (if running)
        With shell=True, collect logs from HDFS after job completion

        Parameters
        ----------
        shell: bool
             Shell out to yarn CLI (default False)

        Returns
        -------
        log: dictionary
            logs from each container (when possible)
        """
        return self.yarn_api.logs(self.app_id, shell=shell)

    def wait_for_completion(self, timeout=10):
        """
        Wait for completion of the yarn application
        
        Returns
        -------
        bool:
            True if successful, False otherwise
        """
        cur_status = self.runtime_status()
        while cur_status != 'SUCCEEDED' and timeout > 0:
            time.sleep(0.2)
            timeout -= 0.2
            cur_status = self.runtime_status()

        return timeout > 0

    def kill(self):
        """
        Method to kill a yarn application

        Returns
        -------
        bool:
            True if successful, False otherwise.
        """
        # TODO: set app_id back to None?
        try:
            return self.client.kill()
        except Py4JError:
            logger.debug("Error while attempting to kill", exc_info=1)

        # fallback
        return self.yarn_api.kill(self.app_id)

    def __del__(self):
        if self.app_id is not None:
            self.kill()

    def status(self):
        """ Get status of an application

        Returns
        -------
        log: dictionary
            status of application
        """
        return self.yarn_api.status(self.app_id)
    
    def runtime_status(self):
        """ Get runtime status of an application

        Returns
        -------
        str:
            status of application
        """
        if self.client is None:
            return "NONE"
        try:
            status = self.client.status()
            # rename finished to succeeded
            if status == "FINISHED":
                return "SUCCEEDED"
            return status
        except Py4JError:
            logger.debug("Error while fetching status", exc_info=1)

        # fallback
        status = self.status(self.app_id)
        final_status = status['app']['finalStatus']
        if final_status == 'UNDEFINED':
            return status['app']['state']
        return final_status

    def list_envs(self):
        """List knit conda environments already in HDFS
        
        Looks in location /user/{user}/.knitDeps/ for zip-files
        
        Returns: list of dict
            Details for each zip-file."""
        try:
            import hdfs3
            hdfs = hdfs3.HDFileSystem(self.nn, int(self.nn_port))
            files = hdfs.ls('/user/{}/.knitDeps/'.format(self.user), True)
            return [f for f in files if f['name'].endswith('.zip')]
        except (ImportError, IOError, OSError):
            raise ImportError('HDFS3 library required to list HDFS '
                              'environments')

    def check_env_needs_upload(self, env_path):
        """Upload is needed if zip file does not exist in HDFS or is older"""
        if self.upload_always:
            return True
        try:
            import hdfs3
            st = os.stat(env_path)
            size = st.st_size
            t = st.st_mtime
            hdfs = hdfs3.HDFileSystem(self.nn, int(self.nn_port))
            fn = ('/user/{}/.knitDeps/'.format(self.user) +
                  os.path.basename(env_path))
            info = hdfs.info(fn)
        except (ImportError, IOError, OSError):
            return True
        if info['size'] == size and t < info['last_mod']:
            return False
        else:
            return True
