from __future__ import absolute_import, division, print_function

import os
import logging
import socket
import atexit
import select
import signal
import platform
from subprocess import Popen, PIPE, call
import struct
import time

from .conf import conf, DEFAULT_KNIT_HOME
from .env import CondaCreator
from .exceptions import KnitException
from .yarn_api import YARNAPI

from py4j.protocol import Py4JError
from py4j.java_gateway import JavaGateway, GatewayClient

logger = logging.getLogger(__name__)
on_windows = platform.system() == "Windows"


def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]


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
    hdfs_home: str
        Explicit location of a writable directory in HDFS to store files. 
        Defaults to the user 'home': hdfs://user/<username>/
    replication_factor: int (3)
        replication factor for files upload to HDFS (default: 3)
    autodetect: bool
        Autodetect configuration
    upload_always: bool(=False)
        If True, will upload conda environment zip always; otherwise will
        attempt to check for the file's existence in HDFS (using the hdfs3
        library, if present) and not upload if that matches the existing local
        file in size and is newer.
    knit_home: str
        Location of knit's jar

    Examples
    --------

    >>> k = Knit()
    >>> app_id = k.start('sleep 100', num_containers=5, memory=1024)
    """

    JAR_FILE = "knit-1.0-SNAPSHOT.jar"
    JAVA_APP = "io.continuum.knit.Client"

    def __init__(self, autodetect=True, upload_always=False, hdfs_home=None,
                 knit_home=DEFAULT_KNIT_HOME, pars=None, **kwargs):

        self.conf = conf.copy() if autodetect else {}
        if pars:
            self.conf.update(pars)
        self.conf.update(kwargs)

        if conf.get('yarn.http.policy', '').upper() == "HTTPS_ONLY":
            self.yarn_api = YARNAPI(conf['rm'], conf['rm_port_https'],
                                    scheme='https')
        else:
            self.yarn_api = YARNAPI(conf['rm'], conf['rm_port'])

        self.KNIT_HOME = knit_home
        self.upload_always = upload_always
        self.hdfs_home = hdfs_home or conf.get('dfs.user.home.base.dir',
                                               '/user/' + conf['user'])

        # must set KNIT_HOME ENV for YARN App
        os.environ['KNIT_HOME'] = self.KNIT_HOME
        os.environ['REPLICATION_FACTOR'] = str(self.conf['replication_factor'])
        os.environ['HDFS_KNIT_DIR'] = self.hdfs_home

        self.client = None
        self.master = None
        self.app_id = None
        self.proc = None
        self._hdfs = None

    def __str__(self):
        return "Knit<NN={0}:{1};RM={2}:{3}>".format(
            self.conf['nn'], self.conf['nn_port'], self.conf['rm'],
            self.conf['rm_port'])

    __repr__ = __str__

    @property
    def JAR_FILE_PATH(self):
        return os.path.join(self.KNIT_HOME, self.JAR_FILE)

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
        if self.app_id:
            raise ValueError('Already started')
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

        if not os.path.exists(self.JAR_FILE_PATH):
            raise KnitException('JAR file %s does not exists - please build'
                                ' with maven' % self.JAR_FILE_PATH)
        args = ["hadoop", "jar", self.JAR_FILE_PATH, self.JAVA_APP,
                "--callbackHost", str(callback_host), "--callbackPort",
                str(callback_port)]

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
        self.proc = proc
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
            raise Exception("Java gateway process exited before sending the"
                            " driver its port number")

        gateway = JavaGateway(GatewayClient(port=gateway_port), auto_convert=True)
        self.client = gateway.entry_point
        self.client_gateway = gateway
        upload = self.check_env_needs_upload(env)
        self.app_id = self.client.start(env, ','.join(files), app_name, queue,
                                        str(upload))

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

        gateway = JavaGateway(GatewayClient(
            address=master_rpchost, port=master_rpcport), auto_convert=True)
        self.master = gateway.entry_point
        self.master.init(env, ','.join(files), cmd, num_containers,
                         virtual_cores, memory)

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
    def create_env(env_name, packages=None, remove=False,
                   channels=None, conda_pars=None):
        """
        Create zipped directory of a conda environment

        Parameters
        ----------
        env_name : str
        packages : list
        conda_root: str
            Location of conda installation. If None, will download miniconda and
            produce an isolated environment.
        remove : bool
            remove possible conda environment before creating
        channels : list of str
            conda channels to use (defaults to your conda setup)
        conda_pars: dict
            Further pars to pass to CondaCreator

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

        channels = channels or []
        c = CondaCreator(channels=channels, **(conda_pars or {}))
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

    def print_logs(self, shell=False):
        """print out a more console-friendly version of logs()"""
        for l, v in self.logs(shell).items():
            print('Container ', l, ', id ', v.get('id', 'None'), '\n')
            for part in ['stdout', 'stderr']:
                print('##', part, '##')
                print(v[part])

    def wait_for_completion(self, timeout=10):
        """
        Wait for completion of the yarn application
        
        Returns
        -------
        bool:
            True if successful, False otherwise
        """
        cur_status = self.runtime_status()
        while cur_status not in ['FAILED', 'KILLED', 'FINISHED']:
            time.sleep(0.2)
            timeout -= 0.2
            cur_status = self.runtime_status()
            if timeout < 0:
                break

        return timeout > 0

    def kill(self):
        """
        Method to kill a yarn application

        Returns
        -------
        bool:
            True if successful, False otherwise.
        """
        if self.client is None:
            # never started, can't stop - should be warning or exception?
            return False
        try:
            self.client.kill()
        except Py4JError:
            logger.debug("Error while attempting to kill", exc_info=1)
            # fallback
            self.yarn_api.kill(self.app_id)
        if self.proc is not None:
            self.client_gateway.shutdown()
            if on_windows:
                subprocess.call(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(self.proc.pid)])
            self.proc.terminate()
            self.proc.communicate()
            self.proc = None
        out = self.runtime_status() == 'KILLED'
        return out

    def __del__(self):
        if self.app_id is not None:
            try:
                self.kill()
            except:
                pass

    def status(self):
        """ Get status of an application

        Returns
        -------
        log: dictionary
            status of application
        """
        return self.yarn_api.apps_info(self.app_id)
    
    def runtime_status(self):
        """ Get runtime status of an application

        Returns
        -------
        str:
            status of application
        """
        try:
            return self.yarn_api.state(self.app_id)
        except:
            return "NONE"

    @property
    def hdfs(self):
        """ An instance of HDFileSystem
        
        Useful for checking on the contents of the staging directory.
        Will be automatically generated using this instance's configuration,
        but can instead directly set ``self._hdfs`` if necessary.
        """
        if self._hdfs is None:
            try:
                import hdfs3
                par2 = self.conf.copy()
                par2['host'] = par2.pop('nn')
                par2['port'] = par2.pop('nn_port')
                del par2['replication_factor']
                del par2['rm_port']
                del par2['rm_port_https']
                self._hdfs = hdfs3.HDFileSystem(pars=par2)
            except:
                self._hdfs = False
        return self._hdfs

    def list_envs(self):
        """List knit conda environments already in HDFS
        
        Looks staging directory for zip-files
        
        Returns: list of dict
            Details for each zip-file."""
        if self.hdfs:
            files = self.hdfs.ls(self.hdfs_home + '/.knitDeps/', True)
            return [f for f in files if f['name'].endswith('.zip')]
        else:
            raise ImportError('HDFS3 library required to list HDFS '
                              'environments')

    def check_env_needs_upload(self, env_path):
        """Upload is needed if zip file does not exist in HDFS or is older"""
        if not env_path:
            return False
        if self.upload_always:
            return True
        fn = (self.hdfs_home + '/.knitDeps/' + os.path.basename(env_path))
        if self.hdfs and self.hdfs.exists(fn):
            st = os.stat(env_path)
            size = st.st_size
            t = st.st_mtime
            info = self.hdfs.info(fn)
            if info['size'] == size and t < info['last_mod']:
                return False
            else:
                return True
        else:
            return True
