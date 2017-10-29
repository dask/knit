from __future__ import absolute_import, division, print_function

import atexit
import os
import logging
import socket
import select
import signal
import platform
import requests
import socket
from subprocess import Popen, PIPE, call
import struct
import time
import weakref

from .conf import get_config, DEFAULT_KNIT_HOME
from .env import CondaCreator
from .exceptions import KnitException, YARNException
from .yarn_api import YARNAPI
from .utils import triple_slash

from py4j.protocol import Py4JError
from py4j.java_gateway import JavaGateway, GatewayClient
from py4j.java_collections import MapConverter, ListConverter

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
    lang: str
        Environment variable language setting, required for ``click`` to
        successfully read from the shell. (default: 'C.UTF-8')
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
    hdfs: HDFileSystem instance or None
        Used for checking files in HDFS.

    Note: for now, only one Knit instance can live in a single process because
    of how py4j interfaces with the JVM.

    Examples
    --------

    >>> k = Knit()
    >>> app_id = k.start('sleep 100', num_containers=5, memory=1024)
    """

    JAR_FILE = "knit-1.0-SNAPSHOT.jar"
    JAVA_APP = "io.continuum.knit.Client"
    _instances = weakref.WeakSet()

    def __init__(self, autodetect=True, upload_always=False, hdfs_home=None,
                 knit_home=DEFAULT_KNIT_HOME, hdfs=None, pars=None,
                 **kwargs):

        self.conf = get_config(autodetect=autodetect, pars=pars, **kwargs)
        gateway_path = self.conf.get('gateway_path', '')
        kerb = self.conf.get(
            'hadoop.http.authentication.type', '') == 'kerberos'
        if not kerb and self.conf.get('hadoop.http.authentication.simple.'
                                      'anonymous.allowed', '') == 'false':
            if 'password' not in self.conf:
                raise KnitException('Simple auth required: please supply'
                                    '`password=`.')
            pw = self.conf['password']
        else:
            pw = None

        if self.conf.get('yarn.http.policy', '').upper() == "HTTPS_ONLY":
            self.yarn_api = YARNAPI(self.conf['rm'], self.conf['rm_port_https'],
                                    scheme='https', gateway_path=gateway_path,
                                    kerberos=kerb, username=self.conf['user'],
                                    password=pw)
        else:
            self.yarn_api = YARNAPI(self.conf['rm'], self.conf['rm_port'],
                                    gateway_path=gateway_path,
                                    kerberos=kerb, username=self.conf['user'],
                                    password=pw)

        self.KNIT_HOME = knit_home
        self.upload_always = upload_always
        self.lang = self.conf.get('lang', 'C.UTF-8')
        self.hdfs_home = hdfs_home or self.conf.get(
            'dfs.user.home.base.dir', '/user/' + self.conf['user'])
        self.client_gateway = None

        # must set KNIT_HOME ENV for YARN App
        os.environ['KNIT_HOME'] = self.KNIT_HOME
        os.environ['REPLICATION_FACTOR'] = str(self.conf['replication_factor'])
        os.environ['HDFS_KNIT_DIR'] = self.hdfs_home

        self.client = None
        self.master = None
        self.app_id = None
        self.proc = None
        self.hdfs = hdfs
        self._instances.add(self)

    def __repr__(self):
        return "Knit<RM={0}:{1}>".format(self.conf['rm'], self.conf['rm_port'])

    @property
    def JAR_FILE_PATH(self):
        return os.path.join(self.KNIT_HOME, self.JAR_FILE)

    def _pre_flight_checks(self, num_containers, virtual_cores, memory,
                           files, queue):
        """Some checks to see if app is possible to schedule

        This depends on YARN's allocations reporting, which do not necessarily
        reflect the true amount of resources on the cluster. Other failure
        modes, such as full disc, are not likely to be caught here.
        """
        try:
            # check response from RM
            met = self.yarn_api.cluster_metrics()
        except YARNException:
            raise
        except requests.RequestException as e:
            if isinstance(e, requests.Timeout):
                m = 'Connection timeout'
            else:
                m = 'Connection error'
            raise YARNException(m + ' when talking to the '
                                'YARN REST server at {}. This can mean that '
                                'the server/port values are wrong, that you '
                                'are using the wrong protocol (http/https) or '
                                'that you need to route through a proxy.'
                                ''.format(self.yarn_api.url))
        if met['activeNodes'] < 1:
            raise KnitException('No name-nodes active')
        # What if we simply don't have the full yarn-site.xml available?
        mmin = int(self.conf.get('yarn.scheduler.minimum-allocation-mb', 1024))
        # 300MB default allocation for AM in client.scala
        mem = (max(300, mmin) + num_containers * max(memory, mmin))
        if met['availableMB'] < mem:
            raise KnitException('Memory estimate for app (%iMB) exceeds cluster'
                                ' capacity (%iMB)' % (mem, met['availableMB']))
        c = 1 + num_containers * virtual_cores
        if met['availableVirtualCores'] < c:
            raise KnitException('vCPU request for app (%i) exceeds cluster capa'
                                'city (%i)' % (c, met['availableVirtualCores']))
        nodes = self.yarn_api.nodes()
        if all((max(mmin, memory) > n['availMemoryMB']) and
               (virtual_cores > n['availableVirtualCores'])
               for n in nodes):
            # cannot test without multiple nodemanagers
            raise KnitException('No NodeManager can fit any single container')
        if self.hdfs:
            df = self.hdfs.df()
            cap = (df['capacity'] - df['used']) // 2**20
            fs = [self.JAR_FILE_PATH] + [f for f in files
                                         if not f.startswith('hdfs://')]
            need = sum(os.stat(f).st_size for f in fs) // 2**20
            # NB: if replication > 1 this might not be enough
            if cap < need:
                raise KnitException('HDFS space requirement (%iMB) exceeds'
                                    'capacity (%iMB)' % (need, cap))

    def start(self, cmd, num_containers=1, virtual_cores=1, memory=128,
              files=None, envvars=None, app_name="knit", queue="default",
              checks=True):
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
        files: list
            list of files to be include in each container. If starting with
            `hdfs://`, assume these already exist in HDFS and don't need
            uploading. Otherwise, if hdfs3 is installed, existence of the
            file on HDFS will be checked to see if upload is needed.
            Files ending with `.zip` will be decompressed in the
            container before launch as a directory with the same name as the
            file: if myarc.zip contains files inside a directory stuff/, to
            the container they will appear at ./myarc.zip/stuff/* .
        envvars: dict
            Environment variables to pass to AM *and* workers. Both keys
            and values must be strings only.
        app_name: String
            Application name shown in YARN (default: "knit")
        queue: String
            RM Queue to use while scheduling (default: "default")
        checks: bool=True
            Whether to run pre-flight checks before submitting app to YARN

        Returns
        -------
        applicationId: str
            A yarn application ID string
        """
        files = files or []
        envvars = envvars or {'KNIT_LANG': self.lang}
        for k, v in envvars.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise ValueError('Environment must contain only strings (%s)'
                                 % ((k, v),))
        if self.app_id:
            raise ValueError('Already started')
        if not isinstance(memory, int):
            raise KnitException("Memory argument must be an integer")
        if files:
            if not isinstance(files, list):
                raise KnitException("File argument must be a list of strings")

        if checks:
            self._pre_flight_checks(num_containers, virtual_cores, memory,
                                    files, queue)
        # From https://github.com/apache/spark/blob/d83c2f9f0b08d6d5d369d9fae04cdb15448e7f0d/python/pyspark/java_gateway.py
        # thank you spark

        ## Socket for PythonGatewayServer to communicate its port to us
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

        ## Launch the Java gateway.
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
            raise Exception("The JVM Knit client failed to launch successfully."
                            " Check that java is installed and the Knit JAR"
                            " file exists.")

        gateway = JavaGateway(GatewayClient(port=gateway_port),
                              auto_convert=True)
        self.client = gateway.entry_point
        self.client_gateway = gateway
        logger.debug("Files submitted: %s" % files)
        upfiles = [f for f in files if (not f.startswith('hdfs://')
                   and self.check_needs_upload(f))]
        logger.debug("Files to upload: %s" % upfiles)
        jfiles = ListConverter().convert(upfiles, gateway._gateway_client)
        jenv = MapConverter().convert(envvars, gateway._gateway_client)

        self.app_id = self.client.start(jfiles, jenv, app_name, queue)

        ## Wait for AM to appear
        long_timeout = 100
        master_rpcport = -1
        while master_rpcport == -1:
            master_rpcport = self.client.masterRPCPort()
            time.sleep(0.2)
            long_timeout -= 0.2
            if long_timeout < 0:
                break

        if master_rpcport in [-1, 'N/A']:
            raise Exception(
"""The application master JVM process failed to report back. This can mean:
 - that the YARN cluster cannot scheduler adequate resources - check
   k.yarn_api.cluster_metrics() and other diagnostic methods;
 - that the ApplicationMaster crashed - check the application logs, k.logs();
 - that the cluster is otherwise unhealthy - check the RM and NN logs 
   (use k.yarn_api.system_logs() to find these on a one-node system
""")
        master_rpchost = self.client.masterRPCHost()

        gateway = JavaGateway(GatewayClient(
            address=master_rpchost, port=master_rpcport), auto_convert=True)
        self.master = gateway.entry_point
        rfiles = [triple_slash(f) if f.startswith('hdfs://') else
                  '/'.join([self.hdfs_home, '.knitDeps', os.path.basename(f)])
                  for f in files]
        logger.debug("Resource files: %s" % rfiles)
        jfiles = ListConverter().convert(rfiles, gateway._gateway_client)
        jenv = MapConverter().convert(envvars, gateway._gateway_client)
        self.master.init(jfiles, jenv, cmd, num_containers,
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

        Returns
        -------
        container_list: List
            List of dicts with each container's details

        """
        if self.app_id:
            return self.yarn_api.app_containers(self.app_id)
        else:
            raise KnitException('Cannot get containers, app has not started')

    def get_container_statuses(self):
        """Get status info for each container

        Returns dict where the values are the raw text output.
        """
        return {c['id']: c['state'] for c in self.get_containers()}

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
        if container_id not in self.get_container_statuses():
            raise KnitException('Attempt to remove container nor owned by this'
                                'app: ' + container_id)
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
        if self.app_id:
            return self.yarn_api.logs(self.app_id, shell=shell)
        else:
            raise KnitException('Cannot get logs, app not started')

    def print_logs(self, shell=False):
        """print out a more console-friendly version of logs()"""
        for l, v in self.logs(shell).items():
            print('\n### Container ', l, ', id ', v.get('id', 'None'), ' ###\n')
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
                call(["cmd", "/c", "taskkill", "/f", "/t", "/pid",
                      str(self.proc.pid)])
            self.proc.terminate()
            self.proc.communicate()
            self.proc = None
        self.client = None
        out = self.runtime_status() == 'KILLED'
        return out

    def __del__(self):
        if self.app_id is not None:
            try:
                self.kill()
            except:
                pass
        self.app_id = None

    def status(self):
        """ Get status of an application

        Returns
        -------
        log: dictionary
            status of application
        """
        if self.app_id:
            return self.yarn_api.apps_info(self.app_id)
        else:
            raise KnitException("Cannot get status, app not started")
    
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

    def list_envs(self):
        """List knit conda environments already in HDFS
        
        Looks in staging directory for zip-files
        
        Returns: list of dict
            Details for each zip-file."""
        if self.hdfs:
            files = self.hdfs.ls(self.hdfs_home + '/.knitDeps/', True)
            return [f for f in files if f['name'].endswith('.zip')]
        else:
            raise ImportError('Set the `hdfs` attribute to be able to list'
                              'environments.')

    def check_needs_upload(self, path):
        """Upload is needed if file does not exist in HDFS or is older"""
        if self.upload_always:
            return True
        fn = '/'.join([self.hdfs_home, '.knitDeps', os.path.basename(path)])
        if self.hdfs and self.hdfs.exists(fn):
            st = os.stat(path)
            size = st.st_size
            t = st.st_mtime
            info = self.hdfs.info(fn)
            if info['size'] == size and t < info['last_mod']:
                return False
            else:
                return True
        else:
            return True

    @classmethod
    def _cleanup(cls):
        # called on program exit to destroy lingering connections/apps
        for instance in cls._instances:
            instance.kill()

atexit.register(Knit._cleanup)
