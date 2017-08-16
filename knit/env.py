from __future__ import absolute_import, division, print_function

import os
import sys
import shutil
import requests
import logging
import zipfile
from subprocess import Popen, PIPE

from .exceptions import CondaException
from .utils import shell_out

mini_file = "Miniconda-latest.sh"
miniconda_urls = {"linux": "https://repo.continuum.io/miniconda/"
                           "Miniconda3-latest-Linux-x86_64.sh",
                  "darwin": "https://repo.continuum.io/miniconda/"
                            "Miniconda3-latest-MacOSX-x86_64.sh",
                  "win": "https://repo.continuum.io/miniconda/"
                         "Miniconda3-latest-Windows-x86_64.exe"}

logger = logging.getLogger(__name__)


class CondaCreator(object):
    """
    Create Conda Env
    """

    def __init__(self, conda_root=None, channels=[]):
        self.conda_dir = os.path.join(os.path.dirname(__file__), 'tmp_conda')

        self.minifile_fp = os.path.join(self.conda_dir, mini_file)
        self.conda_root = conda_root or os.path.join(self.conda_dir, 'miniconda')
        self.python_bin = os.path.join(self.conda_root, 'bin', 'python')
        self.conda_envs = os.path.join(self.conda_root, 'envs')
        self.conda_bin = os.path.join(self.conda_root, 'bin', 'conda')
        self.channels = channels

    @property
    def miniconda_url(self):

        if sys.platform.startswith('linux'):
            url = miniconda_urls['linux']
        elif sys.platform.startswith('darwin'):
            url = miniconda_urls['darwin']
        else:
            url = miniconda_urls['win']

        # 64bit check
        if not sys.maxsize > 2**32:
            url = url.replace("_64", "")

        return url

    @property
    def miniconda_check(self):
        return os.path.exists(self.conda_root)

    def _download_miniconda(self):
        if not os.path.exists(self.conda_dir):
            os.mkdir(self.conda_dir)

        mini_file = self.minifile_fp
        if os.path.exists(mini_file):
            return mini_file

        logger.debug("Downloading latest Miniconda.sh")
        r = requests.get(self.miniconda_url, stream=True)
        with open(mini_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
            f.flush()

        return os.path.abspath(mini_file)

    def _install_miniconda(self):
        """
        Install miniconda.

        Returns True if miniconda is successfully installed or was previously
        created
        """

        if self.miniconda_check:
            return self.conda_root

        install_cmd = "bash {0} -b -p {1}".format(self.minifile_fp, self.conda_root).split()

        self._download_miniconda()
        logger.debug("Installing Miniconda in {0}".format(self.conda_root))

        proc = Popen(install_cmd, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()

        logger.debug(out)
        logger.debug(err)

        return os.path.exists(self.python_bin)

    def _create_env(self, env_name, packages=None, remove=False):
        """
        Create Conda env environment

        Parameters
        ----------
        env_name : str
        packages : list
        remove : bool
            remove environment should it exist - start from 

        Returns
        -------
        path : str
            path to newly created conda environment
        """

        # ensure miniconda is installed
        self._install_miniconda()
        env_path = os.path.join(self.conda_root, 'envs', env_name)

        if os.path.exists(env_path):
            conda_list = shell_out(
                [self.conda_bin, 'list', '-n', env_name]).split()

            # filter out python/python=3
            pkgs = [p for p in packages if 'python' not in p]

            # try to be idempotent -- if packages exist don't recreate
            if all(p in conda_list for p in pkgs):
                return env_path

            if not remove:
                raise CondaException("Conda environment: {0} already exists"
                                     " but does not contain {1}".format(
                                        env_name, packages))
            else:
                shutil.rmtree(env_path)

        if not isinstance(packages, list):
            raise TypeError("Packages must be a list of strings")

        ch = []
        [ch.extend(['-c', c]) for c in self.channels]
        cmd = [self.conda_bin, 'create', '-p', env_path, '-y',
               '-q'] + packages + ch
        logger.info("Creating new env {0}".format(env_name))
        logger.info(' '.join(cmd))

        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()

        logger.debug(out)
        logger.debug(err)

        env_python = os.path.join(env_path, 'bin', 'python')

        if not os.path.exists(env_python):
            raise CondaException("Failed to create Python binary.")

        return env_path

    def find_env(self, env_name):
        """
        Find full path to env_name

        Parameters
        ----------
        env_name : str

        Returns
        -------
        path : str
            path to conda environment
        """

        env_path = os.path.join(self.conda_root, 'envs', env_name)

        if os.path.exists(env_path):
            return env_path

    def create_env(self, env_name, packages=None, remove=False):
        """
        Create zipped directory of a conda environment

        Parameters
        ----------
        env_name : str
        packages : list
        remove : bool
            remove environment should it exist

        Returns
        -------
        path : str
            path to zipped conda environment
        """

        if not packages:
            env_path = self.find_env(env_name)
        else:
            env_path = self._create_env(env_name, packages, remove)

        return self.zip_env(env_path)

    def zip_env(self, env_path):
        """
        Zip env directory

        Parameters
        ----------
        env_path : string

        Returns
        -------
        path : string
            path to zipped file
        """

        fname = os.path.basename(env_path) + '.zip'
        env_dir = os.path.dirname(env_path)
        zFile = os.path.join(env_dir, fname)
        
        # ZipFile does not have a contextmanager in Python 2.6
        f = zipfile.ZipFile(zFile, 'w')
        try:
            for root, dirs, files in os.walk(env_path):
                for file in files:
                    relfile = os.path.join(
                        os.path.relpath(root, self.conda_envs), file)
                    absfile = os.path.join(root, file)
                    f.write(absfile, relfile)
            return zFile

        finally:
            f.close()
