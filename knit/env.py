from __future__ import absolute_import, division, print_function

import json
import os
import sys
import shutil
import requests
import logging
import tempfile
import zipfile
from subprocess import Popen, PIPE
import warnings

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
here = os.path.dirname(__file__)


def miniconda_url():
    """What to download for this platform"""
    if sys.platform.startswith('linux'):
        url = miniconda_urls['linux']
    elif sys.platform.startswith('darwin'):
        url = miniconda_urls['darwin']
    else:
        url = miniconda_urls['win']
    if not sys.maxsize > 2 ** 32:
        # 64bit check
        url = url.replace("_64", "")
    return url


class CondaCreator(object):
    """
    Create Conda Env

    The parameters below can generally be guessed from the system.
    If `conda info` is required, it will only be run on the first
    invocation, and the result cached.

    Parameters
    ----------
    conda_root: str
        Location of a conda installation. The conda executable is expected
        at /bin/conda within.
        If None, runs `conda info` to find out relevant information.
        If no conda is found for `conda info`, or no conda executable exists
        within the given location, will download and install miniconda
        at that location. If the value is None, that location is within the
        source tree.
    conda_envs: str
        directory in which to create environments; usually within the
        source directory (so as not to pollute normal usage of conda)
    miniconda_url: str
        location to download miniconda from, if needed. Uses `miniconda_urls`
        for the appropriate platform if not given.
    channels: list of str
        Channels to specify to conda. Note that defaults in .condarc will also
        be included
    conda_pkgs: str
        Directory containing cached conda packages, normally within conda_root.
    """
    conda_info = {}

    def __init__(self, conda_root=None, conda_envs=None, miniconda_url=None,
                 channels=None, conda_pkgs=None):
        if conda_root is None:
            self._get_conda_info()
            if self.conda_info:
                self.conda_root = self.conda_info['conda_prefix']
            else:
                self.conda_root = os.path.join(here, 'tmp_conda')
        self.conda_bin = os.path.join(self.conda_root, 'bin', 'conda')
        if not os.path.exists(self.conda_bin):
            self._install_miniconda(self.conda_root, miniconda_url)
        self.conda_envs = conda_envs or os.sep.join([here, 'tmp_conda', 'envs'])
        self.conda_pkgs = conda_pkgs
        self.channels = channels or []

    def _install_miniconda(self, root, url):
        url = url or miniconda_url()
        tmp = tempfile.mkdtemp()
        minifile = os.path.join(tmp, 'Miniconda3')
        logger.debug("Downloading latest Miniconda.sh")
        r = requests.get(url, stream=True)
        with open(minifile, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
            f.flush()
        install_cmd = "bash {0} -b -p {1}".format(minifile, root).split()
        logger.debug("Installing Miniconda in {0}".format(root))
        proc = Popen(install_cmd, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        logger.debug(out)
        logger.debug(err)
        self.conda_info['conda_prefix'] = root

    def _get_conda_info(self):
        """Ask a conda on PATH where it is installed"""
        if self.conda_info:
            # already did this before
            return
        try:
            self.conda_info.update(json.loads(shell_out(
                ['conda', 'info', '--json'])))
        except (OSError, IOError):
            warnings.warn('No conda found on PATH')

    def _create_env(self, env_name, packages=None, remove=False):
        """
        Create Conda env environment. If env_name is found in self.conda_envs,
        if will be used without checking the existence of any given packages

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
        env_path = os.path.join(self.conda_envs, env_name)

        if os.path.exists(env_path):
            if not remove:
                # assume env is OK, ignore packages.
                return env_path
            shutil.rmtree(env_path)

        if not isinstance(packages, list):
            raise TypeError("Packages must be a list of strings")

        ch = []
        [ch.extend(['-c', c]) for c in self.channels]
        cmd = [self.conda_bin, 'create', '-p', env_path, '-y',
               '-q'] + packages + ch
        logger.info("Creating new env {0}".format(env_name))
        logger.info(' '.join(cmd))

        env = dict(os.environ)
        if self.conda_pkgs:
            env['CONDA_PKGS_DIRS'] = self.conda_pkgs
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, env=env)
        out, err = proc.communicate()

        logger.debug(out)
        logger.debug(err)

        env_python = os.path.join(env_path, 'bin', 'python')

        if not os.path.exists(env_python):
            raise CondaException("Failed to create Python binary at %s."
                                 "" % env_python)

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

        env_path = os.path.join(self.conda_envs, env_name)

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
