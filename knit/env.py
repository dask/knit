from __future__ import absolute_import, division, print_function

import os
import sys
import requests
import logging
from subprocess import check_call
mini_file = "Miniconda-latest.sh"

linux_miniconda_url = "https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh"
osx_miniconda_url = "https://repo.continuum.io/miniconda/Miniconda-latest-MacOSX-x86_64.sh"
win_miniconda_url = "https://repo.continuum.io/miniconda/Miniconda-latest-Windows-x86_64.exe"

logger = logging.getLogger(__name__)

class CondaCreator(object):
    """
    Create Conda Env
    """

    def __init__(self, conda_root=None):
        self.conda_dir = os.path.join(os.path.dirname(__file__), 'tmp_conda')

        self.minifile_fp = os.path.join(self.conda_dir, mini_file)
        self.conda_root = conda_root or os.path.join(self.conda_dir, 'miniconda')
        self.python_bin = os.path.join(self.conda_root, 'bin', 'python')
        self.conda_bin = os.path.join(self.conda_root, 'bin', 'conda')


    @property
    def miniconda_url(self):
        conda_os = zip(['linux', 'darwin', 'win'], [linux_miniconda_url, osx_miniconda_url, win_miniconda_url])
        conda_os = dict((system, url) for system, url in conda_os)
        plat = sys.platform

        if sys.platform.startswith('linux'):
            url = conda_os['linux']
        elif sys.platform.startswith('darwin'):
            url = conda_os['darwin']
        else:
            url = conda_os['win']

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

        mini_file = os.path.join(self.conda_dir, self.minifile_fp)
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

    def _install(self):
        """
        Returns True if miniconda is successfully installed
        """

        install_cmd = "bash {} -b -p {}".format(self.minifile_fp, self.conda_root).split()

        if self.miniconda_check:
            return self.conda_root

        self._download_miniconda()
        logger.debug("Installing Miniconda in {}".format(self.conda_root))
        check_call(install_cmd)
        return os.path.exists(self.python_bin)

    def create_env(self, env_name, packages=[]):
        """
        Create Conda env environment

        Parameters
        ----------
        env_name : str
        packages : list

        Returns
        -------
        path : str
            path to zipped conda environment
        """

        if not isinstance(packages, list):
            raise TypeError("Packages must be a list of strings")

        p_str = ' '.join(packages)
        cmd = [self.conda_bin, 'create', '-n', env_name, p_str, ' --copy']
        logger.info("Creating new env {}".format(env_name))
        logger.info(cmd)
        check_call(cmd)








