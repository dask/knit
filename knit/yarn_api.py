from __future__ import absolute_import, division, print_function

import os
import requests
import logging
from functools import wraps
from subprocess import STDOUT

from .utils import shell_out
from .exceptions import YARNException
logger = logging.getLogger(__name__)


class YARNAPI(object):
    def __init__(self, rm, rm_port):
        self.rm = rm
        self.rm_port = rm_port
        self.host_port = "{0}:{1}".format(self.rm, self.rm_port)

    @property
    def apps(self):
        url = "http://{0}/ws/v1/cluster/apps/".format(self.host_port)
        logger.debug("Getting Resource Manager Info: {0}".format(url))
        r = requests.get(url)
        data = r.json()
        logger.debug(data)
        if not data['apps']:
            return []
        apps = [d['id'] for d in data['apps']['app']]
        return apps

    def app_containers(self, app_id=None, info=None):
        """
        Get list of container information for given app. If given app_id,
        will automatically get its info, or can skip by providing the info
        directly.
        
        Parameters
        ----------
        app_id: str
            YARN ID for the app
        info: dict
            Produced by app_info()
    
        Returns
        -------
        List of container info dictionaries
        """
        if (app_id is None) == (info is None):
            raise TypeError('Must provide app_id or info')
        if app_id:
            info = self.status(app_id)

        amHostHttpAddress = info['app']['amHostHttpAddress']

        url = "http://{0}/ws/v1/node/containers".format(
            amHostHttpAddress)
        r = requests.get(url)

        data = r.json()['containers']
        if not data:
            raise YARNException("No container logs available")

        container = data['container']
        logger.debug(container)

        # container_1452274436693_0001_01_000001
        def get_app_id_num(x):
            return "_".join(x.split("_")[1:3])

        app_id_num = get_app_id_num(app_id)
        containers = [d for d in container
                      if get_app_id_num(d['id']) == app_id_num]
        return containers

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
        if not shell:
            try:
                containers = self.app_containers(app_id)
                logs = {}
                for c in containers:
                    log = dict(nodeId=c['nodeId'])

                    # grab stdout
                    url = "{0}/stdout/?start=0".format(c['containerLogsLink'])
                    logger.debug("Gather stdout/stderr data from {0}:"
                                 " {1}".format(c['nodeId'], url))
                    r = requests.get(url)
                    log['stdout'] = r.text

                    # grab stderr
                    url = "{0}/stderr/?start=0".format(c['containerLogsLink'])
                    r = requests.get(url)
                    log['stderr'] = r.text

                    logs[c['id']] = log

                return logs

            except Exception:
                logger.warning("Error while attempting to fetch logs,"
                               " using fallback", exc_info=1)

        # fallback
        # TODO: this is just a location in HDFS
        cmd = ["yarn", "logs", "-applicationId", app_id]
        out = shell_out(cmd)
        return str(out)

    def container_status(self, container_id):
        """Ask the YARN shell about the given container"""
        cmd = ["yarn", "container", "-status", container_id]
        return str(shell_out(cmd))

    def status(self, app_id):
        """ Get status of an application

        Parameters
        ----------
        app_id: str
             A yarn application ID string

        Returns
        -------
        dictionary: status of application
        """
        host_port = "{0}:{1}".format(self.rm, self.rm_port)
        url = "http://{0}/ws/v1/cluster/apps/{1}".format(host_port, app_id)
        logger.debug("Getting Application Info: {0}".format(url))
        r = requests.get(url)
        data = r.json()
        logger.debug(data)
        return data

    def kill_all(self, knit_only=True):
        """Kill a set of applications
        
        Parameters
        ----------
        knit_only: bool (True)
            Only kill apps with the name 'knit' (i.e., ones we started)
        """
        for app in self.apps:
            stat = self.status(app)['app']
            if knit_only and stat['name'] != 'knit':
                continue
            if stat['state'] not in ['KILLED', 'FINISHED']:
                continue
            self.kill(app)

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

        cmd = ["yarn", "application", "-kill", app_id]
        out = shell_out(cmd, stderr=STDOUT)

        return "Killed application" in out
