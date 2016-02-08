from __future__ import absolute_import, division, print_function

import os
import requests
import logging
from functools import wraps
from subprocess import Popen, PIPE

from .utils import shell_out
from .exceptions import YARNException
logger = logging.getLogger(__name__)


def check_app_id(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        cls = args[0]
        app_id = args[1]
        if app_id not in cls.apps:
            raise YARNException("{}: not a valid Application "
                                "Id".format(app_id))
        return func(*args, **kwargs)
    return wrapper


class YARNAPI(object):
    def __init__(self, rm, rm_port):
        self.rm = rm
        self.rm_port = rm_port
        self.host_port = "{}:{}".format(self.rm, self.rm_port)


    @property
    def apps(self):
        url = "http://{}/ws/v1/cluster/apps/".format(self.host_port)
        logger.debug("Getting Resource Manager Info: {}".format(url))
        r = requests.get(url)
        data = r.json()
        logger.debug(data)
        if not data['apps']:
            return []
        apps = [d['id'] for d in data['apps']['app']]
        return apps

    @check_app_id
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

        if shell:

            cmd = ["yarn", "logs", "-applicationId", app_id]

            out = shell_out(cmd)
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

        data = r.json()['containers']
        if not data:
            raise YARNException("No container logs available")

        container = data['container']
        logger.debug(container)

        # container_1452274436693_0001_01_000001
        def get_app_id_num(x):
            return "_".join(x.split("_")[1:3])

        app_id_num = get_app_id_num(app_id)
        containers = [d for d in container if get_app_id_num(d['id']) == app_id_num]

        logs = {}
        for c in containers:
            log=dict(nodeId=c['nodeId'])

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

    @check_app_id
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

    @check_app_id
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

        # need Popen because YARN killed message occurs on stderr
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()

        logger.debug(out)
        logger.debug(err)

        return any("Killed application" in s for s in [str(out), str(err)])
