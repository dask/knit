from __future__ import absolute_import, division, print_function

import os
import re
import requests
import logging
from subprocess import Popen, PIPE

logger = logging.getLogger(__name__)

JAR_FILE = "rambling-1.0-SNAPSHOT.jar"
JAVA_APP = "com.continuumio.rambling.Client"


class Rambling(object):
    def __init__(self, namenode="localhost", nm_port=9000,
                 resourcemanager="localhost", rm_port=9026):
        """
        Connection to HDFS/YARN

        Parameters
        ----------
        namenode: str
            Namenode hostname/ip
        nm_port: int
            Namenode Port (default: 9000)
        resourcemanager: str
            Resource Manager hostname/ip
        rm_port: int
            Resource Manager port (default: 9026)
        rm_port: int
            Resource Manager port (default: 9026)
        """
        self.namenode = os.environ.get("NAMENODE") or namenode
        self.nm_port = nm_port

        self.resourcemanager = os.environ.get("RESOURCEMANAGER") or resourcemanager
        self.rm_port = rm_port

    def start_application(self, cmd, num_containers=1, virtual_cores=1, memory=128):
        """
        Method to start a yarn app with a distributed shell

        Parameters
        ----------
        cmd: str
            command to run in each yarn container
        num_containers: int
            number of containers to start (default 1)
        num_containers: int
            Number of containers YARN should request (default: 1)
            * A container should be requested with the number of cores it can saturate, i.e.
            * the average number of threads it expects to have runnable at a time.
        virtual_cores: int
            Number of virtual cores per container (default: 1)
            * A node's capacity should be configured with virtual cores equal to
            * its number of physical cores.
        memory: int
            Memory per container (default: 128)
            * The unit for memory is megabytes.

        Returns
        -------
        applicationId: str
            A yarn application ID string
        """

        JAR_FILE_PATH = os.path.join(os.path.dirname(__file__), "java_libs", JAR_FILE)
        args = ["hadoop", "jar", JAR_FILE_PATH, JAVA_APP, "--numInstances", str(num_containers),
                "--command", cmd, "--virutalCores", str(virtual_cores), "--memory", str(memory)]

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

    def get_application_logs(self, app_id, shell=False):
        """
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

        host_port = "{}:{}".format(self.resourcemanager, self.rm_port)
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

    def get_application_status(self, app_id):
        """

        Parameters
        ----------
        app_id: str
             A yarn application ID string

        Returns
        -------
        log: dictionary
            status of application
        """
        host_port = "{}:{}".format(self.resourcemanager, self.rm_port)
        url = "http://{}/ws/v1/cluster/apps/{}".format(host_port, app_id)
        logger.debug("Getting Application Info: {}".format(url))
        r = requests.get(url)
        data = r.json()

        return data

    def kill_application(self, application_id):
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
