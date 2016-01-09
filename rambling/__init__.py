from __future__ import absolute_import, division, print_function

import os
import subprocess
import requests
from subprocess import Popen, PIPE

JAR_FILE = "rambling-1.0-SNAPSHOT.jar"
NAMENODE = os.environ.get("NAMENODE") or "salt-master-hostname:9000"
HDFS_JAR_PATH = os.path.join("hdfs://", NAMENODE, "jars", JAR_FILE)
JAVA_APP = "com.continuumio.rambling.Client"

def start_application(cmd, num_containers=1):
    """
    Method to start a yarn app with a distributed shell

    Parameters
    ----------
    num_containers: int
        number of containers to start (default 1)

    cmd: str
        command to run in each yarn container

    Returns
    -------
    applicationId: str
        A yarn application ID string

    """

    JAR_FILE_PATH = os.path.join(os.path.dirname(__file__), "java_libs", JAR_FILE)
    args = ["hadoop", "jar", JAR_FILE_PATH, JAVA_APP, HDFS_JAR_PATH, str(num_containers), cmd]

    proc = subprocess.Popen(args, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()

    # last string in out is applicationId
    # TODO Better JAVA Python communcation: appId, Resources, Yarn, etc.
    # appId = out.split()[-1]
    return out, err



def get_application_logs(app_id, resource_manager, port):
    """

    Parameters
    ----------
    app_id: str
         A yarn application ID string

    resource_manager: str
             Resource Manager host name/ip

    port: str
             port for Resource Manager

    Returns
    -------
    log: dictionary
        logs from each container
    """

    url = "http://{}:{}/ws/v1/cluster/apps/{}".format(resource_manager, port, app_id)
    r = requests.get(url)
    data = r.json()

    amHostHttpAddress = data['app']['amHostHttpAddress']
    url = "http://{}/ws/v1/node/containers".format(amHostHttpAddress)
    r = requests.get(url)
    data = r.json()['containers']['container']

    #container_1452274436693_0001_01_000001
    get_app_id_num = lambda x: "_".join(x.split("_")[1:3])

    app_id_num = get_app_id_num(app_id)
    containers = [d for d in data if get_app_id_num(d['id']) == app_id_num]

    logs = {}
    for c in containers:
        log = {}
        log['nodeId'] = c['nodeId']

        # grab stdout
        url = "{}/stdout/?start=0".format(c['containerLogsLink'])
        r = requests.get(url)
        log['stdout'] = r.text

        # grab stderr
        url = "{}/stderr/?start=0".format(c['containerLogsLink'])
        r = requests.get(url)
        log['stderr'] = r.text

        logs[c['id']] = log

    return logs