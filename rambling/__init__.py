from __future__ import absolute_import, division, print_function

import os
import subprocess
from subprocess import Popen, PIPE

JAR_FILE = "rambling-1.0-SNAPSHOT.jar"
NAMENODE = os.environ.get("NAMENODE") or "salt-master-hostname:9000"
HDFS_JAR_PATH = os.path.join("hdfs://", NAMENODE, "jars", JAR_FILE)
JAVA_APP = "com.continuumio.rambling.Client"

def start_application(cmd, num_containers=1):
    """
    Method to start a cm

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



