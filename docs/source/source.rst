Running from source
===================

The following steps can be used to install and run ``rambling`` from source.
These instructions were tested on Ubuntu 14.04, CDH 5.5.1, and Hadoop 2.6.0.

Update and install system dependencies:

.. code-block:: bash
    
   $ sudo apt-get update
   $ sudo apt-get install git maven openjdk-7-jdk -y

Clone git repository and build maven project:

.. code-block:: bash

   $ git clone https://github.com/blaze/rambling
   $ cd rambling/rambling_jvm
   $ mvn clean install

Copy JAR files into HDFS:

.. code-block:: bash

   $ cd target
   $ hdfs dfs -mkdir /jars
   $ hdfs dfs -put -f ./rambling-1.0-SNAPSHOT.jar /jars


Usage
-----

Python
~~~~~~

.. code-block:: python

   import rambling
   r = rambling.Rambling(namenode="ip-XX-XXX-XX", resourcemanager="ip-XX-XXX-XX")
   cmd = "python -c 'import sys; print(sys.path); import socket; print(socket.gethostname())'"
   appId = r.start_application(cmd)
   r.get_application_logs(appId)

Java
~~~~

.. code-block:: bash

   $ hadoop jar ./rambling-1.0-SNAPSHOT.jar com.continuumio.rambling.Client --help
   $ hadoop jar ./rambling-1.0-SNAPSHOT.jar com.continuumio.rambling.Client --jarPath hdfs://{{NAMENODE}}:9000/jars/rambling-1.0-SNAPSHOT.jar --numInstances 1 --command "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"

.. code-block:: bash

   $ ./rambling-1.0-SNAPSHOT.jar com.continuumio.rambling.Client hdfs://localhost:9000/jars/rambling-1.0-SNAPSHOT.jar 1 "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"


Helpful aliases
---------------

.. code-block:: bash

   $ alias yarn-status='yarn application -status'
   $ alias yarn-log='yarn logs -applicationId'
   $ alias yarn-kill='yarn application -kill'
