Quickstart
==========

Install
-------

Use ``pip`` or ``conda`` to install::

   $ pip install knit --upgrade
   $ conda install knit -c conda-forge

Commands
--------

Start
~~~~~

Instantiate ``knit`` with valid ResourceManager/Namenode IP/Ports and create a command string to run
in all YARN containers

.. code-block:: python

   >>> from knit import Knit
   >>> k = Knit(autodetect=True) # autodetect IP/Ports for YARN/HADOOP
   >>> cmd = 'date'
   >>> k.start(cmd)
   'application_1454900586318_0004'

``start`` also takes parameters: ``num_containers``, ``memory``,
``virtual_cores``, ``env``, and ``files``

Status
~~~~~~

After starting/submitting a command you can monitor its progress.  The ``status`` method
communicates with YARN's `ResourceManager`_ and returns a python dictionary with current
monitoring data.

.. code-block:: python

   >>> k.status()
   {'app': {'allocatedMB': 512,
  'allocatedVCores': 1,
  'amContainerLogs': 'http://192.168.1.3:8042/node/containerlogs/container_1454100653858_0011_01_000001/ubuntu',
  'amHostHttpAddress': '192.168.1.3:8042',
  'applicationTags': '',
  'applicationType': 'YARN',
  'clusterId': 1454100653858,
  'diagnostics': '',
  'elapsedTime': 123800,
  'finalStatus': 'UNDEFINED',
  'finishedTime': 0,
  'id': 'application_1454100653858_0011',
  'memorySeconds': 63247,
  'name': 'knit',
  'numAMContainerPreempted': 0,
  'numNonAMContainerPreempted': 0,
  'preemptedResourceMB': 0,
  'preemptedResourceVCores': 0,
  'progress': 0.0,
  'queue': 'default',
  'runningContainers': 1,
  'startedTime': 1454276990907,
  'state': 'ACCEPTED',
  'trackingUI': 'UNASSIGNED',
  'user': 'ubuntu',
  'vcoreSeconds': 123}}

Often we track the ``state`` of an application.  Possible ``states`` include: ``NEW``,
``NEW_SAVING``, ``SUBMITTED``, ``ACCEPTED``, ``RUNNING``, ``FINISHED``, ``FAILED``, ``KILLED``

Logs
~~~~

We retrieve log data directly from a ``RUNNING`` Application Master::

   >>> k.logs()

Or, if log aggregation is enabled, we retrieve the resulting aggregated log data stored in HDFS.  Note:
aggregated log data is only available **after** the application has finished or been terminated,
usually with a small lag of a few seconds while log aggregation takes place.


Kill
~~~~

To stop an application from executing immediately, use the ``kill`` method::

   >>> k.kill()


Python Applications
-------------------

Python applications can be created by first making a conda environment for them to run within.
This can be done directly with ``CondaCreator`` (and such environments are cached and reused)
or with the ``knit`` instance itself.

A simple Python based application:

.. code-block:: python

   from knit import Knit
   k = Knit()

   env = k.create_env('test', packages=['python=3.5']])
   cmd = 'python -c "import sys; print(sys.version_info); import random; print(str(random.random()))"'
   app_id = k.start(cmd, num_containers=2, env=env)

A long running Python application. Here we reuse the same environment create above:

.. code-block:: python

   from knit import Knit
   k = Knit()

   cmd = 'python -m SimpleHTTPServer'
   app_id = k.start(cmd, num_containers=2, env=env)

.. _ResourceManager: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
