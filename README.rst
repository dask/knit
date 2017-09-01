knit
====

|Build Status| |Coverage Status|

The ``knit`` library provides a Python interface to Scala for interacting
with the YARN resource manager.

View the documentation_ for ``knit``.

Overview
--------

``knit`` allows you to use python in conjunction with YARN, the most common resource
manager for Hadoop systems.
It provides to following high-level entry-points:

- ``CondaCreator``, a way to create zipped conda environments, so that they can be uploaded to
  HDFS and extracted for use in YARN containers
- ``YARNAPI``, an interface to the YARN resource manager to get application/container statuses,
  logs, and to kill running jobs
- ``Knit``, a YARN application runner, which generates an instance of a scala-based YARN client,
  and launches an application on YARN, which in turn runs commands in YARN containers
- ``DaskYARNCluster``, launches a Dask distributed cluster on YARN, one worker process
  per container.

The intent is to use ``knit`` from a cluster edge-node, i.e.,
with YARN configuration and the CLI available locally.

Quickstart
----------

Install from conda-forge

> conda install -c conda-forge knit

or with pip

> pip install knit

If installing from source, you must first build the java library (requires java and maven)

> python setup.py install mvn

To run an arbitrary command on the yarn cluster

.. code-block:: python

   import knit
   k = knit.Knit()
   k.start('env')  # wait some time
   k.logs()

To start a dask cluster on YARN

.. code-block:: python

   from knit import dask_yarn
   cluster = dask_yarn.DaskYARNCluster()
   cluster.start(nworkers=4, memory=1024, cpus=2)


.. _documentation: http://knit.readthedocs.io/en/latest/


.. |Build Status| image:: https://travis-ci.org/dask/knit.svg?branch=master
   :target: https://travis-ci.org/dask/knit
.. |Coverage Status| image:: https://coveralls.io/repos/github/dask/knit/badge.svg
   :target: https://coveralls.io/github/dask/knit
