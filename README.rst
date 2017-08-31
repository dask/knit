knit
====

|Build Status| |Coverage Status|

The ``knit`` library provides a Python interface to Scala for interacting
with the YARN resource manager.

View the documentation_ for ``knit``.

Overview
--------

Quickstart
----------

Install from conda-forge

> conda install -c conda-forge knit

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
