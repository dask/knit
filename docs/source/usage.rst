Usage
=====

``Knit`` can be used in several novel ways.  Our primary concern is supporting easy deployment of
distributed Python runtimes; though, we can also consider other languages (R, Julia, etc) should
interest develop.  Below are a few novels ways we can currently use ``Knit``

Python
~~~~~~

The example below use any Python found in the ``$PATH``.  This is usually the system Python (i.e.,
on a cluster where it has already been installed for you).

.. code-block:: python

   >>> import knit
   >>> k = knit.Knit()
   >>> cmd = "python -c 'import sys; print(sys.path); import socket; print(socket.gethostname())'"
   >>> appId = k.start(cmd)


Zipped Conda Envs
~~~~~~~~~~~~~~~~~

Often nodes managed under YARN may not have desired Python libraries or the Python binary at all!  In these cases,
we want to package up an environment to be shipped along with the command.  ``knit`` allows us to declare a
zipped directory with the following structure typical of Python environments::


   $ ll dev/
   drwxr-xr-x+ 23 ubuntu  ubuntu   782B Jan 30 17:55 bin
   drwxr-xr-x+ 20 ubuntu  ubuntu   680B Jan 30 17:55 include
   drwxr-xr-x+ 39 ubuntu  staff    1.3K Jan 30 17:55 lib
   drwxr-xr-x+  4 ubuntu  staff    136B Jan 30 17:55 share
   drwxr-xr-x+  6 ubuntu  ubuntu   204B Jan 30 17:55 ssl

.. code-block:: python

   >>> appId = k.start(cmd, env='<full-path>/dev.zip')

When we ship ``<full-path>/dev.zip``, knit uploads ``dev.zip`` to a temporary directory within the
user's home HDFS space e.g. ``/users/ubuntu/.knitDeps`` and the following bash ENVIRONMENT variables
will be available:

- ``$CONDA_PREFIX``: full path to prefix location of zipped directory
- ``$PYTHON_BIN``: full path to Python binary

With the ENVIRONMENT variables available users can build more nuanced commands like the following:

.. code-block:: python

   >>> cmd = '$PYTHON_BIN $CONDA_PREFIX/bin/dask-worker 8786'

``knit`` also provides a convenience method with ``conda`` to help build zipped environments.  The following
builds an environment ``env.zip`` with Python 3.5 and a variety of popular data Python libraries:

.. code-block:: python

   >>> env_zip = k.create_env(env_name='dev', packages=['python=3', 'distributed',
   ...                                                  'dask', 'pandas', 'scikit-learn'])

Adding Files
~~~~~~~~~~~~
``Knit`` can also pass local files to each container.

.. code-block:: python

   >>> files = ['creds.txt', 'data.csv']
   >>> k.start(cmd, files=files)

With the above, we are send files ``creds.txt`` and ``data.csv`` to each container and can reference
them as local file paths in the ``cmd`` command.

Dask Clusters
~~~~~~~~~~~~~

The previous methods can be combined to launch a full distributed dask cluster on YARN with code
like the following

.. code-block:: python

   from dask_yarn import DaskYARNCluster
   cluster = DaskYARNCluster(env='my/conda/env.zip')
   cluster.start(8, cpu=2, memory=2048)

The object ``cluster`` starts a dask scheduler, and can also be used to start or stop more
containers than the original 8 referenced above. The same set of config options apply as for a
``Knit`` object, in addition to conda creation options, which will define the environment in
which the workers run.

To start a dask client in the same session, you can simply do

.. code-block:: python

   from dask.distributed import Client
   c = Client(cluster)

and use as usual, or look at ``cluster.scheduler_address`` for clients connecting from other sessions.

Note that DaskYARNCluster can also be used as a context manager, which will ensure that it gets
closed (and the corresponding YARN application killed) when the ``with`` context finishes.


Instance Connections
~~~~~~~~~~~~~~~~~~~~

The main instances that you will handle in this library have attributes which
are instances of other classes, and expose functionality. Generally, parameters are
passed down, so that the constructor parameters for DaskYarnCluster will also be used
for Knit (e.g., ``replication_factor``), CondaCreator (e.g., ``channels``) and YARNAPI
(e.g., ``rm``).

DaskYarnCluster:

- ``.knit`` is an instance of Knit, and exposes methods to check the yarn application
state, logs and to increase/decrease the container count
- an instance of ``CondaCreator`` is created on-the-fly if making/zipping a conda environment
- ``.local_cluster`` is an instance of ``dask.distributed.LocalCluster``, with no local
workers. The only parameter passed in is ``ip``.

Knit:

- ``.yarn_api`` is an instance of YARNAPI, which provides commands to be directly
executed by the ResourceManager, including several informational calls, mostly via REST
- an instance of ``CondaCreator`` is created on-the-fly if making/zipping a conda environment


