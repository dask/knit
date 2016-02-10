Usage
=====

``Knit`` can be used in several novel ways.  Our primary concern is supporting easy deployment of
distributed Python runtimes; though, we can also consider other languages (R, Julia, etc) should
interest develop.  Below are a few novels ways we can currently use ``Knit``

Python
~~~~~~

The example below use Python found in the ``$PATH``.  This is usually the system Python.

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
user's home HDFS space e.g. ``/Users/ubuntu/.knitDeps`` and the following bash ENVIRONMENT variables
will be available:

- ``$CONDA_PREFIX``: full path to prefix location of zipped directory
- ``$PYTHON_BIN``: full path to Python binary

With the ENVIRONMENT variables available users can build more nuanced commands like the following:

.. code-block:: python

   >>> cmd = '$PYTHON_BIN $CONDA_PREFIX/bin/dworker 8786'

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

JVM CLI
~~~~~~~

We can also call out to the HADOOP jar directly::

   $ hadoop jar ./knit-1.0-SNAPSHOT.jar io.continuum.knit.Client --help
      knit x.1
      Usage: scopt [options]

        -n <value> | --numContainers <value>
              Number of YARN containers
        -m <value> | --memory <value>
              Amount of memory per container
        -c <value> | --virtualCores <value>
              Virtual cores per container
        -C <value> | --command <value>
              Command to run in containers
        -p <value> | --pythonEnv <value>
              Number of YARN containers
        --help
              command line for launching distributed python

   $ hadoop jar ./knit-1.0-SNAPSHOT.jar io.continuum.knit.Client --numContainers 1 \
     --command "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"


Helpful aliases
---------------

.. code-block:: bash

   $ alias yarn-status='yarn application -status'
   $ alias yarn-log='yarn logs -applicationId'
   $ alias yarn-kill='yarn application -kill'
