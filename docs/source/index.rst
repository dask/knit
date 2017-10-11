Knit
====

*Knit launches YARN applications from Python.*

Knit provides Python methods to quickly launch, monitor, and destroy
distributed programs running on a YARN cluster, such as is found in traditional
Hadoop environments.

Knit was originally designed to deploy Dask_ applications on YARN, but can
deploy more general, non-Dask, applications as well.

Using conda_, Knit can also deploy fully-featured Python environments within
YARN containers, sending along useful libraries like NumPy, Pandas, and
Scikit-Learn to all of the containers in the YARN cluster.

Install
-------

Use ``pip`` or ``conda`` to install::

   conda install knit -c conda-forge
   # or
   pip install knit


Launch Generic Applications
---------------------------

Instantiate ``knit`` with valid ResourceManager/Namenode IP/Ports and create a command string to run
in all YARN containers

.. code-block:: python

   from knit import Knit
   k = Knit(autodetect=True) # autodetect IP/Ports for YARN/HADOOP

Create a software environment with necessary packages

.. code-block:: python

   env = k.create_env('my-environment',
                      packages=['python=3.5', 'scikit-learn','pandas'],
                      channels=['conda-forge'])  # specify anaconda.org channels

Run a command within that environment on multiple containers

.. code-block:: python

   cmd = 'python -c "import sys; print(sys.version_info);"'
   app_id = k.start(cmd, num_containers=2, env=env)  # start application

The ``start`` method also takes parameters to define the resource requirements
of the application like ``num_containers=``, ``memory=``, ``virtual_cores=``,
``env=``, and ``files=``.

Launch Dask
-----------

Knit makes it easy to launch Dask on Yarn:

.. code-block:: python

   from knit.dask_yarn import DaskYARNCluster
   env = k.create_env('my-environment',
                      packages=['python=3.6', 'scikit-learn', 'pandas', 'dask'],
                      channels=['conda-forge'])  # specify anaconda.org channels

   cluster = DaskYARNCluster(env=env)
   cluster.start(nworkers=10, memory=4096, cpus=2)

   from dask.distributed import Client
   client = Client(cluster)  # Connect local Dask client to cluster

If you want to connect to the remote Dask cluster from your local computer as
is done in the last line then your local and remote environments should be
similar.


Packaged Environments
---------------------

Yarn clusters typically lack strong Python environments with common libraries
like NumPy, Pandas, and Scikit Learn.  To resolve this, Knit creates
redeployable conda environments that can be shipped along with your Yarn job,
effectively bringing a fully-featured Python software environment to your Yarn
cluster.

To achieve this Knit uses redeployable conda_ environments.  Every time you
create a new environment Knit will use conda locally to manage and download
dependencies, and then will wrap those packages into a self-contained zip file
that can be shipped to Yarn applications.


Scope
-----

Knit is not a full featured YARN solution.  Knit focuses on the common case in
computational workloads of starting a distributed process on many workers for a
relatively short period of time.  It does not provide fine-grained access to
all Yarn functionality.


Related Work
------------

* `Apache Slider`_: General purpose YARN application with a focus on
  long-running applications/services: HBase, Accumulo, etc.
* kitten_: General purpose YARN application with Lua based configuration

See :doc:`the quickstart <quickstart>` to get started.


.. _YARN: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
.. _kitten: https://github.com/cloudera/kitten
.. _`Apache Slider`: https://slider.incubator.apache.org/
.. _Dask: http://dask.pydata.org/en/latest/
.. _conda: http://conda.pydata.org/docs/

.. toctree::
   :maxdepth: 1

   install
   quickstart
   usage
   troubleshooting
   api
   configuration
   examples
