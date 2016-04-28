knit
====

Knit enables data scientists to quickly launch, monitor, and destroy distributed programs on a YARN cluster.

Many YARN applications are simple distributed shell commands -- running a shell command on several nodes in a cluster.
Knit enables users to express and deploy applications and software environments managed under YARN through Python or
directly on the command line.


Motivation
----------

Knit was built to support batch-oriented non-JVM applications.  For example, Knit can deploy Python based distributed
applications such as `IPython Parallel`_ and `Dask+Distributed`_.  Knit was built with the following motivations in
mind:

*  **PyData Support** Bring the PyData stack into the Hadoop/YARN ecosystem
*  **Easy Setup:** Support a minimal installation effort and the common cases with easy to use CLI and Python interface.
*  **Deployable Runtimes:** Build and ship self contained environments along with the application.  Knit uses `conda`_
   to resolve library dependencies and deploy user libraries without IT infrastructure and management


Scope
-----

Knit enables data scientists to quickly launch, monitor, and destroy simple distributed programs.

Knit is not a full featured YARN solution.  Knit focuses on the common case in scientific workloads of starting a
distributed process on many workers for a relatively short period of time.  Knit does not handle dynamic container
management nor is it suitable for running long-term infrastructural applications.


IPython Parallel Example
~~~~~~~~~~~~~~~~~~~~~~~~

As an example we install and deploy the popular `IPython Parallel`_ project on a YARN cluster.  This example can be
performed by anyone with user privileges on a YARN cluster and a local Python installation.  It does not depend on root
privileges nor on Python being widely deployed throughout the cluster.

We install and start the IP Controller on the head node::

   $ conda install ipyparallel
   or
   $ pip install ipyparallel
   $ ipcontroller --ip=*


The IPController creates a file: ``ipcontroller-engine.json`` which contains metadata and security information
needed by worker nodes to connect back to the controller.  In a separate shell or terminal we use knit to
ship a self-contained environment with ``ipyparallel`` (plus other dependencies) and start ``ipengine``

.. code-block:: python

   >>> from knit import Knit
   >>> k = Knit(autodetect=True)
   >>> env = k.create_env(env_name='ipyparallel', packages=['numpy', 'ipyparallel', 'python=3'])
   >>> controller = '/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json'
   >>> cmd = '$PYTHON_BIN $CONDA_PREFIX/bin/ipengine --file=ipcontroller-engine.json'
   >>> app_id = k.start(cmd, env=env, files=[controller], num_containers=3)

IPython Parallel is now running in 3 containers on our YARN managed cluster:

.. code-block:: python

   >>> from ipyparallel import Client
   >>> c = Client()
   >>> c.ids
   [2, 3, 4]


Related Work
------------

* `Apache Slider`_: General purpose YARN application with a focus on long-running applications/services:
  HBase, Accumulo, etc.
* kitten_: General purpose YARN application with Lua based configuration

See :doc:`the quickstart <quickstart>` to get started.


.. _YARN: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
.. _Hello World: http://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html
.. _kitten: https://github.com/cloudera/kitten
.. _`Apache Slider`: https://slider.incubator.apache.org/
.. _`IPython Parallel`: https://ipython.org/ipython-doc/3/parallel/
.. _`Dask+Distributed`: http://distributed.readthedocs.io/en/latest/
.. _`conda`: http://conda.pydata.org/docs/

.. toctree::
   :maxdepth: 1

   install
   quickstart
   usage
   examples
   api
