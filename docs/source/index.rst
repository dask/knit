knit
========

``Knit`` is an easy to use interface for launching distributed applications with `YARN`_.  Additionally, it
provides many conveniences for Python and other runtime languages.  Many YARN applications are
nothing more than distributed shell commands -- the `Hello World`_ for YARN is running a Unix
command on several nodes in a cluster.  ``knit`` implements a generalized distributed shell command
and allows users to express those commands/applications, as well as desired resources managed under YARN
through Python or directly on the command line.

IPython Parallel Example
~~~~~~~~~~~~~~~~~~~~~~~~

Install `IPython Parallel`_ and start IP Controller::

   $ conda install ipyparallel
   or
   $ pip ipyparallel
   $ ipcontroller --ip=*


IPController will create a file: ``ipcontroller-engine.json`` which contains metadata and security information
needed by worker nodes to connect back to the controller.  In a separate shell or terminal we use knit to
ship a self-contained environment with ``ipyparallel`` (and other dependenices) and start ``ipengine``

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


Motivation
----------

``Knit`` is built to support batch-oriented non JVM based applications.  For example, Knit can
deploy Python based distributed applications such as `Dask + Distributed`_ and `IPython Parallel`_

*  **Convenience:** General purpose YARN applications with easy to use CLI and Python interface
*  **PyData Support** Bring PyData stack into the Hadoop/YARN ecosystem
*  **Deployable Runtimes:** Knit can build and ship self contained runtime environments.  This means
   we can resolve library dependencies without significant IT infrastructure
   and management

Scope
-----

Knit enables data scientists to quickly launch, monitor, and destroy simple distributed programs.
Knit is not yet a full featured YARN solution.  Knit focuses on the common case in scientific workloads of starting a
distributed process on many workers for a relatively short period of time.  Knit does not
handle dynamic container management nor is it suitable for running infrastructural applications.

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
.. _`Dask + Distributed`: http://distributed.readthedocs.org/en/latest/

.. toctree::
   :maxdepth: 1

   install
   quickstart
   usage
   examples
   api


