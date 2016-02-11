knit
========

``Knit`` is an easy interface for launching distributed applications with `YARN`_.  Additionally, it
provides many conveniences for Python and other runtime languages.  Many YARN applications are
nothing more than distributed shell commands -- the `Hello World`_ for YARN is running a Unix
command on several nodes in a cluster.  ``knit`` implements a generalized distributed shell command
and allows users to express commands/applications, as well as desired resources managed under YARN
through Python or directly on the command line.

.. code-block:: python

   from knit import Knit
   k = Knit(autodetect=True) # autodetect IP/Ports for YARN

   app_id = k.start('date', num_containers=10, memory=4096)
   status = k.status(app_id)
   logs = k.logs(app_id)

Motivation
----------

``Knit`` is built to support long-running non JVM based applications.  For example, Knit can
deploy Python based distributed applications such as `IPython Parallel`_

*  **Convenience:** General purpose YARN applications with easy to use CLI and Python interface
*  **PyData Support** Bring PyData stack into the Hadoop/YARN ecosystem
*  **Deployable Runtimes:** Knit can build and ship self contained runtime environments.  This means
   we can resolve library dependencies without significant IT infrastructure
   and management

*Note: we currently do not support dynamic container management*

Related Work
------------

* `Apache Slider`_: General purpose YARN application with a focus on establish applications/services:
   HBase, Accumulo, etc.
* kitten_: General purpose YARN application with Lua based configuration

See :doc:`the quickstart <quickstart>` to get started.


.. _YARN: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
.. _Hello World: http://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html
.. _kitten: https://github.com/cloudera/kitten
.. _`Apache Slider`: https://slider.incubator.apache.org/
.. _`IPython Parallel`: https://ipython.org/ipython-doc/3/parallel/
.. _`Distributed`: http://distributed.readthedocs.org/en/latest/

.. toctree::
   :maxdepth: 1

   install
   quickstart
   usage
   api


