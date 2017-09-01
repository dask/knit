knit
====

Knit enables data scientists to quickly launch, monitor, and destroy distributed programs on a YARN cluster.

Many YARN applications are simple distributed shell commands -- running a shell command on several nodes in a cluster.
Knit enables users to express and deploy applications and software environments managed under YARN through Python.

Knit is part of the `Dask`_ project, committed to bringing cluster-based data science within easy
reach of python developers.

Motivation
----------

Knit was built to support batch-oriented non-JVM applications.  For example, Knit can deploy Python based distributed
applications such as `IPython Parallel`_, with particular support for `Dask`_.
Knit was built with the following motivations in
mind:

*  **PyData Support** Bring the PyData stack into the Hadoop/YARN ecosystem
*  **Easy Setup:** Support a minimal installation effort and the common cases with easy to use Python interface.
*  **Deployable Runtimes:** Build and ship self contained environments along with the application.  Knit uses `conda`_
   to resolve library dependencies and deploy user libraries without IT infrastructure and management


Scope
-----

Knit enables data scientists to quickly launch, monitor, and destroy simple distributed programs.

Knit is not a full featured YARN solution.  Knit focuses on the common case in scientific workloads of starting a
distributed process on many workers for a relatively short period of time.  Knit does  handle some
dynamic container management but it is not suitable for running long-term infrastructural applications.


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
   api
   configuration
   examples