knit
========

``knit`` is an easy interface for launching distributed applications with `YARN`_.  Additionally, it provides many
conveniences for Python and other runtime languages.  Many YARN applications are nothing more than distributed shell
commands -- the `Hello World`_ for YARN is running a unix command on several nodes in a cluster.  ``knit`` implements
a generalized distrubted shell command and allows users to express commands/applications, as well as desired resources managed
under YARN through Python or directly on the command line.

.. code-block:: python

   from knit import Knit
   k = Knit(autodetect=True) # autodetect IP/Ports for YARN/HADOOP

   app_id = k.start('date', num_containers=10, memory=4096)
   status = k.status(app_id)
   logs = k.logs(app_id)

See :doc:`the quickstart <quickstart>` to get started.


.. _YARN: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
.. _Hello World: http://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html
.. toctree::
   :maxdepth: 1

   install
   usage
   api


