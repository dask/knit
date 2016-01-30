knit
========

The ``knit`` library provides a Python interface to Scala for interacting
with the YARN resource manager.

Example
~~~~~~~

.. code-block:: python

   from knit import Knit
   k = Knit()  # use local defaults for namenode address

   app_id = k.start_application('sleep 100', num_containers=10, memory=1024)
   logs = k.get_application_logs(app_id)
   k.kill_application(app_id)

Refer to the following documentation to get started with `knit`:

.. toctree::
   :maxdepth: 1

   install
   usage
   api
