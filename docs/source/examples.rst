Examples
========

Dask + Distributed
~~~~~~~~~~~~~~~~~~

Install `Dask + Distributed` and start the ``dscheduler``::

    $ conda install distributed -c dask
    $ dscheduler
    distributed.scheduler - INFO - Start Scheduler at:         172.31.54.49:8786
    distributed.scheduler - INFO -            http at:         172.31.54.49:49616

The scheduler now awaits workers to connect to it.  Knit runs
the ``dworker`` command with a specific IP:Port to connect to.

.. code-block:: python

   >>> from knit import Knit
   >>> k = Knit(autodetect=True)
   >>> env = k.create_env(env_name='dev', packages=['dask', 'distributed', 'pandas'])
   >>> cmd = '$PYTHON_BIN $CONDA_PREFIX/bin/dworker 172.31.54.49:8786'

The scheduler will report back the new connection from the workers::

    distributed.core - INFO - Connection from 172.31.54.48:51508 to Scheduler
    distributed.scheduler - INFO - Register ('172.31.54.48', 34959)
    distributed.core - INFO - Connection from 172.31.54.45:46071 to Scheduler
    distributed.scheduler - INFO - Register ('172.31.54.45', 43406)
    distributed.core - INFO - Connection from 172.31.54.47:53764 to Scheduler
    distributed.scheduler - INFO - Register ('172.31.54.47', 44928)

IPython Parallel
~~~~~~~~~~~~~~~~

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


.. _`IPython Parallel`: https://ipython.org/ipython-doc/3/parallel/
.. _`Dask + Distributed`: http://distributed.readthedocs.io/en/latest/
