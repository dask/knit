Usage
=====


Python
~~~~~~

.. code-block:: python

   >>> import knit
   >>> k = knit.Knit()
   >>> cmd = "python -c 'import sys; print(sys.path); import socket; print(socket.gethostname())'"
   >>> appId = k.start(cmd)
   >>> k.logs(appId)


Shipping Conda
~~~~~~~~~~~~~~

Ship runtime environment with latest ``distributed`` and PyData stack.

.. code-block:: python

   >>> import knit
   >>> k = knit.Knit()
   >>> cmd = '$PYTHON_BIN $CONDA_PREFIX/bin/dworker 8787'
   >>> env_zip = k.create_env(env_name='dev', packages=['python=3', 'distributed', 'dask', 'pandas', 'scikit-learn'])
   >>> appId = k.start(cmd, env=env_zip)
   >>> k.logs(appId)


Java
~~~~

.. code-block:: bash

   $ hadoop jar ./knit-1.0-SNAPSHOT.jar io.continuum.knit.Client --help
   $ hadoop jar ./knit-1.0-SNAPSHOT.jar io.continuum.knit.Client --numInstances 1 --command "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"

.. code-block:: bash

   $ ./knit-1.0-SNAPSHOT.jar io.continuum.knit.Client hdfs://localhost:9000/jars/knit-1.0-SNAPSHOT.jar 1 "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"


Helpful aliases
---------------

.. code-block:: bash

   $ alias yarn-status='yarn application -status'
   $ alias yarn-log='yarn logs -applicationId'
   $ alias yarn-kill='yarn application -kill'
