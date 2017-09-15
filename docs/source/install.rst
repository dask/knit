Installation
============

The runtime requirements of ``knit`` are python, lxml, requests, py4j. Python versions
2.7, 3.5 and 3.6 are currently supported. Dask is required
to launch a Dask cluster. These are all available via conda (py4j on the conda-forge channel).

Testing depends on ``pytest``.

Easy
~~~~

Use ``pip`` or ``conda`` to install::

   $ conda install knit -c conda-forge
   or
   $ pip install knit --upgrade


For dask clusters, you also need dask itself::

   $ conda install dask distributed

Source
~~~~~~

The following steps can be used to install and run ``knit`` from source.

Update and install system dependencies (e.g., for debian systems):

.. code-block:: bash

   $ sudo apt-get update
   $ sudo apt-get install git maven openjdk-7-jdk -y

or install these via conda

.. code-block:: bash

   $ conda install -y -c conda-forge setuptools maven openjdk

Clone git repository and build maven project:

.. code-block:: bash

   $ git clone https://github.com/dask/knit
   $ cd knit
   $ python setup.py install mvn

Testing on Docker
~~~~~~~~~~~~~~~~~

If you would like to test this package, but don't have a YARN cluster hanging around, you
could make a small test one in your machine. This is essentially how the Continuous Integration tests
work.

   $ export CONTAINER_ID=`docker run -d mdurant/hadoop`
   $ docker exec -it $CONTAINER_ID bash
   # conda install dask distributed -y
   # conda install -c conda-forge lxml py4j knit
   # py.test -vv knit