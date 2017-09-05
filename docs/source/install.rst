Installation
============

The runtime requirements of ``knit`` are python, lxml, requests, py4j. Python versions
2.7, 3.5 and 3.6 are currently supported. Dask is required
to launch a Dask cluster. These are all available via conda (py4j on the conda-forge channel).

Easy
~~~~

Use ``pip`` or ``conda`` to install::

   $ conda install knit -c conda-forge
   or
   $ pip install knit --upgrade


Source
~~~~~~

The following steps can be used to install and run ``knit`` from source.

Update and install system dependencies:

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
