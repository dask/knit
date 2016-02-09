Installation
============

Easy
~~~~

Use ``pip`` or ``conda`` to install::

   $ conda install knit -c dask
   or
   $ pip install knit --upgrade


Source
~~~~~~

The following steps can be used to install and run ``knit`` from source.
These instructions were tested on Ubuntu 14.04, CDH 5.5.1, and Hadoop 2.6.0.

Update and install system dependencies:

.. code-block:: bash

   $ sudo apt-get update
   $ sudo apt-get install git maven openjdk-7-jdk -y

Clone git repository and build maven project:

.. code-block:: bash

   $ git clone https://github.com/dask/knit
   $ cd knit
   $ python setup.py install mvn
