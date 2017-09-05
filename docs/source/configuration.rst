Configuration
=============

Several methods are available for configuring ``Knit``.

The simplest is to load values from system ``.xml`` files.
Knit will search typical locations and reads default configuration parameters from there.
The file locations may also be specified with the environment variables ``HADOOP_CONF_DIR``,
which is the directory containing the XLM files, ``HADOOP_INSTALL``, in which case the
files are expected in subdirectory ``hadoop/conf/``.

It is also possible to pass parameters when instantiating ``Knit`` or ``DaskYARNCluster``.
You
can either provide individual common overrides (e.g., ``rm='myhost'``) or provide
a whole configuration as a dictionary (``pars={}``) with the same key names as typically
contained in the XML config files. These parameters will take precedence over any loaded
from files, or you can disable using the default configuration at all with ``autodetect=False``.


Connection with hdfs3
---------------------

Some operations, such as checking for uploaded conda environments, optionally make use of
`hdfs3`_. The configuration system, above, and that for hdfs3 are very similar, so you may
well not have to make any extra steps to get this working correctly for you. However, you may
well wish to be more explicit about the configuration of the HDFileSystem instance you want
knit to use. In this case, create the instance as usual, and assign it to the Knit instance
as follows

.. code-block:: python

   hdfs = HDFileSystem(...)
   k = Knit(...)
   k._hdfs = hdfs

or, similarly for a Dask cluster

.. code-block:: python

   cluster = DaskYARNCluster(...)
   cluster.knit._hdfs = hdfs

The special environment variable ``LIBHDFS3_CONF`` will be automatically set when parsing
the config files, if possible. Since the library is only loaded upon the first instantiation
of a HDFileSystem, you still have the option to change its value in ``os.environ``.

.. _hdfs3: http://hdfs3.readthedocs.io/en/latest/