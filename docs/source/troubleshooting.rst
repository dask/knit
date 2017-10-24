Troubleshooting
===============

Outline
-------

YARN is a complex system. This library contains core python classes for getting
YARN information, creating and managing an Application, and building a Dask
cluster on top of it. Typical usage involves building a .zip-file containing
a full python environment, pushing this to HDFS, getting Yarn to start an Application,
which in turn starts containers that use the environment to bootstrap python processes.

Aside from python code, there is also code in Scala for a Yarn Client
(which runs locally) and, separately, a Yarn ApplicationMaster (which runs in
a special container allocated by Yarn). Communication between python and the two
Scala/JVM processes is via sockets managed by py4j_ and buffered in a background thread
on the python side.

.. _py4j: https://www.py4j.org/

Sources of information
----------------------

Should an application fail or hang, here are a number of places to look first for
thr source of the problem.

console feedback
~~~~~~~~~~~~~~~~

Both the python and the Scala code are fairly verbose to inform the user of the stage
currently occurring. On the python side, you can use standard logging to set the
logging level to DEBUG and get more information. Most exceptions result in a message
giving some possible remedies for the situation.

Application/Container logs
~~~~~~~~~~~~~~~~~~~~~~~~~~

If Yarn started any containers, they will emit logs. The first of these will be the
ApplicationMaster, where you will see debug logging while the application is setting itself
up. Logs also are created by each of the python processes being run in the worker
containers. Any exception in the python processes should be visible in the logs.

Whilst the application is running, you can display logs for containers as
with ``k.print_logs()``, where ``k`` is the Knit instance, or ``cluster.knit.print_logs()``
where ``cluster`` is the DaskYarnCluster instance.

Logs are available for containers sol ong as they are alive. After an application
finishes (including if it is killed), the logs will be stored to HDFS and be accessible
to you with the same commands `if log aggregation is enabled on the cluster`. Typically,
aggregation isn't immediate, so there may be a lag after application end while the logs
are not available. If log aggregation is off, the logs are lost after a container ends,
although they may still be available locally on the machine that hosted the container.

Cluster Information
~~~~~~~~~~~~~~~~~~~

A YARNAPI instance (usually the ``.yarn_api`` attribute of a Knit instance) gives access
to various information about the connected Yarn cluster and pplications registered on it

Some methods of interest:

apps
    Names of all apps known to Yarn
apps_info, app_attempts, app_containers
    Detailed information about the current status of a given app
cluster_info, cluster_metrics
    Global cluster information, including whether the Resource Manager is up and happy,
    and the global resource (memory, cpu) constraints it has to work with
nodes
    Information about the connected Node Managers, whether they are healthy and the
    resources each has available. If unhealthy, there should be a message stating why.

RM/NM Logs
~~~~~~~~~~

The Resource Manager and Node Managers keep logs of their activity in a local directory
on their respective machines. If, for some reason, an application is rejected at time
of submission, or an application is killed without any exception in the application
logs, the reason may well be given in here.

Because these logs are only written to local discs, direct access to the machine is
neede to read them - they may well not be available to you.

If run on the same machine as the Resource Manager and/or Node Manager
(such as the special case of a single-node pseudo-cluster, useful for testing), the YARNAPI
method ``system_logs`` will attempt to find the location of logs, which you can view
with usual system tools such as ``tail``.

Specific issues
---------------

Here follow some specific cases that have caused problems in the past, with
guidance of what might be done.

Don't make a .zip of the conda root
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CondaCreator can zip up any directory, and makes copies of files referenced by
symbolic links. Normally you would use this with a conda environment directory,
usually in the ``/envs/`` directory of a conda installation, or created by Knit
in a local directory especially for this purpose. If you attempt to use the root
environment (i.e., a directory `containing` ``/envs/``), zip will fail with
link recursion, probably after attempting make an enormous file.


Newer java version if .zip > 2GB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is easy to make a conda environment zip with size > 2GB, if including many
libraries. At that threshold, the Zip64 extension is invoked, although file-sizes
up to 4GB are supposed to be possible without it.

If the java version running Yarn is old enough, it will not be able to handle these
larger .zip files. ``java.util.zip.ZipException: invalid CEN header (bad signature)``
will be printed in the ResourceManager logs.
Either reduce the size of the conda environment, or update java on the cluster.

Dask client and workers versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask requires the versions of dask and other auxiliary libraries to match between the
clients and workers, so that functions and data can be deserialised. If you create an
environment using ``packages=``, then this will pull the latest versions from the
repo, unless you specify exact versions. Also note that the channels passed should match
your system settings, as some packages are not compatible between defaults and conda-forge
or other channels.

Once a dask cluster is running, you can see the versions on the workers by starting a
client. In the same session as the DaskYarnCluster you could do:

.. code-block::python

   from dask.distributed import Client
   c = Client(cluster)  # cluster is the running DaskYarnCluster instance
   c.get_versions()

REST routing to YARN
~~~~~~~~~~~~~~~~~~~~

Although application submission and launching are handled via RPCs in scala, several
informational calls are made using the Yarn REST end-points. These must be
reachable.

If you see HTTP connection errors, then there is a possibility that the end-points are
protected by a proxy/gateway such as Knox. You will need to find the appropriate host, port
and path to supply to YARNAPI, such as:

.. code-block::python

   k = knit.Knit(rm='proxy.server.org', rm_port=9999, gateway_path='/default/resourcemanager')

The example would set the API end-point to ``'proxy.server.org:9999/default/resourcemanager/ws/v1/'``,
and whether access is HTTP or HTTPS would depend on the value of ``'yarn.http.policy'``.

There is no way for Knit to be able to automatically determine the right URL to contact,
the information must come from systems operations.

REST auth
~~~~~~~~~

The REST end-points may require Kerberos authentication, which will generally depend on the
value of configuration parameter ``hadoop.http.authentication.type``. The extra package
`request-kerberos`_ is required, but otherwise the connection should be seamless, so long
as a valid ticket exists.

.. _request-kerberos: https://github.com/requests/requests-kerberos

Alternatively, in the case that the authentication is simple, but anonymous access is disallowed,
you must provide a password upon instantiation and perhaps a user-name different from the apparent
user who own the session.

.. code-block::python

   k = knit.Knit(password='mypass', user='alternate_user')


IP of scheduler
~~~~~~~~~~~~~~~

Upon startup, the Dask cluster scheduler guesses its own IP as
``socket.gethostbyname(socket.gethostname())`` - this is the value that workers
will be passed.

On some networks, it is possible that the IP that workers need in order to be able
to contact the scheduler is different from the value that would be guessed. The
parameter ``ip=`` can be passed to set the correct value.

Console language of workers for click
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dask worker is executed as a console application, with the library ``click`` being
used to parse command-line options. ``click`` needs to interpret the character encoding
of the command line it receives, with the result that if the language is not specified,
you will see ``Exit 1`` and an informational statement about setting the language in the
worker logs. A ``lang=`` is provided to set the effective language setting that the
worker processes see. However, there are further constraints on what languages are
permitted, set by the host system - an unwise choice may cause errors like
"LC.ALL=xxx: not a valid identifier", and no python process at all.

System constraints
~~~~~~~~~~~~~~~~~~

Yarn has a large number of system parameters that it matches, and constraints that must be
simultaneously met in order to launch an application. Failures due to obvious unmet
conditions (e.g., asking for more memory than the total available to the cluster) will
probably be flagged before attempting to launch a cluster, if ``checks=True`` in ``.start()``.

However, there are more subtle fail cases. For example, the minimum memory allotment to a
container is rarely less than 1GB, often more, so an application may take much more than
the request passed to Knit suggests.

An even more subtle example: Yarn Node Managers watch disc usage, and if the used fraction
goes above a predetermined threshold (default: 90%), the disc will be labelled "bad", logging
will be prevented, and the entire node will refuse to take jobs until the situation
is rectified.

Build the .jar if running from source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If installing from the repo source, the .jar file needs to be created before/during
installation::

   python setup.py install mvn

which requires ``maven`` to be available, as well as java.

Configuration
~~~~~~~~~~~~~

Knit does its best to find configuration files, but it is always best to check the
contents of the ``.conf`` attribute of a Knit instance (a dictionary) to make sure
that inference was successful, and provide any overrides that might be necessary.
