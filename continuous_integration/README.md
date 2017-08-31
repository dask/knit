# Continuous Integration

How to test knit on your system locally. Note that YARN within a docker container still
requires substantial resources. Failures may be related to depleting these, especially if
running the whole test-suite in one command. If running on OSX, docker lives within a VM
with limited memory and CPUs, so we recommend running py.test on each of the knit/tests/test_*
files one at a time.

Requirements:
-  `docker`

## Run within a container

This is the same setup as is used in Travis CI. The command also runs code coverage;
feel free to edit the test script to taste.

Run this in the topmost knit directory, or replace $PWD, below, with the appropriate
location. 

> docker run -d -name knit_test -v $PWD:/knit mdurant/hadoop
> export CONTAINER_ID=`docker ps | grep knit_test | awk '{print $1}'`
> sleep 20

> docker exec -it $CONTAINER_ID conda create -y -n test python=$TRAVIS_PYTHON_VERSION pytest dask distributed

> docker exec -it $CONTAINER_ID bash /knit/continuous_integration/coverage_test.sh

## Run outside container

Install knit in your system.

Either set up your own YARN system, or start a YARN using the container above, e.g.,

> docker run -p 8020:8020 -p 8088:8088 mdurant/hadoop

> py.test knit/tests -vv

## Note on miniconda

Without further configuration, knit downloads miniconda and any environments into the code
directory. This will cause problems if the same code directory is executed from both within
a container and from without. If "file not found" errors appear referencing conda, you
should delete the contents of knit/knit/tmp_conda.


