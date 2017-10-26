#!/usr/bin/env bash
source activate test
conda install -c conda-forge -y py4j hdfs3
cd /knit
python setup.py install mvn
if [ "$TEST_DASK" = "true"]; then
    py.test -vv knit
else
    py.test -vv dask_yarn
fi
