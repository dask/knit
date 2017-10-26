#!/usr/bin/env bash
source activate test
conda install -c conda-forge -y py4j hdfs3
cd /knit
python setup.py install mvn
py.test -vv