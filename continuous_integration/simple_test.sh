#!/usr/bin/env bash
source activate test
conda install -c conda-forge -y lxml py4j
cd /knit
python setup.py install mvn
py.test -vv -s