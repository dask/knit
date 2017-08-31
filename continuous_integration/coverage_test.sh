#!/usr/bin/env bash

source activate test
conda install -y py4j lxml requests pytest-cov python-coveralls coveralls -c conda-forge
cd /knit
python setup.py install mvn
py.test --cov=knit knit/tests --cov-report term-missing -s -vv
