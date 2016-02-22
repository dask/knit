ANACONDADIR=${1:-"/opt/anaconda"}

$ANACONDADIR/bin/pip install pytest-cov
$ANACONDADIR/bin/py.test --cov=knit knit/tests --cov-report term-missing -s -vv