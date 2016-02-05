set -e
cat .coverage
pip install coveralls
coveralls
