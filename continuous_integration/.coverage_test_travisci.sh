set -e

if [[ $TRAVIS_PYTHON_VERSION == '3.5' ]]
then
    docker exec -it $CONTAINER_ID /knit/continuous_integration/coverage_report.sh
fi
