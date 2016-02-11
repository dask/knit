unamestr=`uname`
if [[ $unamestr == 'Linux' ]]; then
    if [ -z $JAVA_HOME]; then
        # Building with java-jdk from anaconda.org
        export JAVA_HOME=/opt/miniconda/envs/_build/jre
        export JRE_HOME=/opt/miniconda/envs/_build/jre
        export LD_LIBRARY_PATH=/opt/miniconda/envs/_build/jre/lib/amd64/:$LD_LIBRARY_PATH
    fi
elif [[ $unamestr == 'Darwin' ]]; then
   export JAVA_HOME=$(/usr/libexec/java_home)
fi
java -version
$PYTHON setup.py install mvn
