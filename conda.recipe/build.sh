unamestr=`uname`
if [[ $unamestr == 'Linux' ]]; then
    if [ -z $JAVA_HOME]; then
        # Building with java-jdk on anaconda.org
        export JAVA_HOME=/opt/anaconda/envs/_build/jre
        export LD_LIBRARY_PATH=/opt/anaconda/envs/_build/jre/lib/amd64/:$LD_LIBRARY_PATH
    fi
elif [[ $unamestr == 'Darwin' ]]; then
   export JAVA_HOME=$(/usr/libexec/java_home)
fi
$PYTHON setup.py install mvn
