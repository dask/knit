unamestr=`uname`
if [[ $unamestr == 'Linux' ]]; then
    if [[ -z $JAVA_HOME]]; then
        export JAVA_HOME=/opt/anaconda/bin/java
    fi
elif [[ $unamestr == 'Darwin' ]]; then
   export JAVA_HOME=$(/usr/libexec/java_home)
fi
$PYTHON setup.py install mvn
