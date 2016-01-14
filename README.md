
## Build
Builds against cdh 5.5.1 hadoop 2.6.0

```
mvn clean install
```

## Installation

hdfs dfs -mkdir /jars
hdfs dfs -put -f ./rambling-1.0-SNAPSHOT.jar /jars

alias yarn-status='yarn application -status'
alias yarn-log='yarn logs -applicationId'
alias yarn-kill='yarn application -kill'

## Usage

### Python
```
import rambling
r = rambling.Rambling(namenode="ip-XX-XXX-XX", resourcemanager="ip-XX-XXX-XX")
cmd = "python -c 'import sys; print(sys.path); import socket; print(socket.gethostname())'"
appId = r.start_application(cmd)
r.get_application_logs(appId)
```

### JAVA
```
hadoop jar ./rambling-1.0-SNAPSHOT.jar com.continuumio.rambling.Client hdfs://{{NAMENODE}}:9000/jars/rambling-1.0-SNAPSHOT.jar 1 "python -c 'import sys; print(sys.path); import random; print(str(random.random()))' 1 128"
```

Originally forked from: https://github.com/phatak-dev/blog/tree/master/code/YarnScalaHelloWorld

./rambling-1.0-SNAPSHOT.jar com.continuumio.rambling.Client hdfs://localhost:9000/jars/rambling-1.0-SNAPSHOT.jar 1 "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"
