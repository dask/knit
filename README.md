
## Build
Builds against cdh 5.5.1 hadoop 2.6.0

```
mvn clean install
```

## Installation

hdfs dfs -mkdir /jars
hdfs dfs -put -f ./rambling-1.0-SNAPSHOT.jar /jars

## Execute
```
hadoop jar ./rambling-1.0-SNAPSHOT.jar com.continuumio.rambling.Client 1 hdfs://{{NAMENODE}}:9000/jars/rambling-1.0-SNAPSHOT.jar 1 "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"
```

Originally forked from: https://github.com/phatak-dev/blog/tree/master/code/YarnScalaHelloWorld