## OSX


export HADOOP_HOME=/usr/local/Cellar/hadoop/2.6.0/
export HADOOP_CONF_DIR=$HADOOP_HOME/libexec/etc/hadoop

Copy ./etc/hadoop to $HADOOP_HOME/libexec/


*If you want to avoid annoying password prompts*:
```
$ ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
$ cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
```

## Configure and Start Services

```
hdfs namenode -format
start-dfs.sh
hdfs dfs -mkdir -p /user/`whoami`
# confirm directory is created
hdfs dfs -ls /user/
start-yarn.sh
```

## Confirm Services are running
curl http://localhost:50070
curl http://localhost:8088

