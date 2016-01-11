#!/bin/bash

echo "Starting HDFS"

# Start HDFS
sudo service hadoop-hdfs-namenode start
sudo service hadoop-hdfs-datanode start

echo "Starting YARN"

# Start YARN
sudo service hadoop-yarn-resourcemanager start
sudo service hadoop-yarn-proxyserver start
sudo service hadoop-yarn-nodemanager start

echo "Ready"

# Block
sleep infinity
