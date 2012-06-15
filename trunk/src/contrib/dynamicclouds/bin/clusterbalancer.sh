#!/bin/sh
HADOOP_HOME=/home/dms/hadoop/hadoop-cluster

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`


DCROOT=`cd $bin/../; pwd`

COMMAND=$1
if [ -z $COMMAND ]; then
	# Print usage
	echo "Usage: clusterbalancer.sh (server (start|stop)|client params)"
fi
HADOOP_CLASSPATH=$DCROOT/dynamicclouds.jar
if [ ! -f $HADOOP_CLASSPATH ]; then
	echo "You have to build the project with ant jar first"
	exit 1;
fi
export HADOOP_CLASSPATH

if [ "$COMMAND" = "server" ]; then
	CLASS=org.apache.hadoop.mapred.DynamicCloudsDaemon
	$HADOOP_HOME/bin/hadoop-daemon.sh $2 $CLASS
fi

if [ "$COMMAND" = "client" ]; then
	CLASS=org.apache.hadoop.mapred.ClusterBalancerTool
	shift
	$HADOOP_HOME/bin/hadoop $CLASS $@
fi
