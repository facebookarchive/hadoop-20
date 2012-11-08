#!/bin/bash
BASE_DIR=$(dirname ${BASH_SOURCE[0]})
BASE_DIR=`cd "$BASE_DIR"; pwd`
if ([ "$1" != "corona" ] && [ "$1" != "mapred" ]); then
    echo "Usage: source singleNodeSwitch.sh <corona|mapred>"
else
    echo "Before issuing any commands, you should have built hadoop with 'ant bin-package'."
    echo "If this is run for the first time, you should execute 'hadoop namenode -format'."
    echo "Then, you can start HDFS with 'start-dfs'."
    echo "The HDFS web ui is http://localhost:50070"
    export HADOOP_HOME="$BASE_DIR/../" 
    export HADOOP_LOG_DIR="$HADOOP_HOME/logs"
    export HADOOP_PID_DIR="${HADOOP_LOG_DIR}" 
    if [ "$1" == "corona" ]; then
	export HADOOP_CONF_DIR="$BASE_DIR/coronaConf"
	export HADOOP_CLASSPATH="${HADOOP_HOME}/src/contrib/corona/lib/libthrift-0.7.0.jar"
	alias start-corona="${HADOOP_HOME}/bin/start-corona.sh"
	alias stop-corona="${HADOOP_HOME}/bin/stop-corona.sh"
	alias new-corona="cp -r $CORONA_WEBAPPS_DIR $BUILD_WEBAPPS_DIR; stop-corona; stop-dfs; hadoop namenode -format; start-dfs; start-corona"
	CORONA_WEBAPPS_DIR="${HADOOP_HOME}/build/contrib/corona/webapps/*"
	BUILD_WEBAPPS_DIR="${HADOOP_HOME}/build/webapps/"
	echo "Copying corona files from $CORONA_WEBAPPS_DIR to $BUILD_WEBAPPS_DIR"
	echo "cp -r $CORONA_WEBAPPS_DIR $BUILD_WEBAPPS_DIR"
	cp -r $CORONA_WEBAPPS_DIR $BUILD_WEBAPPS_DIR
	echo "Available corona specific aliases: start-corona, stop-corona"
	echo "To start the corona, type 'start-corona'"
	echo "The corona web ui is http://localhost:50032"
    else
	export HADOOP_CONF_DIR="$BASE_DIR/mapredConf"
	alias start-mapred-single="${HADOOP_HOME}/bin/start-mapred-single.sh"
	alias stop-mapred-single="${HADOOP_HOME}/bin/stop-mapred-single.sh"
	echo "Available mapred specific aliases: start-mapred-single, stop-mapred-single"
	echo "You might need to change the HDFS permissions before starting the job tracker (i.e. 'hadoop fs -chmod -R 777 /'"
	echo "To start the jobtracker type 'start-mapred-single'"
	echo "The jobtracker web ui is http://localhost:50030"
    fi
    alias hadoop="${HADOOP_HOME}/bin/hadoop"
    alias hadoop_streaming="${HADOOP_HOME}/bin/hadoop_streaming"
    alias start-dfs="${HADOOP_HOME}/bin/start-dfs.sh"
    alias stop-dfs="${HADOOP_HOME}/bin/stop-dfs.sh"
fi
