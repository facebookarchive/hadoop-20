#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Runs a Hadoop command as a daemon.
#
# Environment Variables
#
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HADOOP_LOG_DIR   Where log files are stored.  PWD by default.
#   HADOOP_MASTER    host:path where hadoop code should be rsync'd from
#   HADOOP_PID_DIR   The pid files are stored. /tmp by default.
#   HADOOP_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   HADOOP_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: hadoop-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] [--instance uniqid] [--namenode_jmx <port>] (start|stop) <hadoop-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

hadoop_rotate_log ()
{
    rlog=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$rlog" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$rlog.$prev" ] && mv "$rlog.$prev" "$rlog.$num"
	    num=$prev
	done
	mv "$rlog" "$rlog.$num";
    fi
}

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# Make sure we start the daemons with the correct username  
if [ -n "${HADOOP_USERNAME}" -a "$(whoami)" != "${HADOOP_USERNAME}" ]; then  
  echo "Must be run as ${HADOOP_USERNAME}. You are $(whoami)" 
  exit 1  
fi  

# get log directory
if [ "$HADOOP_LOG_DIR" = "" ]; then
  export HADOOP_LOG_DIR="$HADOOP_HOME/logs"
fi
mkdir -p "$HADOOP_LOG_DIR"

if [ "$HADOOP_PID_DIR" = "" ]; then
  HADOOP_PID_DIR=/tmp
fi

if [ "$HADOOP_IDENT_STRING" = "" ]; then
  ident_string=$USER
  if [ -n "$HADOOP_INSTANCE" ]; then
    ident_string="${ident_string}-${HADOOP_INSTANCE}"
  fi
  export HADOOP_IDENT_STRING=$ident_string
fi

# some variables
export HADOOP_LOGFILE=hadoop-$HADOOP_IDENT_STRING-$command-$HOSTNAME.log
export HADOOP_ROOT_LOGGER="INFO,DRFA"
jps_cmd="$JAVA_HOME/bin/jps"
log=$HADOOP_LOG_DIR/hadoop-$HADOOP_IDENT_STRING-$command-$HOSTNAME.out
pid=$HADOOP_PID_DIR/hadoop-$HADOOP_IDENT_STRING-$command.pid
gc_log=$HADOOP_LOG_DIR/hadoop-$HADOOP_IDENT_STRING-$command-gc.log

# Set default scheduling priority
if [ "$HADOOP_NICENESS" = "" ]; then
    export HADOOP_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$HADOOP_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        # On Linux, process pids and thread pids are indistinguishable to
        # signals. It's possible that the pid in our pidfile is now a thread
        # owned by another process. Let's check to make sure our pid is
        # actually a running process.
        ps -e -o pid | egrep "^[[:space:]]*`cat $pid`$" >/dev/null 2>&1
        if [ $? -eq 0 ]; then
          # If the pid is from a JVM process of the same type, then we need
          # to abort. If not, then we can clean up the pid file and carry on.
          type_of_pid="$(set -o pipefail; $jps_cmd | awk /^$(cat $pid)/'{print tolower($2)}')"
          if [ $? -ne 0 ]; then
            echo "$jps_cmd failed. Running process `cat $pid` might be"\
                 "a $command process. Please investigate."
            exit 1
          fi
          if [ "$command" == "$type_of_pid" ]; then
            echo $command running as process `cat $pid`.  Stop it first.
            exit 1
          fi
        fi
        rm $pid
      fi
    fi
    
    if [ "$HADOOP_MASTER" != "" ]; then
      echo rsync from $HADOOP_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $HADOOP_MASTER/ "$HADOOP_HOME"
    fi

    hadoop_rotate_log $log
    hadoop_rotate_log $gc_log
    echo starting $command, logging to $log
    cd "$HADOOP_HOME"
    nohup nice -n $HADOOP_NICENESS "$HADOOP_HOME"/bin/hadoop --config $HADOOP_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    if ! kill -0 `cat $pid` > /dev/null 2>&1; then
      echo start failed. Check log file.
      exit 1
    fi
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
        #we found a pidfile, but cant kill -0 the process, nuke the file.
        rm $pid
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


