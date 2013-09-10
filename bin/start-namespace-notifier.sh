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

usage="Usage: start-namespace-notifier.sh [-service service_id]"

params=$#
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh
. "$bin"/../conf/hadoop-env.sh

# get arguments
if [ $# -ge 1 ]; then
    startArg=$1
    case $startArg in 
      (-service)
        if [ $# -ge 2 ]; then
          service_id=$2
        else 
          echo $usage
          exit 1
        fi
        ;;
      (*)
        echo $usage
        exit 1
        ;;
    esac
fi

export NOTIFIER_JMX_OPTS=" -Dcom.sun.management.jmxremote.port=$NOTIFIER_JMX_PORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

# use the thrift 0.9.0 jar in the class path
export HADOOP_CLASSPATH=${HADOOP_HOME}/contrib/namespace-notifier/lib/libthrift-0.9.0.jar:${HADOOP_CLASSPATH}

"$bin"/hadoop-daemon.sh --config $HADOOP_CONF_DIR start notifier $startArg $service_id
