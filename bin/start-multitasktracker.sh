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


# Start hadoop map reduce daemons. Run this on the local machine. By default
# logs are written to /tmp/hadoop/

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

# Add contrib jars to classpath. Needed for FairScheduler
for f in "$bin"/../build/contrib/*/*.jar; do
  echo "Adding $f to classpath"
  export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f;
done

export HADOOP_MULTITASKTRACKER_OPTS=" -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8697 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -verbose:gc -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:/usr/local/hadoop/logs/MRSIM/multitasktracker.gc.log -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC"
# start mapred daemons
# start jobtracker first to minimize connection errors at startup
"$bin"/hadoop-daemon.sh --config $HADOOP_CONF_DIR start multitasktracker
