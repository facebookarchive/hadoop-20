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


# Stop hadoop tasktracker and its children

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh
if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

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
pid=$HADOOP_PID_DIR/hadoop-$HADOOP_IDENT_STRING-tasktracker.pid
# The children of a task tracker are map/reduce tasks. The process tree under
# each of those tasks is a single process group. Kill all these process groups
if [ -f $pid ]; then
  pidvalue=$(cat $pid)
  for i in `ps -o pid --no-headers --ppid $pidvalue`; do
    echo "Killing process group $i"
    kill -s 9 -- -$i;
  done
fi

# Now kill the tasktracker itself.
"$bin"/hadoop-daemon.sh --config $HADOOP_CONF_DIR stop tasktracker
