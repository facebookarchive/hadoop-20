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

usage="Usage: stop-nfs-server.sh"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# Needs to be root user to restart rpcidmapd. Will sudo as hadoop later
if [ $UID -ne 0 ]; then
  echo "Error: must run as root user"
  exit 1
fi

# Does hadoop user exist on system?
if ! /usr/bin/getent passwd hadoop >/dev/null 2>/dev/null; then echo
  echo "Error: hadoop user does not exist on system."
  exit 1
fi

. "$bin"/hadoop-config.sh

sudo -u hadoop "$bin"/hadoop-daemon.sh --config $HADOOP_CONF_DIR stop hdfsnfsproxy
