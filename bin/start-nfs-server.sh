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

usage="Usage: start-nfs-server.sh"

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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

# get arguments
if [ $# -ne 1 ]; then
    echo $usage
fi

port=$1

# hdfs-nfs-proxy is using NFSv4 Client
# To have the correct user/group information
# hdfs-nfs-proxy needs the /etc/init.d/rpcidmapd service
# And, /etc/idmapd.conf needs to be configured as:

#[General]
#Domain = localdomain

#[Translation]
#Method = nsswitch

domain=`awk -F'= *' '/^Domain / {print $2}' /etc/idmapd.conf`
method=`awk -F'= *' '/^Method / {print $2}' /etc/idmapd.conf`

if [ $domain != "localdomain" ]; then
  echo "/etc/idmapd.conf needs to config Domain as localdomain. It is: $localdomain, could not start hdfs-nfs-proxy."
  exit 1
elif [ $method != "nsswitch" ]; then
  echo "/etc/idmapd.conf needs to config Method as nsswitch. It is: $method, could not start hdfs-nfs-proxy."
  exit 1
fi

/etc/init.d/rpcidmapd restart >/dev/null

sudo -u hadoop "$bin"/hadoop-daemon.sh --config $HADOOP_CONF_DIR start hdfsnfsproxy $port
