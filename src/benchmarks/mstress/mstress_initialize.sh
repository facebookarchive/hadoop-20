#!/bin/bash

# $Id$
#
# Author: Thilee Subramaniam
#
# Copyright 2012 Quantcast Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
#
# The mstress runs stress tests on QFS metaserver and HDFS namenode. To be able
# to work with the namenode, the java HDFS client needs to be compiled and run
# with hadoop hdfs jars.
#
# To make this process uniform across mstress master and slaves, we copy the
# jars to a local directory and copy it around to all participating hosts at
# a fixed location.
# (Without this, each client would need to add entries to /etc/yum.repos.d/,
#  and require root access to install hadoop client rpms).
#
# This script packages the jars locally on the build node, so that the
# 'mstress_prepare_clients.sh' script can copy it over to master and clients.
#
# Run this program with the path of hadoop client jars as argument (default
# /usr/lib/hadoop/client/), and it will create a "mstress_hdfs_client_jars"
# directory containing the jars.
#

if [[ "$1" = -* ]]
then
	echo "Usage: $0 [ path/to/hadoop/client/jars ]"
  echo "  This prepares the build environment on mstress build host"
  echo "  Default path is '/usr/lib/hadoop/client/'"
	exit
fi

JARS_SOURCE=${1:-"/usr/lib/hadoop/client/"}

DIR="$( cd "$( dirname "$0" )" && pwd )"
JARS_TARGET=${DIR}/mstress_hdfs_client_jars

if [ ! -d "$JARS_SOURCE" ] || [ -z "$(ls -A "$JARS_SOURCE"/*.jar)" ]; then
  echo ""$JARS_SOURCE" is not a directory or does not have the jars."
  exit 1
fi

if [ -d "$JARS_TARGET" ]; then
  if [ "$(ls -A "$JARS_TARGET"/*.jar 2> /dev/null)" ]; then
    echo ""$JARS_TARGET" already has the jars. Nothing to do."
    exit 0
  fi
fi

mkdir -p "$JARS_TARGET"
cp $JARS_SOURCE/*.jar "$JARS_TARGET"

if [ $? -ne 0 ]
then
  echo "Failed to copy jars."
  exit 1
fi

echo "Hadoop client jars from $JARS_SOURCE copied to $JARS_TARGET."

exit 0

