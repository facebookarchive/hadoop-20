#!/bin/bash

#
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
# This is a single-button command of the benchmarking process. You just give
# it the metaserver (perhaps run by example/sampleservers/sample_setup.py) and
# the namenode (see README for how to install and run namenode), and this will 
# package the jars, compile the sources, setup the clients on localhost, and
# run the tests.
#

SERVER_ARGS=""

function usage() {
  echo "Usage: $0 [-q host:port] [-h host:port]
      -q: QFS metaserver host and port.
      -h: HDFS namenode host and port.
  
  This benchmark, run on CentOS systems, tests the QFS metaserver against
  HDFS namenode. Please refer to the README file in this directory if you run
  into any setup, build, or run issue while using the mstress benchmark." >&2
}

while getopts 'q:h:' OPTION
  do
    case $OPTION in
    q)
      QFS_HOST=$(echo "$OPTARG" | cut -d ":" -f 1)
      QFS_PORT=$(echo "$OPTARG" | cut -d ":" -f 2)
      SERVER_ARGS=$SERVER_ARGS" qfs,"$QFS_HOST","$QFS_PORT""
      ;;
    h)
      HDFS_HOST=$(echo "$OPTARG" | cut -d ":" -f 1)
      HDFS_PORT=$(echo "$OPTARG" | cut -d ":" -f 2)
      SERVER_ARGS=$SERVER_ARGS" hdfs,"$HDFS_HOST","$HDFS_PORT""
      ;;
    ?)
      usage
      exit 1
      ;;
    esac
done

shift $(($OPTIND - 1))
if [ $# -ne 0 ]; then
  usage
  exit 2
fi

[ -z "$SERVER_ARGS" ] && usage && exit 0

[ -z "$JAVA_HOME" ] && echo "Need JAVA_HOME to be set." && exit 1

mstress_dir="$(cd `dirname "$0"` && pwd)"
cd "$mstress_dir"
baseDir="$mstress_dir/../.."

make_flags=""
if [ -d "$baseDir/include" ]; then
  echo "Running from tarball."
  makeFlags="KFS_BUILD_INCLUDE=$baseDir/include KFS_BUILD_STATLIB=$baseDir/lib/static"
else
  echo "Running from source tree."
fi

make_args="${makeFlags} ccclient"
if grep -q hdfs <<<"$SERVER_ARGS"; then
  make_args="${makeFlags} all"
  ./mstress_initialize.sh
  [ $? -ne 0 ] && echo "Failed to prepare hdfs client jars. Please verify hadoop hdfs namenode is installed." && exit 1
fi

make $make_args
[ $? -ne 0 ] && echo "Failed to compile mstress clients." && exit 1

./mstress_prepare_master_clients.sh localhost
[ $? -ne 0 ] && echo "Failed to prepare mstress on localhost." && exit 1

pushd ~/mstress

echo "Running ./mstress_run.py localhost ${SERVER_ARGS} .."
./mstress_run.py localhost ${SERVER_ARGS}
ret=$?
popd

[ $ret -ne 0 ] && echo "Failed to run benchmark on localhost." && exit 1

exit 0
