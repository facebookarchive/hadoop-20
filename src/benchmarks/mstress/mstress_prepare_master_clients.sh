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
#
# To run mstress, the participating client hosts and the master host should all
# have the mstress files in the same path.
#
# This script, run with a comma-separated list of hostnames, will copy the
# tar + gz bundle of the mstress directory to the home directory of the hosts
# and untar + unzip them for usage.
#

if [ -z $1 ] || [[ "$1" = -* ]]
then
    echo "Usage: $0 file with comma-separated hosts"
    echo "  This copies the mstress bundle to master and client hosts."
	exit
fi

which tar &>/dev/null
if [ $? -ne 0 ]
then
  echo "tar command not found."
  exit 1
fi

script_dir=$(dirname $(realpath "$0"))

cd $script_dir/.. && tar cvfz mstress.tgz `basename $script_dir`
if [ $? -ne 0 ]
then
  echo "failed to create archive."
  cd -
  exit 1
fi

cd -
for v in `echo $(cat $1) |sed 's/,/ /g'`
do
    scp $script_dir/../mstress.tgz $v:/tmp
	ssh $v "rm -rf /tmp/`basename $script_dir` && cd /tmp && tar xvfz mstress.tgz" &
done

# wait for all the child 'ssh' processes in the above loop to finish
wait
