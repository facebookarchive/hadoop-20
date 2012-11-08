#!/bin/bash

# Use this script to start local zookeeper service. This is necessary for
# the local developer cluster to work.
#
# Usage: bash zookeeper.sh [start|stop]
#

set -e
usage="USAGE
bash $(basename $0) [--help] (start|stop)

OPTIONS
  --help - show this help message
  start - starts zookeeper
  stop - stops zookeeper
"

if [[ ${PWD##*/} != "hdfs-autoconf" ]]; then
  echo "The script should be launched from ./hdfs-autoconf directory. Exiting.."
  exit 1
fi

if (( $# >= 1 )); then
  if [[ $1 == "--help" ]]; then
    echo $usage;
    exit 0;
  fi
fi

if (( $# > 0 )); then
  if [[ "$1" == "start" ]]; then
    command="start";
    shift;
  elif [[ "$1" == "stop" ]]; then
    command="stop";
    shift;
  fi
fi

if (( $# > 0 )); then
  echo "$usage"
  exit 1;
fi

source config.sh

export ZOO_LOG_DIR=$LOGS_DIRECTORY
if ! [[ -e $ZOO_LOG_DIR ]]; then
  mkdir -p $ZOO_LOG_DIR;
fi
$ZOOKEEPER_PATH/bin/zkServer.sh $command $PWD/$TEMPLATES_DIR/zoo.cfg

