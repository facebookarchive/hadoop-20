#!/bin/bash
set -e
usage="USAGE
  bash $(basename $0) [--help]

DESCRIPTION
  Stops all the avatar instances that are currently running on the local
  machine.

OPTIONS
  --help - show this help message
"

if [[ ${PWD##*/} != "hdfs-autoconf" ]]; then
  echo "The script should be launched from ./hdfs-autoconf directory. Exiting.."
  exit 1
fi

if (( $# >= 1 )); then
  if [[ $1 == "--help" ]]; then
    echo "$usage";
    exit 0;
  fi
fi

if (( $# > 0 )); then
  echo "$usage";
  exit 1;
fi

source scripts/common.sh

for i in $(ps aux | grep avatar | grep java | tr -s ' ' | cut -d' ' -f2); do
  kill -9 $i
done

if [[ -e $CLUSTER_IS_RUNNING ]]; then
  rm $CLUSTER_IS_RUNNING;
fi

