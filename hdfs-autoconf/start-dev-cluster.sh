#!/bin/bash
set -e

usage="USAGE
  bash $(basename $0) [--help] [--format] [--count number | --seekconfig]

DESCRIPTION
  Launching without arguments equals to '--seekconfig'
  specified argument

  Starts locally two avatar namenodes and, depending on the option
  given, some amount of datanodes. All the instances are launched
  as daemons, logs could be tailed from \$LOGS_DIRECTORY.

OPTIONS
  --help - show this help message
  --format - formats both namenode and datanode directories for the
      cluster
  --count - if this parameter is given, the script runs the specified
      number of datanodes simultaniously. Each datanode's configuration file is
      generated automatically with the template file
      '\$METACONF_DIR/avatar-datanode.template' by substituting
      every entrance of XXX sequence with the datanode's sequential number.
      After that a datanode instance is run via 'avatar-datanode-start --conf
      <generated_config> --daemon' command.
  --seekconfig - this option might be used if someone needs
      more control on cluster structure. With this option given,
      the script looks up for all avatar datanode configuration
      files and launches a single datanode per every config file.
      (the avatar datanode configuration file is the one which name
      satisfies to pattern \$DATANODE_CONFIG_FILES)
"

if (( $# >= 1 )); then
  if [[ $1 == "--help" ]]; then
    echo "$usage"
    exit 0;
  fi
fi

format=""
if (( $# >= 1 )); then
  if [[ $1 == "--format" ]]; then
    format="--format";
    shift;
  fi
fi

mode="seek";

if (( $# >= 1 )); then
  if [[ $1 == "--seekconfig" ]]; then
    shift;
  elif [[ $1 == "--count" ]]; then
    shift;
    mode=$1;
    shift;
  fi
fi

if (( $# > 0 )); then
  echo "$usage";
  exit 1;
fi


if [[ ${PWD##*/} != "hdfs-autoconf" ]]; then
  echo "The script should be launched from ./hdfs-autoconf directory. Exiting.."
  exit 1
fi

source scripts/common.sh

if [[ -e $CLUSTER_IS_RUNNING ]]; then
  fail "The developer cluster is already running!"
fi

if [[ "$format" == "--format" ]]; then
  ./avatar-format
fi

touch $CLUSTER_IS_RUNNING;
./avatar-zero-start --daemon
./avatar-one-start --daemon

if [[ $mode == "seek" ]]; then
  num=0
  for i in $(ls -1 $DATANODE_CONFIG_FILES); do
    num=$((num+1))
    export HADOOP_LOG_DIR=${LOGS_DIRECTORY}/datanode-$num-logs
    ./avatar-datanode-start $format --conf $i --daemon
  done
else
  while (( $mode > 0 )); do
    tmp=$(mktemp -t avatar-datanode-autoconfig-$mode.XXX)
    args="$format --conf $tmp --daemon"
    cp ${METACONF_DIR}/avatar-datanode.template $tmp
    sed -i -e "s/XXX/$mode/g" $tmp
    ./avatar-datanode-start $args
    rm -f $tmp
    mode=$((mode-1));
  done
fi

