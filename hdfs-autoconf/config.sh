#!/bin/bash

# This script is sources by every other script.

# let's stop execution when some simlpe command fails
set -e

# ==================================================
#         CONFIGURE BEFORE USE
# ==================================================

# This argument specifies the hadoop checkout. So the binaries will be run
# from ${HADOOP_VERSION}/bin directory, and configuration files assumed to be
# located in ${HADOOP_VERSION}/conf directory.
# HADOOP_VERSION=
if [[ -z $HADOOP_VERSION ]]; then
  HADOOP_VERSION=$(readlink -f ../)
fi

# This is the directory that will hold all the log files for different
# instances.
# DISCLAIMER: Full path must be specified here!
if [[ -z $LOGS_DIRECTORY ]]; then
  LOGS_DIRECTORY=$HADOOP_VERSION/logs
fi

# ===================================================
# ===================================================


METACONF_DIR="./config-meta"
TEMPLATES_DIR="./config-templates"
LAUNCHPAD_DIR="./launchpad"
# This is the pattern that will be searched for the datanode configuration files
DATANODE_CONFIG_FILES="$METACONF_DIR/avatar-datanode*.sed"
# This is the file that will exist as long as the cluster is running.
# Used by start-dev-cluster and stop-dev-cluster scripts
CLUSTER_IS_RUNNING=$LOGS_DIRECTORY/cluster-is-running-now


if ! [[ -d $METACONF_DIR ]]; then
  echo "Cannot find $METACONF_DIR directory; check config.sh to correct the dir"
  exit 1
fi

if ! [[ -d $TEMPLATES_DIR ]]; then
  echo "Cannot find $TEMPLATES_DIR directory; check config.sh to correct the dir"
  exit 1
fi

if ! [[ -d $LAUNCHPAD_DIR ]]; then
  mkdir -p $LAUNCHPAD_DIR
fi

if [[ -z $ZOOKEEPER_PATH ]]; then
  ZOOKEEPER_PATH="`pwd`/../../../VENDOR.zookeeper/fb-trunk/"
fi
