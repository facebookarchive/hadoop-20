#!/bin/bash

source config.sh

# Colors!
# How to use them? See example:
#   echo -e "See the real ${cRED}RED${cRESET} color"

cBLACK='\E[0;30m'
cRED='\E[0;31m'
cGREEN='\E[0;32m'
cYELLOW='\E[0;33m'
cBLUE='\E[0;34m'
cMAGENTA='\E[0;35m'
cCYAN='\E[0;36m'
cWHITE='\E[1;37m'
cRESET='\E[00m'

# just print a message in red color
function fail {
  echo -e "${cRED}$1${cRESET}"
  exit 1
}

# The script patches a template file with sed scripts. All changes
# are made in-place
#
# Usage
# bash patcher.sh <template file> <sed script 1> <sed script 2> ...
function patch {
  if [[ $# < 2 ]]; then
    echo "usage: bash patcher.sh <temlpate file> <sed 1> [<sed 2>...]"
    exit 1
  fi
  # first argument is a template file to patch
  template=$1;
  shift;

  for sedScript in $@; do
    sed -f $sedScript -i $template
  done;
}

function cleanLaunchpad {
  if [[ -e $LAUNCHPAD_DIR ]]; then
    echo "Cleaning $LAUNCHPAD_DIR/ directory.."
    rm -r $LAUNCHPAD_DIR
    mkdir $LAUNCHPAD_DIR
  fi
}

function genAvatarConfigFiles {
  if [[ $# < 1 ]]; then
    echo "Usage: genAvatarConfigFiles <avatar config files>"
    exit 1;
  fi

  cp $TEMPLATES_DIR/avatar-site.xml.template ${HADOOP_VERSION}/conf/avatar-site.xml
  patch ${HADOOP_VERSION}/conf/avatar-site.xml $@

  cp $TEMPLATES_DIR/core-site.xml.template ${HADOOP_VERSION}/conf/core-site.xml
  patch ${HADOOP_VERSION}/conf/core-site.xml $@

  echo "Config files created."
}

function genDatanodeLaunchpadFiles {
  if [[ $# < 1 ]]; then
    echo "Usage: generateDatanodeLaunchpadFiles <datanode config file>"
    exit 1;
  fi

  cp $TEMPLATES_DIR/format-avatardatanode.sh.template $LAUNCHPAD_DIR/dn-format
  patch $LAUNCHPAD_DIR/dn-format $@

  cp $TEMPLATES_DIR/run-datanode.sh $LAUNCHPAD_DIR/run
}
