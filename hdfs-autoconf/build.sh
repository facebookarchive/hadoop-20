#!/bin/bash
set -e

usage="USAGE
  $(basename $0) [--help] [--fast]

DESCRIPTION
  Builds HDFS from sources.

OPTIONS
  --help - shows this help
  --fast - EXPERIMENTAL option, does some build 3 times faster than default
      build.
"

if (( $# >= 1 )); then
  if [[ $1 == "--help" ]]; then
    echo "$usage";
    exit 0;
  fi
fi

compile="full"
if (( $# >= 1 )); then
  if [[ $1 == "--fast" ]]; then
    compile="fast"
    shift
  fi
fi

if (( $# > 0 )); then
  echo "$usage"
  exit 1
fi

source config.sh

cd ${HADOOP_VERSION};

if [[ $compile == "full" ]]; then
  ant clean compile
elif [[ $compile == "fast" ]]; then
  ant clean compile-core
  cd src/contrib/highavailability
  ant clean compile
fi


