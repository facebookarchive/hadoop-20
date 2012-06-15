#!/bin/sh
# This is used for starting multiple datanodes on the same machine.
# run it from hadoop-dir/ just like 'bin/hadoop' 

bin=`dirname "$0"`
bin=`cd "$bin" >/dev/null && pwd`

if [ $# -lt 2 ]; then
  S=`basename $0`
  echo "Usage: $S [start|stop] offset(s)"
  echo ""
  echo "    e.g. $S start 1 2"
  exit
fi 

run_datanode () {
  DN=$2
  export HADOOP_IDENT_STRING="$USER-$DN"
  HADOOP_DATANODE_ARGS="\
    -D dfs.datanode.address=0.0.0.0:`expr 50010 + $DN` \
    -D dfs.datanode.http.address=0.0.0.0:`expr 50075 + $DN` \
    -D dfs.datanode.ipc.address=0.0.0.0:`expr 50020 + $DN` \
    -D dfs.data.dir=/tmp/hadoop-$USER/dfs/data$DN"
  "$bin"/hadoop-daemon.sh $1 datanode $HADOOP_DATANODE_ARGS
}

cmd=$1
shift;

for i in $*
do
  run_datanode  $cmd $i
done
