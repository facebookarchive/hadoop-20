#!/bin/sh

CLASSPATH=
TOP=../../../..

# the hadoop libraries
for f in $TOP/build/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# the apache libraries
for f in $TOP/lib/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# the thrift libraries
for f in $TOP/lib/thrift/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# the thrift server
for f in $TOP/build/contrib/thriftdatanode/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done
# the thrift hadoop api
for f in $TOP/src/contrib/thriftdatanode/lib/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

echo CLASSPATH=$CLASSPATH

java -Dcom.sun.management.jmxremote -cp $CLASSPATH org.apache.hadoop.thriftdatanode.HadoopThriftDatanodeServer $*

