#!/bin/sh
#
# Use this script to publish this jars to your local maven repo (~/.m2).
# This is required for HBase to be able to pick up the HDFS jars (core
# and test) built in titan/VENDOR/hadoop-0.20/.
#

BASEDIR=`dirname $0`
cd ${BASEDIR}

if [ ! -f build/hadoop-0.20.1-dev-core.jar ]; then
 if [ ! -f build/hadoop-0.20.1-core.jar ]; then
  echo "core jar not found. Running 'ant jar'..."
  ant jar | grep BUILD;
 fi
fi

if [ ! -f build/hadoop-0.20.1-dev-test.jar ]; then
 if [ ! -f build/hadoop-0.20.1-test.jar ]; then
   echo "test jar not found. Running 'ant jar-test'..."
   ant jar-test | grep BUILD;
 fi
fi


#
# The names of core/test jar name depend
# on whether they were generated using 
# build_all.sh script or just the vanilla
# simple ant jar/jar-test
#
if [ -f build/hadoop-0.20.1-dev-core.jar ]; then
  CORE_JAR=build/hadoop-0.20.1-dev-core.jar
else
  CORE_JAR=build/hadoop-0.20.1-core.jar
fi

if [ -f build/hadoop-0.20.1-dev-test.jar ]; then
  TEST_JAR=build/hadoop-0.20.1-dev-test.jar
else
  TEST_JAR=build/hadoop-0.20-test.jar
fi

echo "** Publishing hadoop* core & test jars "
echo "** to "
echo "**   your local maven repo (~/.m2/repository). "
echo "** HBase builds will  pick up the HDFS* jars from the local maven repo."

mvn install:install-file \
    -DgeneratePom=true \
    -DgroupId=com.facebook.hadoop \
    -DartifactId=hadoop-core \
    -Dversion=0.20.1 \
    -Dpackaging=jar \
    -Dfile=${CORE_JAR}

mvn install:install-file \
    -DgeneratePom=true \
    -DgroupId=com.facebook.hadoop \
    -DartifactId=hadoop-test \
    -Dversion=0.20.1 \
    -Dpackaging=jar \
    -Dfile=${TEST_JAR}
