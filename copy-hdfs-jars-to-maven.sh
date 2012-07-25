#!/bin/sh
#
# Use this script to publish this jars to your local maven repo (~/.m2).
# This is required for HBase to be able to pick up the HDFS jars (core
# and test) built in titan/VENDOR/hadoop-0.20/.
#

set -e -u -o pipefail
BASEDIR=`dirname $0`
cd ${BASEDIR}

VERSION=$( ant -q print-version | head -1 | awk '{print $2}' )
if [ -z "$VERSION" ]; then
  echo "Unable to determine Hadoop version" >&2
  exit 1
fi

TARGETS=""

CORE_JAR=build/hadoop-$VERSION-core.jar
if [ ! -f $CORE_JAR ]; then
  TARGETS="$TARGETS jar"
fi

CORE_POM=build/ivy/maven/generated.pom
if [ ! -f $CORE_POM ]; then
  TARGETS="$TARGETS makepom"
fi

TEST_JAR=build/hadoop-$VERSION-test.jar
if [ ! -f $TEST_JAR ]; then
  TARGETS="$TARGETS jar-test"
fi

if [ -n "$TARGETS" ]; then
  ant $TARGETS
fi

# Clear the optional flag on Hadoop dependencies so these dependencies can be
# included transitively in other projects.
CORE_POM_MODIFIED=$CORE_POM.new
./edit_generated_pom.py >$CORE_POM_MODIFIED

echo "** Publishing hadoop* core & test jars "
echo "** to "
echo "**   your local maven repo (~/.m2/repository). "
echo "** HBase builds will  pick up the HDFS* jars from the local maven repo."

# When running under Commander, use the setting.xml file that specifies
# the localRepository for a central mvn repo that can be shared between
# all of the build/test agents
OPTS=""
if [[ -n "${COMMANDER_WORKSPACE:-}" || "$USER" == "svcscm" ]]; then
    OPTS="-s /scm/git/electric/hadoop_builds/settings.xml"
fi

mvn $OPTS install:install-file \
    -DpomFile=$CORE_POM_MODIFIED \
    -DgroupId=org.apache.hadoop \
    -DartifactId=hadoop-core \
    -Dversion=$VERSION \
    -Dpackaging=jar \
    -Dfile=${CORE_JAR}

mvn $OPTS install:install-file \
    -DgeneratePom=true \
    -DgroupId=org.apache.hadoop \
    -DartifactId=hadoop-test \
    -Dversion=$VERSION \
    -Dpackaging=jar \
    -Dfile=${TEST_JAR}

