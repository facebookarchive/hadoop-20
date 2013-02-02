if [[ "$IS_HADOOP_ENV_ALREADY_SOURCED" != "true" ]]; then
  export IS_HADOOP_ENV_ALREADY_SOURCED="true"
  # Set Hadoop-specific environment variables here.

  # The only required environment variable is JAVA_HOME.  All others are
  # optional.  When running a distributed configuration it is best to
  # set JAVA_HOME in this file, so that it is correctly defined on
  # remote nodes.

  # The java implementation to use.  Required.
  # export JAVA_HOME=/usr/lib/j2sdk1.5-sun

  # Extra Java CLASSPATH elements.  Optional.
  #export HADOOP_CLASSPATH=${HADOOP_TRUNK_MAIN}/VENDOR/hadoop-0.20/lib/

  # use ipv4 if we can:
  export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true"

  # The maximum amount of heap to use, in MB. Default is 1000.
  export HADOOP_HEAPSIZE=2000

  # Extra Java runtime options.  Empty by default.
  # export HADOOP_OPTS=-server

  # Command specific options appended to HADOOP_OPTS when specified
  export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
  export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
  export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
  export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"
  export HADOOP_RAIDNODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_RAIDNODE_OPTS"
  export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote -Xmx3g -Xms3g $HADOOP_NAMENODE_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=9070"
  #export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote.port=8998 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

  # The only user who can start hadoop daemons.
  # If this is not set, any user can start hadoop daemons.
  #export HADOOP_USERNAME="hadoop"

  # Java Runtime garbage collection options to pass to all Hadoop
  # servers (Namenode, Jobtracker, Datanode, Tasktracker). This must end
  # with a colon ; to which the dynamically generated gc log filename will
  # be appended to. The below defaults work for the Sun JVM, for example
  # in IBM GC, use '-Xverbosegclog:'.
  #export HADOOP_GC_LOG_OPTS="-XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:"

  # export HADOOP_TASKTRACKER_OPTS=
  # The following applies to multiple commands (fs, dfs, fsck, distcp etc)
  # export HADOOP_CLIENT_OPTS

  # Extra ssh options.  Empty by default.
  # export HADOOP_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HADOOP_CONF_DIR"

  # Where log files are stored.  $HADOOP_HOME/logs by default.
  # export HADOOP_LOG_DIR=${HADOOP_HOME}/logs

  # File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
  # export HADOOP_SLAVES=${HADOOP_HOME}/conf/slaves

  # host:path where hadoop code should be rsync'd from.  Unset by default.
  # export HADOOP_MASTER=master:/home/$USER/src/hadoop

  # Seconds to sleep between slave commands.  Unset by default.  This
  # can be useful in large clusters, where, e.g., slave rsyncs can
  # otherwise arrive faster than the master can service them.
  # export HADOOP_SLAVE_SLEEP=0.1

  # The directory where pid files are stored. /tmp by default.
  # export HADOOP_PID_DIR=/var/hadoop/pids

  # A string representing this instance of hadoop. $USER by default.
  # export HADOOP_IDENT_STRING=$USER

  # The scheduling priority for daemon processes.  See 'man nice'.
  # export HADOOP_NICENESS=10
fi
