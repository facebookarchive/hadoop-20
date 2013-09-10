#!/bin/bash

# Mount an HDFS uri into a local directory. Start up an instance of the proxy
# if there isn't one already running.

hdfs_uri=$1
local_dir=$2

START_PORT=34550
END_PORT=34700

# Needs to be root user to restart rpcidmapd. Will sudo as hadoop later
if [ $UID -ne 0 ]; then
  echo "Error: must run as root user"
  exit 1
fi

# Does hadoop user exist on system?
if ! /usr/bin/getent passwd hadoop >/dev/null 2>/dev/null; then echo
  echo "Error: hadoop user does not exist on system."
  exit 1
fi

# Find a free port to bind to between the start and end ports
# Return 0 if nothing was free in that range
find_free_port() {
  start=$1
  end=$2
  port=$start
  while true; do
    free=$(lsof -iTCP:$port | wc -l)
    if [ $free == "0" ]; then
      break
    fi
    port=$(( $port + 1))
    if [ $port -gt $end ]; then
      port=0
      break
    fi
  done
  echo $port
}

# Get a port of an existing NFS proxy. If there isn't one, return 0
get_existing_port() {
  running_pid=$(/usr/bin/pgrep -f org.apache.hadoop.hdfs.nfs.nfs4.NFS4Server)
  if [ $? != "0" ]; then
    echo "0"
    return
  fi

  if [ $(echo "${running_pid}" | wc -l) != "1" ]; then
    # More than one proxy. What's going on?
    exit 6
  fi

  port=$(/bin/awk -F'\0' '{ print $(NF-1) }' /proc/$running_pid/cmdline)
  if ! echo "${port}" | /bin/egrep -q '^[0-9]+$'; then
    # Command line looks weird. What's going on?
    exit 7
  fi

  echo ${port}
}

# Start up an instance of the proxy
start_proxy() {
  # Pick a free port to run on
  free_port=$(find_free_port $START_PORT $END_PORT)
  if [ $free_port -eq 0 ]; then
    echo "Error: could not find a free port"
    exit 4
  fi
  $(dirname ${BASH_SOURCE[0]})/start-nfs-server.sh $free_port >/dev/null 2>/dev/null
  sleep 5
  echo $free_port
}

if [ $# -ne 2 ]; then
  echo "Usage: $0 <hdfs uri> <directory>"
  echo
  echo "  Mounts the HDFS location into the local directory"
  echo
  exit 1
fi

if ! echo $1 | /bin/egrep -q "^hdfs://[^:/]+:[0-9]+/.+$"; then
  echo "Error: HDFS URI '$hdfs_uri' is not valid"
  exit 2
fi

short_uri=$(echo "${hdfs_uri}" | sed -e 's/^hdfs:\/\/*//' -e 's/^\([^:]*\):\([0-9]*\)/\1.\2/')

if [ ! -d "${local_dir}" ]; then
  echo "Error: Directory '${local_dir}' does not exist"
  exit 3
fi

existing_port=$(get_existing_port)

if [ $existing_port == "0" ]; then
  existing_port=$(start_proxy)
fi

/bin/mount -t nfs4 "localhost:/${short_uri}" "${local_dir}" -o rw,intr,port=${existing_port}

exit $?
