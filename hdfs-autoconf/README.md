What is this?
=============

This is autoconfigurator and autolauncher for a local HDFS cluster.
It is supposed to be mainly used for developer purposes, and it provides
you with bunch of scripts for setting everything up in a minute.. or maybe two.
Enjoy!

DISCLAIMER: The scripts are written and tested on the GNU system and relies
on GNU tools. At least two of them (`sed` & `readlink`) are known
to be incompatible with their BSD implementations.



STARTING CLUSTER
================

1. Make sure you have a zookeeper quorum started somewhere and that file
  `config-meta/avatar-shared.sed` has a `zookeeper-quorum` entry that points
  to the quorum. If not, you can start a local zookeeper via
  `zookeeper.sh start` command
2. `./build.sh` - builds all sources needed to start HDFS cluster
3. `./avatar-format` - formats cluster directories
4. `./start-dev-cluster --count 3` - starts local cluster with 3 datanodes.

[OPTIONAL] If you want to change any `core-site.xml` or `hdfs-site.xml`
  properties, make the necessary changes in the `config-templates/core-site.xml.template` and
  `config-meta/hdfs-site.xml.template` files. If you want to configure cluster
  directories, please refer to FAQ questions "Where do namenodes store their data?" and
  "Where do datanodes store their data?".



F.A.Q
=====

Where do I find cluster log files?
----------------------------------

Logs directory is specified by `$LOGS_DIRECTORY` variable, which defauls to
`$HADOOP_VERSION/logs`.


Where do namenodes store their data?
------------------------------------

1. The directory that is used as a local directory for the active namenode is
  specified in the `./config-meta/avatar-zero.sed` file.
2. Similar to the active namenode, the local directory for the standby
  specified in the `./config-meta/avatar-one.sed` file.
3. The shared namenodes directory is specified in the
  `./config-meta/avatar-shared.sed` file


Where do datanodes store their data?
------------------------------------

Each datanode has a set of volumes, and autotool maps volumes
to distinct local directories. These directories are specified in
datanode configuration file which is only one line long and has the following
entry:

```
s:{{DataNode-volumes}}:<path-to-volume-1-directory>[,<path-to-volume-2-directory>...]:g
```

In case of starting cluster with `./start-dev-cluster --count 5` command,
every of 5 datanodes will be started with a configuration file produced with the
help of `./config-meta/avatar-datanode.template` template. Consider having the following
template:

```
s:{{DataNode-volumes}}:/tmp/hadoop-datanode-XXX-vol0/,/tmp/hadoop-datanode-XXX-vol1/:g
```

This would mean that the first datanode has two volumes mapped to
`/tmp/hadoop-datanode-1-vol0/` and `/tmp/hadoop-datanode-1-vol1/` directories, and the
forth one has `/tmp/hadoop-datanode-4-vol0/` and `/tmp/hadoop-datanode-4-vol1/`.
That is because the "XXX" sequence in the `avatar-datanode.template` file is
substituted with the sequential datanode number to provide it with unique
directories on the local machine.


What is the format of files in `config-meta` directory?
-------------------------------------------------------

These files are SED (Stream Editor) scripts. Though the syntax of SED scripts
is not coincise, autoconf tool utilizes only `substitute` command.

The substitution command basically looks like this:

```
s:cat:dog:g
```

This example will substitute every 'cat' for 'dog'. The 's' letter stands for
'substitute' command, and the trailing 'g' is a flag that enforces sed to substitute
every entry of 'cat'; otherwise it would be done only for first occurences of
'cat' per line.

Any symbol could be used as a command delimeter. Thus said, the followings are fully
equal to the previous example
```
  s_cat_dog_g
  s%cat%dog%g
  s/cat/dog/g
```

This feature could be utilized to avoid escaping inside of sed scripts. Consider
looking at the following example
```
  s:some-folder:/tmp/foo:g
  s_URL_localhost:7777_g
```


How do I add new datanode configuration file?
---------------------------------------------

1. create a file with the name that matches format 'avatar-datanode-*.sed'
(the format of the datanode configuration files is specified by
`$DATANODE_CONFIG_FILES` variable in `config.sh` file)

2. Fill in the file with the following content
```
s:{{DataNode-volumes}}:<path-to-volume-1-directory>[,<path-to-volume-2-directory>...]:g
```


What is an example of datanode config file with multiple volumes?
-----------------------------------------------------------------

A datanode with two volumes, each resides in its own directory, will look the
following way

```
s:{{DataNode-volumes}}:/tmp/mydatanode-volume-1/,/tmp/mydatanode-volume-2/:g
```

So the directories should be listed one after another, separated with comma
delimeter.
NOTE: Make sure you do not put any spaces!


What exactly does autoconf tool do?
-----------------------------------

Whenever autoconf tool starts some HDFS instance, it does the following
sequence of actions:

1. Picks template files from `config-templates` direcotry
2. Runs `sed` scripts from `config-meta` directory over them
3. Puts results of sed execution to the `hadoop-0.20/bin` directory (the path
  to `hadoop-0.20` directory is specified via `$HADOOP_VERSION`)
4. Launches the HDFS instance


PRO stuff: multiple hadoop checkouts
------------------------------------

To switch between multiple hadoop checkouts just edit `./config.sh` file,
setting a `$HADOOP_VERSION` variable to the path of checkout you would like.



Files overview
==============

Client scripts
--------------

This is the list of scripts that are designed to be used by user. For more
information, you can refer to the source code of every script or just
run it with `--help` argument.

* `./build.sh` - builds everything
* `./avatar-format` - formats directories for avatar namenodes (both active and
  standby)
* `./avatar-zero-start` - starts active avatar
* `./avatar-one-start` - starts standby avatar
* `./avatar-datanode-start` - allows you to choose a config and start a datanode
  instance configured according to it.
  instance. Zookeeper is absolutely necessary for the cluster functioning, and
  it is started and stopped automatically with cluster
* `./start-dev-cluster.sh` - starts all the nodes as daemons for the local cluster
* `./stop-dev-cluster.sh` - stops instantiated developer cluster (simply killing
  all the processes with `avatar` in the name)
* `./zookeeper.sh` - this script is used to start and stop local zookeeper


Other directory files
---------------------

* `./config-meta` - the directory that contains all the options for the local
  cluster
  - `./config-meta/avatar-shared.sed` - configuration of shared directories, used by
    both Active and Stand-by avatar nodes
  - `./config-meta/avatar-zero.sed` - configuration of local directories for node zero
  - `./config-meta/avatar-one.sed` - configuration of local directories for node one
  - `./config-meta/avatar-datanode*.sed` - configuration files for datanodes, one file per
    node.
  - `./config-meta/avatar-datanode.template` - configuration file that is used
    to automatically generate datanode configuration files. Read more about this
    file in the FIXME
* `./config-templates` - stores all the files that are been run substitutions over.
* `./launchpad` - that stores generated scripts, should not be used
  unless you _really_ know what you do.
* `./scripts` - here you can find scripts that do the dirty job
* `./README.md` - markdown README in best github traditions.
* `./config.sh` - this file exports a `$HADOOP_VERSION` variable as well as
  couple of other variables. You might refer to the file often if you have
  multiple hadoop checkouts

