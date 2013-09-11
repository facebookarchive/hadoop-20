#!/usr/bin/env python

# $Id$
#
# Author: Thilee Subramaniam
#
# Copyright 2012 Quantcast Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# This program invokes given number of client processes on the given set of
# remote clients (Java and C++) and makes use of the plan file to apply load
# on the DFS server.

import optparse
import sys
import subprocess
import time
import os
import signal
import datetime
import commands
import resource
import re

class Globals:
  MASTER_PATH = ''
  SLAVE_PATH = ''
  SLAVE_BASE_DIR = ''

  CLIENT_PATH = ''
  MSTRESS_LOCK = '/tmp/mstress_master.lock'
  SIGNALLED = False
  SERVER_CMD = ""
  SERVER_KEYWORD = ""
  KFS_SERVER_CMD = "metaserver"
  KFS_SERVER_KEYWORD = "metaserver"
  HDFS_SERVER_CMD = "java"
  HDFS_SERVER_KEYWORD = "NameNode"

def ParseCommandline():
  parser = optparse.OptionParser()
  parser.add_option('-m', '--mode',
                    action='store',
                    default='master',
                    type='string',
                    help='Run as master or slave')
  parser.add_option('-f', '--filesystem',
                    action='store',
                    default=None,
                    type='string',
                    help='Filesystem whose metaserver to test. qfs or hdfs.')
  parser.add_option('-s', '--server',
                    action='store',
                    default=None,
                    type='string',
                    help='Metaserver or Namenode hostname.')
  parser.add_option('-p', '--port',
                    action='store',
                    default=None,
                    type='int',
                    help='Metaserver or Namenode port')
  parser.add_option('-c', '--client-hostname',
                    action='store',
                    default=None,
                    type='string',
                    help='mstress slave\'s hostname (slave only option).')
  parser.add_option('-k', '--client-lookup-key',
                    action='store',
                    default=None,
                    type='string',
                    help='mstress slave\'s lookup key to be used (slave only option).')
  parser.add_option('-t', '--client-testname',
                    action='store',
                    default=None,
                    type='string',
                    help='Test to run on mstress slave (slave only option).')
  parser.add_option('-a', '--plan',
                    action='store',
                    default='%s/plan/plan' % (os.path.dirname(os.path.realpath(__file__))),
                    type='string',
                    help='Plan file containing client instructions in the client.')
  parser.add_option('-l', '--leave-files', action='store_true',
                    default=False, help='Leave files. Does not perform delete test.')

  opts, args = parser.parse_args()
  if args:
    sys.exit('Unexpected arguments: %s.' % str(args))

  if not opts.filesystem or not opts.server or not opts.port:
    sys.exit('Missing mandatory arguments.')
  if opts.mode not in ('master', 'slave'):
    sys.exit('Invalid mode.')
  if opts.mode == 'master':
    # master should not have -c option
    if opts.client_hostname is not None:
      sys.exit('Master: does not support -c option.')
    if opts.client_testname is not None:
      sys.exit('Master: does not support -t option.')
  else:
    # for slave, this is the slave host name.
    hosts = opts.client_hostname.split(',')
    if len(hosts) != 1:
      sys.exit('Slave: Error in client host name.')
    if opts.client_testname is None or opts.client_lookup_key is None:
      sys.exit('Slave: Error in client test name or lookup key.')

  return opts


def PrintMemoryUsage(opts):
  if sys.platform in ('Darwin', 'darwin'):
    psCmd = "ps -o rss,pid,command | grep %s | grep %s | grep -v grep | awk '{print $1}'" % (Globals.SERVER_CMD, Globals.SERVER_KEYWORD)
  else:
    psCmd = "ps -C %s -o rss,pid,cmd | grep %s | awk '{print $1}'" % (Globals.SERVER_CMD, Globals.SERVER_KEYWORD)

  proc = subprocess.Popen(['ssh', opts.server, psCmd],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
  result = proc.communicate()
  if result and len(result[0].strip()) > 0:
    print "Memory usage %sKB" % result[0].strip()
  else:
    print "Memory usage <unknown> KB"


def RunMStressMaster(opts, hostsList):
  """ Called when run in master mode. Calls master funcions for 'create',
       'stat', and 'readdir'.

  Args:
    opts: options object, from parsed commandine options.
    hostsList: list of hosts obtained from plan file.

  Returns:
    True on success. False on failure.
  """

  # print 'Master: called with %r, %r' % (opts, hostsList)

  startTime = datetime.datetime.now()
  if RunMStressMasterTest(opts, hostsList, 'create_write') == False:
    return False
  deltaTime = datetime.datetime.now() - startTime
  print '\nMaster: Create & Write test took %d.%d sec' % (deltaTime.seconds, deltaTime.microseconds/1000000)
  PrintMemoryUsage(opts)
  print '=========================================='
  
  startTime = datetime.datetime.now()
  if RunMStressMasterTest(opts, hostsList, 'stat') == False:
    return False
  deltaTime = datetime.datetime.now() - startTime
  print '\nMaster: Stat test took %d.%d sec' % (deltaTime.seconds, deltaTime.microseconds/1000000)
  print '=========================================='

  startTime = datetime.datetime.now()
  if RunMStressMasterTest(opts, hostsList, 'readdir') == False:
    return False
  deltaTime = datetime.datetime.now() - startTime
  print '\nMaster: Readdir test took %d.%d sec' % (deltaTime.seconds, deltaTime.microseconds/1000000)
  print '=========================================='

  startTime = datetime.datetime.now()
  if RunMStressMasterTest(opts, hostsList, 'read') == False:
    return False
  deltaTime = datetime.datetime.now() - startTime
  print '\nMaster: Read test took %d.%d sec' % (deltaTime.seconds, deltaTime.microseconds/1000000)
  print '=========================================='
  
  startTime = datetime.datetime.now()
  if RunMStressMasterTest(opts, hostsList, 'rename') == False:
    return False
  deltaTime = datetime.datetime.now() - startTime
  print '\nMaster: Rename test took %d.%d sec' % (deltaTime.seconds, deltaTime.microseconds/1000000)
  print '=========================================='
  
  if opts.leave_files:
    print "\nNot Renaming files because of -l option"
    return False

  startTime = datetime.datetime.now()
  if RunMStressMasterTest(opts, hostsList, 'delete') == False:
    return False
  deltaTime = datetime.datetime.now() - startTime
  print '\nMaster: Delete test took %d.%d sec' % (deltaTime.seconds, deltaTime.microseconds/1000000)
  print '=========================================='

  PrintStatsFromClients(hostsList, opts)

  return True

def PrintStatsFromClients(hostsList, opts):
  """
  Called at the end of benchmarking.  This function ssh's into each of the
  client hosts and greps the client logs for benchmark lines inserted by the
  MStress_Client

  Args:
    opts: options object, from parsed commandline options.
    hostsList: list of hosts obtained from plan file.
  """

  # kick off grepping the log files in the client hosts
  running_procs = []
  for host in hostsList:
    p = subprocess.Popen(['/usr/bin/ssh', host,
                         "grep -e '\\[benchmark\\]' %s/plan/*client.log | cut -d' ' \
                         -f 2,3" % (Globals.SLAVE_BASE_DIR)],
                         stdout=subprocess.PIPE)

    running_procs.append(p)

  # mapping from call (str) -> list of timings (float)
  # we maintain all the latencies in this dictionary -> list mapping
  timings = {}
  for proc in running_procs:

    # block until that ssh child is done, and then read out its standard out.
    out, err = proc.communicate()
    if out:

      # the lines are of the form:
      # create_write: 0.023874,0.26489 ...
      # rename: 0.005003 ...
      # parse and bucket them into the timings dictionary
      for line in out.split('\n'):
        if ' ' not in line:
          continue

        (call, latencies) = line.split(' ', 1)
        latencies = latencies.strip()
        if latencies == '':
          continue

        if call not in timings: 
          timings[call] = []
        timings[call].extend(map(lambda x: float(x), latencies.split(',')))

  # for each of the calls, compute the metrics and print them out
  for call in timings:
    timings[call].sort()

    total = sum(timings[call])
    
    if total > 0:
      avg = total/len(timings[call])
    else:
      avg = 0

    print "%s num=%d, sum=%f, avg=%f, p50=%f, p90=%f" % (call,
            len(timings[call]),
            total,
            avg,
            pct(timings[call], 50),
            pct(timings[call], 90))
 
def pct(l, p):
  """ returns the `p`-th percentile element in a sorted list `l`"""
  return l[(int)((p/100.0)*len(l))]

def RunMStressMasterTest(opts, hostsList, test):
  """ Called when run in master mode. Invokes the slave version of the same
      program on the provided hosts list with the given test name.

  Args:
    opts: parsed commandline options.
    hostsList: list of hosts obtained from plan file.
    test: string: test name to call.

  Returns:
    False on error, True on success
  """
  if Globals.SIGNALLED:
    return False

  # invoke remote master client.
  ssh_cmd = '%s -m slave -f %s -s %s -p %d -t %s' % (
              Globals.SLAVE_PATH,
              opts.filesystem,
              opts.server,
              opts.port,
              test)
  clientHostMapping = MapHostnameForTest(hostsList, test)
  running_procs = {}

  for client in hostsList:
    slaveLogfile = Globals.SLAVE_BASE_DIR + '/plan/plan_' + client + '_' + test + '_' + opts.filesystem + '.slave.log'
    p = subprocess.Popen(['/usr/bin/ssh', client,
                          '%s -c %s -k %s >& %s' % (ssh_cmd, client, clientHostMapping[client], slaveLogfile)],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    running_procs[p] = client

  success = True
  isLine1 = True
  while running_procs:
    tobedelkeys = []
    for proc in running_procs.iterkeys():
      client = running_procs[proc]
      retcode = proc.poll()
      if retcode is not None:
        sout,serr = proc.communicate()
        if sout:
          print '\nMaster: output of slave (%s):%s' % (client, sout)
        if serr:
          print '\nMaster: err of slave (%s):%s' % (client, serr)
        tobedelkeys.append(proc)
        if retcode != 0:
          print "\nMaster: '%s' test failed. Please make sure test directory is empty and has write permission, or check slave logs." % test
          success = False
      else:
        if Globals.SIGNALLED:
          proc.terminate()

    for k in tobedelkeys:
      del running_procs[k]

    if running_procs:
      if isLine1:
        sys.stdout.write('Master: remote slave running \'%s\'' % test)
        isLine1 = False
      else:
        sys.stdout.write('.')
      sys.stdout.flush()
      time.sleep(0.5)
  return success

def MapHostnameForTest(clients, test):
  """ Determines the '-c' argument to use for slave invocation. This argument
      is passed to the C++/Java client so that the client can use it as a key
      to read the plan file.

      For 'create', this name is the same as the client name. But for doing
      a 'stat' or a 'readdir' we want to run the tests on a client different
      from the one that created the path.
  Args:
    clients: list of strings, clients.
    test: string, the name of the test.

  Returns:
    map of strings, client name to '-c' argument.
  """
  mapping = {}
  length = len(clients)
  for i in range(0, length):
    if test == 'stat' or test == 'readdir' or test == 'read':
      mapping[clients[i]] = clients[(i+1)%length]
    else:
      mapping[clients[i]] = clients[i]

  return mapping

def RunMStressSlave(opts, clientsPerHost):
  """ Called when the code is run in slave mode, on each slave.
      Invokes number of client processes equal to 'clientsPerHost'.

  Args:
    opts: parsed commandline options.
    clientsPerHost: integer, number of processes to run on each host.

  Returns:
    True if client returns success. False otherwise.
  """

  print 'Slave: called with %r, %d' % (opts, clientsPerHost)
  os.putenv('KFS_CLIENT_DEFAULT_FATTR_REVALIDATE_TIME',"-1")

  running_procs = []
  for i in range(0, clientsPerHost):
    clientLogfile = '%s_%s_proc_%02d_%s_%s.client.log' % (opts.plan, opts.client_hostname, i, opts.client_testname, opts.filesystem)
    args = ["%s -s %s -p %s -a %s -c %s -t %s -n proc_%02d >& %s" % (
            Globals.CLIENT_PATH,
            opts.server,
            str(opts.port),
            opts.plan,
            opts.client_lookup_key,
            opts.client_testname,
            i,
            clientLogfile)]
    print 'Slave: args = %r' % args
    p = subprocess.Popen(args,
                         shell=True,
                         executable='/bin/bash',
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    running_procs.append(p)

  success = True
  isLine1 = True
  while running_procs:
    for proc in running_procs:
      ret = proc.poll()
      if ret is not None:
        sout,serr = proc.communicate()
        if sout:
          print '\nSlave: output of (ClientHost %s, ClientNo %r):%s' % (opts.client_hostname, proc, sout)
        if serr:
          print '\nSlave: err of (ClientHost %s, ClientNo %r):%s' % (opts.client_hostname, proc, serr)
        running_procs.remove(proc)
        if ret != 0:
          print '\nSlave: mstress client failed. Please check client logs.'
          success = False
      else:
        if Globals.SIGNALLED:
          proc.terminate()

    if running_procs:
      if isLine1:
        sys.stdout.write('Slave: load client \'%s\' running' % opts.client_testname)
        isLine1 = False
      else:
        sys.stdout.write('.')
      sys.stdout.flush()
      time.sleep(0.5)
  return success

def ReadPlanFile(opts):
  """ Reads the given plan file to extract the list of client-hosts and
      process-count per client-host.

  Args:
    opts: parsed commandline options.

  Returns:
    hostslist: list of client host names
    clientsPerHost: integer: client processes per client host.
  """

  hostsList = None
  clientsPerHost = None
  leafType = None
  numLevels = None
  numToStat = None
  nodesPerLevel = None

  planfile = open(opts.plan, 'r')
  for line in planfile:
    if line.startswith('#'):
      continue
    if line.startswith('hostslist='):
      hostsList = line[len('hostslist='):].strip().split(',')
    elif line.startswith('clientsperhost='):
      clientsPerHost = int(line[len('clientsperhost='):].strip())
    elif line.startswith('type='):
      leafType = line[len('type='):].strip()
    elif line.startswith('levels='):
      numLevels = int(line[len('levels='):].strip())
    elif line.startswith('nstat='):
      numToStat = int(line[len('nstat='):].strip())
    elif line.startswith('inodes='):
      nodesPerLevel = int(line[len('inodes='):].strip())
  planfile.close()
  if None in (hostsList, clientsPerHost, leafType, numLevels, numToStat, nodesPerLevel):
    sys.exit('Failed to read plan file')

  nodesPerProcess = 0
  leafNodesPerProcess = 0
  for l in range(1,numLevels+1):
    nodesPerProcess += pow(nodesPerLevel,l)
    if l == numLevels:
      leafNodesPerProcess = pow(nodesPerLevel,l)
  inters = nodesPerProcess - leafNodesPerProcess
  overallNodes = nodesPerProcess * len(hostsList) * clientsPerHost
  overallLeafs = leafNodesPerProcess * len(hostsList) * clientsPerHost
  intermediateNodes = inters * len(hostsList) * clientsPerHost + len(hostsList) * clientsPerHost + 1
  totalNumToStat = numToStat * len(hostsList) * clientsPerHost

  print ('Plan:\n' +
        '   o %d client processes on each of %d hosts will generate load.\n' % (clientsPerHost, len(hostsList)) +
        '   o %d levels of %d nodes (%d leaf nodes, %d total nodes) will be created by each client process.\n' % (numLevels, nodesPerLevel, leafNodesPerProcess, nodesPerProcess) +
        '   o Overall, %d leaf %ss will be created, %d intermediate directories will be created.\n' % (overallLeafs, leafType, intermediateNodes))
  return hostsList, clientsPerHost


def SetGlobalPaths(opts):
  if opts.mode == 'master':
    mydir = os.path.dirname(os.path.realpath(__file__))
    Globals.MASTER_PATH = os.path.join(mydir, 'mstress.py')
    Globals.SLAVE_BASE_DIR = '/tmp/%s' % os.path.basename(mydir)
  
  mydir = '/tmp/%s' % os.path.basename(os.path.dirname(os.path.realpath(__file__)))
  Globals.SLAVE_PATH = os.path.join(mydir, 'mstress.py')
  Globals.SLAVE_BASE_DIR = mydir

  if opts.filesystem == 'qfs':
    Globals.CLIENT_PATH = os.path.join(mydir, 'mstress_client')
    Globals.SERVER_CMD = Globals.KFS_SERVER_CMD
    Globals.SERVER_KEYWORD = Globals.KFS_SERVER_KEYWORD
  elif opts.filesystem == 'hdfs':
    hdfsjars = commands.getoutput("echo %s/mstress_hdfs_client_jars/*.jar | sed 's/ /:/g'" % mydir)
    Globals.CLIENT_PATH = 'java -Xmx256m -cp %s:%s MStress_Client' % (mydir,hdfsjars)
    Globals.SERVER_CMD = Globals.HDFS_SERVER_CMD
    Globals.SERVER_KEYWORD = Globals.HDFS_SERVER_KEYWORD
  else:
    sys.exit('Invalid filesystem option')

def CreateLock(opts):
  if opts.mode != 'master':
    return
  if os.path.exists(Globals.MSTRESS_LOCK):
    sys.exit('Program already running. Please wait till it finishes')
  f = open(Globals.MSTRESS_LOCK, 'w')
  f.write(str(os.getpid()))
  f.close()

def RemoveLock(opts):
  if opts.mode != 'master':
    return
  if os.path.exists(Globals.MSTRESS_LOCK):
    f = open(Globals.MSTRESS_LOCK, 'r')
    pid = f.read()
    f.close()
    if int(pid) == os.getpid():
      os.unlink(Globals.MSTRESS_LOCK)

def HandleSignal(signum, frame):
  print "Received signal, %d" % signum
  Globals.SIGNALLED = True

def main():
  signal.signal(signal.SIGTERM, HandleSignal)
  signal.signal(signal.SIGINT, HandleSignal)
  signal.signal(signal.SIGHUP, HandleSignal)

  opts = ParseCommandline()

  SetGlobalPaths(opts)

  CreateLock(opts)

  try:
    (hostsList,clientsPerHost) = ReadPlanFile(opts)

    if opts.mode == 'master':
      return RunMStressMaster(opts, hostsList)
    else:
      return RunMStressSlave(opts, clientsPerHost)
  finally:
    RemoveLock(opts)

if __name__ == '__main__':
  success = main()
  if success:
    sys.exit(0)
  else:
    sys.exit(1)

