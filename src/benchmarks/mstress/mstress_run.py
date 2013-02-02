#!/usr/bin/env python

#
# $Id$
#
# Author: Ying Zheng
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
# This is essentially a wrapper around mstress_plan.py and mstress.py.
#

import optparse
import sys
import subprocess
import time
import os
import datetime
import commands
import resource
import getpass
import re

class Params:
  TARGETS           = []
  CLIENT_HOSTS      = ''
  CLIENTS_PER_HOST  = 5
  PATH_TYPE         = "dir"
  PATH_LEVELS       = 3
  INODES_PER_LEVEL  = 16
  def NumFiles2Stat():
    return Params.INODES_PER_LEVEL**Params.PATH_LEVELS*Params.CLIENTS_PER_HOST*len(Params.CLIENT_HOSTS.split(","))/2
  NumFiles2Stat = staticmethod(NumFiles2Stat)

def Usage():
  print 'Usage: %s [clients] [fs_type,fs_host,fs_port] [fs_type,fs_host,fs_port]..' % sys.argv[0]
  print '       clients: comma separated list of client host names'
  print '       fs_type: qfs or hdfs'
  print '       fs_host: metaserver or namenode hostname'
  print '       fs_port: metaserver or namenode port'
  print 'Eg: %s 10.15.20.25,10.20.25.30 qfs,10.10.10.10,10000 hdfs,20.20.20.20,20000'
  sys.exit(0)


def PrintMsg(m):
  print '\033[34m' + m + '\033[0m'


def MakePlan():
  plan_file = '/tmp/mstress_%s_%s.plan' % (getpass.getuser(), time.strftime("%F-%H-%M-%S", time.gmtime()))
  PrintMsg("Preparing benchmark plan [%s] ..." % plan_file)
  subprocess.call(["./mstress_plan.py",
                   "--client-hosts",     Params.CLIENT_HOSTS,
                   "--clients-per-host", str(Params.CLIENTS_PER_HOST),
                   "--path-type",        Params.PATH_TYPE,
                   "--levels",           str(Params.PATH_LEVELS),
                   "--inodes-per-level", str(Params.INODES_PER_LEVEL),
                   "--num-to-stat",      str(Params.NumFiles2Stat()),
                   "-o",                 plan_file])
  return plan_file


def RunBenchmark(plan_file):
  for t in Params.TARGETS:
    type   = t[0]
    server = t[1]
    port   = t[2]
    result = Execute(type,
                     ["./mstress.py",
                      "-f", type,
                      "-s", server,
                      "-p", port,
                      "-a", plan_file])
    PrintResult(type, result)


def Execute(type, args):
  os.putenv("PYTHONUNBUFFERED","TRUE");
  PrintMsg("\n==========================================\nStarting benchmark for '%s'..." % type)

  result = ""
  proc = subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  while proc.poll() == None:
    output = proc.stdout.read(1)
    result += output
    sys.stdout.write(output)
    sys.stdout.flush()

  output = proc.stdout.read()
  result += output
  sys.stdout.write(output)
  sys.stdout.flush()
  proc.wait()

  return result


def PrintResult(type, result):
  PrintMsg("\nBenchmark results for '%s':" % type)
  for m in re.findall(r"(\w+) test took (\S+) sec",result):
    PrintMsg("%-10s: %s sec"%(m[0],m[1]))
  PrintMsg("\n%s\n==========================================" %
            re.search(r"Memory usage .*$", result, re.MULTILINE).group(0))


def ParseArgs():
  argc = len(sys.argv)
  if argc <= 1 or sys.argv[1].startswith('-') or argc < 3:
    Usage()

  Params.CLIENT_HOSTS = sys.argv[1].strip()

  triple = sys.argv[2].strip().split(',')
  if len(triple) != 3 or triple[0] not in ('qfs', 'hdfs'):
    Usage()
  Params.TARGETS.append(triple)

  if argc > 3:
    triple = sys.argv[3].strip().split(',')
    if len(triple) != 3 or triple[0] not in ('qfs', 'hdfs'):
      Usage()
    Params.TARGETS.append(triple)


def main():
  ParseArgs()
  plan_file = MakePlan()
  RunBenchmark(plan_file)

if __name__ == '__main__':
  main()

