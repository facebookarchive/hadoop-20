#!/usr/bin/env python

#
# $Id$
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
# This is a helper script to cleanup the planfile and the logs from all
# participating hosts.
#

import optparse
import sys
import subprocess
import time
import os
import signal
import datetime
import commands

if len(sys.argv) < 2 or sys.argv[1].startswith('-'):
  print 'Usage: %s <planfile>\nThis will cleanup the planfile and the logs from all participating hosts.' % sys.argv[0]
  sys.exit(0)

if not sys.argv[1].startswith('/tmp'):
  print 'Planfile is typically in the /tmp directory. Are you sure?'
  sys.exit(1)

planFile = sys.argv[1]
hostsList = None
f = None

try:
  f = open(planFile, 'r')
except IOError, e:
  print 'Planfile not found'
  sys.exit(1)

for line in f:
  if line.startswith('#'):
    continue
  if line.startswith('hostslist='):
    hostsList = line[len('hostslist='):].strip().split(',')
    break
f.close()

if len(hostsList) == 0:
  print 'No hosts list found in plan file. Exiting.'
  sys.exit(1)

for host in hostsList:
  cmd = 'ssh %s "rm -f %s*"' % (host, planFile)
  print 'Executing "%s"' % cmd
  print commands.getoutput(cmd)

print 'Done'

