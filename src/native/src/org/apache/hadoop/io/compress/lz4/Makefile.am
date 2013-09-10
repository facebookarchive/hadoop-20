#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

vpath %.c $(HADOOP_NATIVE_SRCDIR)/$(subdir)

AM_CPPFLAGS = @JNI_CPPFLAGS@ -I$(HADOOP_NATIVE_SRCDIR)/src -I$(HADOOP_NATIVE_SRCDIR)/lib/lz4/include
AM_LDFLAGS = @JNI_LDFLAGS@ -L$(HADOOP_NATIVE_SRCDIR)/lib/lz4/lib
AM_CFLAGS = -g -Wall -fPIC -O2 -m$(JVM_DATA_MODEL)

lib_LTLIBRARIES = libhadooplz4.la

libhadooplz4_la_SOURCES = Lz4Compressor.c Lz4Decompressor.c lz4.c lz4hc.c
libhadooplz4_la_LDFLAGS = -version-info 0:1:0

#
#vim: sw=4: ts=4: noet
#
