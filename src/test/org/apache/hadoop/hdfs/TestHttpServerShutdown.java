/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestHttpServerShutdown {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static final Log LOG = LogFactory
      .getLog(TestHttpServerShutdown.class);
  private static Set<Thread> oldThreads;
  private volatile boolean fsckCalled = false;

  private class TestHandler extends InjectionHandler {
    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.NAMENODE_FSCK_START) {
        fsckCalled = true;
        while (true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
  }

  private class FsckThread extends Thread {

    public void run() {
      try {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(os);
        new DFSck(conf, ps).run(new String[] { "/" });
      } catch (Exception e) {
      }
    }
  }

  @Test(timeout = 180000)
  public void testShutdown() throws Exception {
    oldThreads = Thread.getAllStackTraces().keySet();
    conf = new Configuration();
    conf.setInt(HttpServer.HTTP_THREADPOOL_MAX_STOP_TIME, 3000);
    cluster = new MiniDFSCluster(conf, 0, true, null);

    InjectionHandler.set(new TestHandler());
    Thread fsck = new FsckThread();
    fsck.setDaemon(true);
    fsck.start();

    while (!fsckCalled) {
      Thread.sleep(1000);
      LOG.info("Waiting for fsck to hit NN");
    }
    cluster.shutdown();

    LOG.info("Alive Non Daemon threads : ");
    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread
        .getAllStackTraces().entrySet()) {
      Thread t = entry.getKey();
      if (!t.isDaemon() && !oldThreads.contains(t)) {
        LOG.info("Thread : " + t.getName());
        for (StackTraceElement e : entry.getValue()) {
          LOG.info(e);
        }
        fail("Thread : " + t.getName() + " is not a daemon thread");
      }
    }
  }
}
