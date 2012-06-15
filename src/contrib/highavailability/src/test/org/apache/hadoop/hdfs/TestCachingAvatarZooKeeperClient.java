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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import org.apache.hadoop.fs.FileUtil;
import java.nio.channels.FileLock;
import java.lang.reflect.*;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.MiniAvatarCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.MiniAvatarCluster.NameNodeInfo;

public class TestCachingAvatarZooKeeperClient {
  final static Log LOG = LogFactory.getLog(TestCachingAvatarZooKeeperClient.class);

  private Configuration conf;

  @BeforeClass
  public static void setUpStatic() throws Exception {
  }

  public void setUp() throws Exception {
  }
  
  @Test
  public void testTryLock() throws Exception {
    String directoryName = "/tmp/temp" + Long.toString(System.nanoTime());
    try {
      Configuration conf = new Configuration();
      conf.set("fs.ha.zookeeper.cache.dir", directoryName);
      conf.setBoolean("fs.ha.zookeeper.cache", false);
      CachingAvatarZooKeeperClient cazkc = new CachingAvatarZooKeeperClient(conf, null);
      Method m = CachingAvatarZooKeeperClient.class.getDeclaredMethod("tryLock", Boolean.TYPE);
      m.setAccessible(true);
      FileLock fl = (FileLock) m.invoke(cazkc, true);
      fl.release();
      TestCase.assertNotNull(fl);
      fl = (FileLock) m.invoke(cazkc, true);
      TestCase.assertNotNull(fl);
      fl.release();
      new File(directoryName, ".avatar_zk_cache_lock").delete();
      m.invoke(cazkc, true);
    } finally {
      new File(directoryName, ".avatar_zk_cache_lock").delete();
      new File(directoryName).delete();
    }
  }

}
