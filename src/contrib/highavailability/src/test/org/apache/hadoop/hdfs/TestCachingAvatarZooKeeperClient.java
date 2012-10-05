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
import java.nio.channels.FileLock;
import java.lang.reflect.*;

import junit.framework.TestCase;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.CachingAvatarZooKeeperClient.ZooKeeperCall;
import org.apache.hadoop.hdfs.CachingAvatarZooKeeperClient.FileWithLock;

public class TestCachingAvatarZooKeeperClient {
  final static Log LOG = LogFactory.getLog(TestCachingAvatarZooKeeperClient.class);

  @Test
  public void testTryLock() throws Exception {
    String directoryName = "/tmp/temp" + Long.toString(System.nanoTime());
    try {
      Configuration conf = new Configuration();
      conf.set("fs.ha.zookeeper.cache.dir", directoryName);
      conf.setBoolean("fs.ha.zookeeper.cache", false);
      CachingAvatarZooKeeperClient cazkc = new CachingAvatarZooKeeperClient(conf, null);
      Method m = CachingAvatarZooKeeperClient.class.getDeclaredMethod(
          "tryLock", ZooKeeperCall.class);
      ZooKeeperCall call = cazkc.new GetStat(null);
      m.setAccessible(true);
      FileWithLock fl = (FileWithLock) m.invoke(cazkc, call);
      fl.lock.release();
      TestCase.assertNotNull(fl.lock);
      fl = (FileWithLock) m.invoke(cazkc, call);
      TestCase.assertNotNull(fl.lock);
      fl.lock.release();
      new File(directoryName, ".avatar_zk_cache_lock").delete();
      m.invoke(cazkc, call);
    } finally {
      new File(directoryName, ".avatar_zk_cache_lock").delete();
      new File(directoryName).delete();
    }
  }

}
