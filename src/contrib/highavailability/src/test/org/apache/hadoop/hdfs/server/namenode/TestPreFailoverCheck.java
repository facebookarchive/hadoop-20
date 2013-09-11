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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;

public class TestPreFailoverCheck {

  final static Log LOG = LogFactory.getLog(TestPreFailoverCheck.class);

  private MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutDown();
    }
    if (fs != null) {
      fs.close();
    }
    InjectionHandler.clear();
  }

  @Test
  public void testIsInitializedOK() throws Exception {
    conf = new Configuration();
    cluster = new MiniAvatarCluster(conf, 1, true, null, null);
    assertTrue(cluster.getNameNode(0).avatars.get(0).avatar.isInitialized());
    assertTrue(cluster.getNameNode(0).avatars.get(1).avatar.isInitialized());
  }
  
  @Test
  public void testUpgrade() throws Exception {
    conf = new Configuration();
    cluster = new MiniAvatarCluster.Builder(conf).enableQJM(false).build();
    fs = cluster.getFileSystem();
    
    // invalidate shared directory
    EditLogOutputStream elos = invalidateEditsDirAtIndex(0);
    doAnEdit();
    
    try {
      cluster.getNameNode(0).avatars.get(0).avatar.isInitialized();
      fail("Should not be initialized because of failed shared journal");
    } catch (IOException e) {
      LOG.info("Expected exception: " + e.getMessage());
    }
    
    // restore journal
    restoreEditsDirAtIndex(0, elos);
    cluster.getNameNode(0).avatars.get(0).avatar.rollEditLogAdmin();
    
    // avatar 0 should be OK again
    assertTrue(cluster.getNameNode(0).avatars.get(0).avatar.isInitialized());
  }
  
  private boolean doAnEdit() throws IOException {
    return fs
        .mkdirs(new Path("/tmp", Integer.toString(new Random().nextInt())));
  }
  
  private void restoreEditsDirAtIndex(int index, EditLogOutputStream elos) {
    FSImage fsimage = cluster.getNameNode(0).avatars.get(0).avatar.getFSImage();
    FSEditLog editLog = fsimage.getEditLog();

    JournalAndStream jas = editLog.getJournals().get(index);
    jas.setCurrentStreamForTests(elos);
  }
  
  /**
   * Replace the journal at index <code>index</code> with one that throws an
   * exception on flush.
   */
  private EditLogOutputStream invalidateEditsDirAtIndex(int index)
      throws IOException {
    FSImage fsimage = cluster.getNameNode(0).avatars.get(0).avatar.getFSImage();
    FSEditLog editLog = fsimage.getEditLog();

    JournalAndStream jas = editLog.getJournals().get(index);
    EditLogFileOutputStream elos = (EditLogFileOutputStream) jas
        .getCurrentStream();
    EditLogFileOutputStream spyElos = spy(elos);

    doThrow(new IOException("fail on write()")).when(spyElos).write(
        (FSEditLogOp) any());
    doThrow(new IOException("fail on write()")).when(spyElos).writeRaw(
        (byte[]) any(), anyInt(), anyInt());
    doThrow(new IOException("fail on write()")).when(spyElos).writeRawOp(
        (byte[]) any(), anyInt(), anyInt(), anyLong());
    doNothing().when(spyElos).abort();
    jas.setCurrentStreamForTests(spyElos);

    return elos;
  }
}

