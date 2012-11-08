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
package org.apache.hadoop.hdfs.notifier.server;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestServerLogReader {
  static private Logger LOG = LoggerFactory.getLogger(TestServerLogReader.class);

  static Configuration conf;
  
  @BeforeClass
  public static void initConf() {
    Configuration.addDefaultResource("namespace-notifier-server-default.xml");
    Configuration.addDefaultResource("hdfs-default.xml");
    conf = new Configuration();
    conf.addResource("namespace-notifier-server-site.xml");
    conf.addResource("hdfs-site.xml");
    conf = new Configuration();
  }
  
  /**
   * Creates the edits directory.
   * @return edits dir
   */
  private File createEditsDir() {
    while (true) {
      File editsDir = new File(System.getProperty("java.io.tmpdir"),
          new BigInteger(128, new Random()).toString(32));
      if (!editsDir.mkdir())
        continue;
      return editsDir;
    }
  }
  
  
  private EditLogFileOutputStream initEdits(File editsDir) throws IOException {
    File edits = new File(editsDir, "edits");
    File fstime = new File(editsDir, "fstime");

    if (!edits.createNewFile())
      throw new IOException("Failed to create edits file");
    EditLogFileOutputStream out = new EditLogFileOutputStream(edits, null);
    out.create();
    if (!fstime.createNewFile())
      throw new IOException("Failed to create fstime file");
    
    return out;
  }
  

  private EditLogFileOutputStream beginRoll(File editsDir,
      EditLogFileOutputStream editsOutput)
          throws IOException {
    File editsNew = new File(editsDir, "edits.new");

    editsOutput.close();
    if (!editsNew.createNewFile())
      throw new IOException("Failed to create edits.new file");
    EditLogFileOutputStream out = new EditLogFileOutputStream(editsNew, null);
    out.create();
    Assert.assertTrue(editsNew.exists());

    return out;
  }
  
  private void endRoll(File editsDir) throws IOException {
    File edits = new File(editsDir, "edits");
    File editsNew = new File(editsDir, "edits.new");
    File fstime = new File(editsDir, "fstime");
    
    Assert.assertTrue(editsNew.exists());
    Assert.assertTrue(fstime.exists());
    if (!editsNew.renameTo(edits)) {
      edits.delete();
      if (!editsNew.renameTo(edits))
        throw new IOException();
    }
    
    fstime.delete();
    DataOutputStream fstimeOutput =
        new DataOutputStream(new FileOutputStream(fstime));
    fstimeOutput.writeLong(System.currentTimeMillis());
    fstimeOutput.flush();
    fstimeOutput.close();
  }
  
  
  private void writeOperation(EditLogFileOutputStream out,
      long txId) throws IOException {
    FSEditLogOp.AddOp op = new FSEditLogOp.AddOp();
    op.setTransactionId(txId);
    op.set("/a/b", (short)3, 100L, 100L, 100L, new Block[0],
        PermissionStatus.createImmutable("x", "y", FsPermission.getDefault()),
        "x", "y");
    out.write(op);
    LOG.info("Wrote operation " + txId);
    out.setReadyToFlush();
    out.flush();
    LOG.info("Flushed operation " + txId);
  }
  
  
  @Test
  public void testOneOperation() throws Exception {
    File editsDir = createEditsDir();
    DummyServerCore core = new DummyServerCore();
    EditLogFileOutputStream out = initEdits(editsDir);
    ServerLogReader logReader = new ServerLogReader(core,
        editsDir.getAbsolutePath());
    core.logReader = logReader;
    Thread coreThread, logReaderThread;
    
    coreThread = new Thread(core);
    logReaderThread = new Thread(logReader);
    
    logReaderThread.start();
    coreThread.start();
    writeOperation(out, 1000);
    Thread.sleep(500);
    core.shutdown();
    logReaderThread.join();
    coreThread.join();
    
    Assert.assertEquals(1, core.notifications.size());
    Assert.assertEquals(1000, core.notifications.poll().txId);
  }
  
  @Test
  public void testMultipleOperations() throws Exception {
    File editsDir = createEditsDir();
    DummyServerCore core = new DummyServerCore();
    EditLogFileOutputStream out = initEdits(editsDir);
    ServerLogReader logReader = new ServerLogReader(core,
        editsDir.getAbsolutePath());
    core.logReader = logReader;
    Thread coreThread, logReaderThread;
    long txCount = 1000;
    
    coreThread = new Thread(core);
    logReaderThread = new Thread(logReader);
    
    logReaderThread.start();
    coreThread.start();
    for (long txId = 0; txId < txCount; txId ++)
      writeOperation(out, txId);
    Thread.sleep(500);
    core.shutdown();
    logReaderThread.join();
    coreThread.join();
    
    Assert.assertEquals(1000, core.notifications.size());
    for (long txId = 0; txId < txCount; txId ++)
      Assert.assertEquals(txId, core.notifications.poll().txId);
  }
  
  @Test
  public void testTwoOperationsRoll() throws Exception {
    File editsDir = createEditsDir();
    DummyServerCore core = new DummyServerCore();
    EditLogFileOutputStream out = initEdits(editsDir);
    ServerLogReader logReader = new ServerLogReader(core,
        editsDir.getAbsolutePath());
    core.logReader = logReader;
    Thread coreThread, logReaderThread;

    coreThread = new Thread(core);
    logReaderThread = new Thread(logReader);
    
    coreThread.start();
    Thread.sleep(1000);
    logReaderThread.start();
    writeOperation(out, 1000);
    out = beginRoll(editsDir, out);
    writeOperation(out, 1001);
    Thread.sleep(500);
    endRoll(editsDir);
    Thread.sleep(500);
    core.shutdown();
    logReaderThread.join();
    coreThread.join();
    
    Assert.assertEquals(2, core.notifications.size());
    Assert.assertEquals(1000, core.notifications.poll().txId);
    Assert.assertEquals(1001, core.notifications.poll().txId);
  }
  
  private void testMultipleOperationsRoll(long txPerNormalPhase,
      long txPerRollPhase, long txCount) throws Exception {
    File editsDir = createEditsDir();
    DummyServerCore core = new DummyServerCore();
    EditLogFileOutputStream out = initEdits(editsDir);
    ServerLogReader logReader = new ServerLogReader(core,
        editsDir.getAbsolutePath());
    core.logReader = logReader;
    Thread coreThread, logReaderThread;
    boolean rollPhase = false;
    
    coreThread = new Thread(core);
    logReaderThread = new Thread(logReader);
    
    logReaderThread.start();
    coreThread.start();
    Thread.sleep(1000);
    
    long count = 0;
    for (long txId = 0; txId < txCount; txId ++) {
      if (rollPhase) {
        count --;
        if (count == 0) {
          rollPhase = false;
          count = txPerNormalPhase;
          endRoll(editsDir);
        }
      } else {
        count --;
        if (count == 0) {
          rollPhase = true;
          count = txPerRollPhase;
          beginRoll(editsDir, out);
        }
      }
      
      writeOperation(out, txId);
      Thread.sleep(2);
    }
    
    Thread.sleep(1000);
    core.shutdown();
    logReaderThread.join();
    coreThread.join();
    
    Assert.assertEquals(txCount, core.notifications.size());
    for (long txId = 0; txId < txCount; txId ++)
      Assert.assertEquals(txId, core.notifications.poll().txId);
  }
  
  @Test
  public void testMultipleOperationsBigRoll() throws Exception {
    testMultipleOperationsRoll(3, 1000, 10000);
  }
  
  @Test
  public void testMultipleOperationsNormalRoll() throws Exception {
    testMultipleOperationsRoll(1000, 1000, 10000);
  }
  
  @Test
  public void testMultipleOperationsSmallRoll() throws Exception {
    testMultipleOperationsRoll(1000, 3, 10000);
  }
  
  class DummyServerCore extends EmptyServerCore {

    ConcurrentLinkedQueue<NamespaceNotification> notifications = 
        new ConcurrentLinkedQueue<NamespaceNotification>();

    IServerLogReader logReader;
    
    @Override
    public void handleNotification(NamespaceNotification n) {
      notifications.add(n);
    }
    
    @Override
    public void run() {
      try {
        while (!shutdownPending()) {
          NamespaceNotification n = logReader.getNamespaceNotification();
          if (n != null)
            handleNotification(n);
        }
      } catch (IOException e) {
        LOG.error("DummyServerCore failed reading notifications", e);
      } finally {
        shutdown();
      }
    }
    
    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }
  
}
