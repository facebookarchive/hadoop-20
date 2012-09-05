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

import junit.framework.TestCase;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestCheckpoint extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  static final int numDatanodes = 3;
  short replication = 3;

  private void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    assertTrue(fileSys.exists(name));
    int replication = fileSys.getFileStatus(name).getReplication();
    assertEquals("replication for " + name, repl, replication);
    //We should probably test for more of the file properties.    
  }
  
  private void cleanupFile(FileSystem fileSys, Path name)
    throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /**
   * put back the old namedir
   */
  private void resurrectNameDir(File namedir) 
    throws IOException {
    String parentdir = namedir.getParent();
    String name = namedir.getName();
    File oldname =  new File(parentdir, name + ".old");
    if (!oldname.renameTo(namedir)) {
      assertTrue(false);
    }
  }

  /**
   * remove one namedir
   */
  private void removeOneNameDir(File namedir) 
    throws IOException {
    String parentdir = namedir.getParent();
    String name = namedir.getName();
    File newname =  new File(parentdir, name + ".old");
    if (!namedir.renameTo(newname)) {
      assertTrue(false);
    }
  }

  /*
   * Verify that namenode does not startup if one namedir is bad.
   */
  private void testNamedirError(Configuration conf, Collection<File> namedirs) 
    throws IOException {
    System.out.println("Starting testNamedirError");
    MiniDFSCluster cluster = null;

    if (namedirs.size() <= 1) {
      return;
    }
    
    //
    // Remove one namedir & Restart cluster. This should fail.
    //
    File first = namedirs.iterator().next();
    removeOneNameDir(first);
    try {
      cluster = new MiniDFSCluster(conf, 0, false, null);
      cluster.shutdown();
      assertTrue(false);
    } catch (Throwable t) {
      // no nothing
    }
    resurrectNameDir(first); // put back namedir
  }

  /*
   * Simulate namenode crashing after rolling edit log.
   */
  private void testSecondaryNamenodeError1(Configuration conf)
    throws IOException {
    System.out.println("Starting testSecondaryNamenodeError 1");
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
        InjectionHandler.set(h);
    Path file1 = new Path("checkpointxx.dat");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, 
                                                false, null);
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edits log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      h.setSimulationPoint(InjectionEvent.SECONDARYNAMENODE_CHECKPOINT0);

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      h.clearHandler();
      secondary.shutdown();

      //
      // Create a new file
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file exists.
    // Then take another checkpoint to verify that the 
    // namenode restart accounted for the rolled edit logs.
    //
    System.out.println("Starting testSecondaryNamenodeError 2");
    cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
    cluster.waitActive();
    // Also check that the edits file is empty here
    // and that temporary checkpoint files are gone.
    FSImage image = cluster.getNameNode().getFSImage();
    for (Iterator<StorageDirectory> it = 
             image.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      assertFalse(NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW).exists());
    }
    for (Iterator<StorageDirectory> it = 
            image.storage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      assertFalse(image.getEditNewFile(sd).exists());
      File edits = image.getEditFile(sd);
      assertTrue(edits.exists()); // edits should exist and be empty
      long editsLen = edits.length();
      assertTrue(editsLen == Integer.SIZE/Byte.SIZE);
    }
    
    fileSys = cluster.getFileSystem();
    try {
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /*
   * Simulate a namenode crash after uploading new image
   */
  private void testSecondaryNamenodeError2(Configuration conf)
    throws IOException {
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
    InjectionHandler.set(h);
    
    System.out.println("Starting testSecondaryNamenodeError 21");
    Path file1 = new Path("checkpointyy.dat");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, 
                                                false, null);
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after uploading the new fsimage.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      h.setSimulationPoint(InjectionEvent.SECONDARYNAMENODE_CHECKPOINT1);

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      h.clearHandler();
      secondary.shutdown();

      //
      // Create a new file
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file exists.
    // Then take another checkpoint to verify that the 
    // namenode restart accounted for the rolled edit logs.
    //
    System.out.println("Starting testSecondaryNamenodeError 22");
    cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    try {
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /*
   * Simulate a secondary namenode crash after rolling the edit log.
   */
  private void testSecondaryNamenodeError3(Configuration conf)
    throws IOException {
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
    InjectionHandler.set(h);
    
    System.out.println("Starting testSecondaryNamenodeError 31");
    Path file1 = new Path("checkpointzz.dat");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, 
                                                false, null);
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edit log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      h.setSimulationPoint(InjectionEvent.SECONDARYNAMENODE_CHECKPOINT0);

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      h.clearHandler();
      secondary.shutdown(); // secondary namenode crash!

      // start new instance of secondary and verify that 
      // a new rollEditLog suceedes inspite of the fact that 
      // edits.new already exists.
      //
      secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();  // this should work correctly
      secondary.shutdown();

      //
      // Create a new file
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file exists.
    // Then take another checkpoint to verify that the 
    // namenode restart accounted for the twice-rolled edit logs.
    //
    System.out.println("Starting testSecondaryNamenodeError 32");
    cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    try {
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /**
   * Simulate a secondary node failure to transfer image
   * back to the name-node.
   * Used to truncate primary fsimage file.
   */
  void testSecondaryFailsToReturnImage(Configuration conf)
    throws IOException {
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
    InjectionHandler.set(h);
    
    System.out.println("Starting testSecondaryFailsToReturnImage");
    Path file1 = new Path("checkpointRI.dat");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, 
                                                false, null);
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    FSImage image = cluster.getNameNode().getFSImage();
    try {
      assertTrue(!fileSys.exists(file1));
      StorageDirectory sd = null;
      for (Iterator<StorageDirectory> it = 
                image.dirIterator(NameNodeDirType.IMAGE); it.hasNext();)
         sd = it.next();
      assertTrue(sd != null);
      long fsimageLength = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE).length();
      //
      // Make the checkpoint
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      h.setSimulationPoint(InjectionEvent.TRANSFERFSIMAGE_GETFILESERVER0);

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
        System.out.println("testSecondaryFailsToReturnImage: doCheckpoint() " +
            "failed predictably - " + e);
      }
      h.clearHandler();

      // Verify that image file sizes did not change.
      for (Iterator<StorageDirectory> it = 
              image.storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
        assertTrue(NNStorage.getStorageFile(it.next(), 
                                NameNodeFile.IMAGE).length() == fsimageLength);
      }

      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  /**
   * Simulate 2NN failing to send the whole file (error type 3)
   * The length header in the HTTP transfer should prevent
   * this from corrupting the NN.
   */
  public void testNameNodeImageSendFailWrongSize()
      throws IOException {
    System.out.println("Starting testNameNodeImageSendFailWrongSize");
    doSendFailTest(InjectionEvent.TRANSFERFSIMAGE_GETFILESERVER1, "is not of the advertised size");
  }

  /**
   * Simulate 2NN sending a corrupt image (error type 4)
   * The digest header in the HTTP transfer should prevent
   * this from corrupting the NN.
   */
  public void testNameNodeImageSendFailWrongDigest()
      throws IOException {
    System.out.println("Starting testNameNodeImageSendFailWrongDigest");
    doSendFailTest(InjectionEvent.TRANSFERFSIMAGE_GETFILESERVER2, "does not match advertised digest");
  }
  
  /**
   * Run a test where the 2NN runs into some kind of error when
   * sending the checkpoint back to the NN.
   * @param errorType the ErrorSimulator type to trigger
   * @param exceptionSubstring an expected substring of the triggered exception
   */
  private void doSendFailTest(InjectionEvent errorType, String exceptionSubstring)
      throws IOException {
    Configuration conf = new Configuration();
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
    InjectionHandler.set(h);
    
    Path file1 = new Path("checkpoint-doSendFailTest-" + errorType + ".dat");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, 
                                                true, null);
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edit log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      h.setSimulationPoint(errorType);

      try {
        secondary.doCheckpoint();  // this should fail
        fail("Did not get expected exception");
      } catch (IOException e) {
        // We only sent part of the image. Have to trigger this exception
        System.out.println("-------------------xxxxxxxx: " + StringUtils.stringifyException(e));
        assertTrue(e.getMessage().contains(exceptionSubstring));
      }
      h.clearHandler();
      secondary.shutdown(); // secondary namenode crash!

      // start new instance of secondary and verify that 
      // a new rollEditLog suceedes inspite of the fact that 
      // edits.new already exists.
      //
      secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();  // this should work correctly
      secondary.shutdown();

      //
      // Create a new file
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  


  /**
   * Test different startup scenarios.
   * <p><ol>
   * <li> Start of primary name-node in secondary directory must succeed. 
   * <li> Start of secondary node when the primary is already running in 
   *      this directory must fail.
   * <li> Start of primary name-node if secondary node is already running in 
   *      this directory must fail.
   * <li> Start of two secondary nodes in the same directory must fail.
   * <li> Import of a checkpoint must fail if primary 
   * directory contains a valid image.
   * <li> Import of the secondary image directory must succeed if primary 
   * directory does not exist.
   * <li> Recover failed checkpoint for secondary node.
   * <li> Complete failed checkpoint for secondary node.
   * </ol>
   */
  void testStartup(Configuration conf) throws IOException {
    System.out.println("Startup of the name-node in the checkpoint directory.");
    String primaryDirs = conf.get("dfs.name.dir");
    String primaryEditsDirs = conf.get("dfs.name.edits.dir");
    String checkpointDirs = conf.get("fs.checkpoint.dir");
    String checkpointEditsDirs = conf.get("fs.checkpoint.edits.dir");
    NameNode nn = startNameNode(conf, checkpointDirs, checkpointEditsDirs,
                                 StartupOption.REGULAR);

    // Starting secondary node in the same directory as the primary
    System.out.println("Startup of secondary in the same dir as the primary.");
    SecondaryNameNode secondary = null;
    try {
      secondary = startSecondaryNameNode(conf);
      assertFalse(secondary.getFSImage().storage.isLockSupported(0));
      secondary.shutdown();
    } catch (IOException e) { // expected to fail
      assertTrue(secondary == null);
    }
    nn.stop(); nn = null;

    // Starting primary node in the same directory as the secondary
    System.out.println("Startup of primary in the same dir as the secondary.");
    // secondary won't start without primary
    nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                        StartupOption.REGULAR);
    boolean succeed = false;
    do {
      try {
        secondary = startSecondaryNameNode(conf);
        succeed = true;
      } catch(IOException ie) { // keep trying
        System.out.println("Try again: " + ie.getLocalizedMessage());
      }
    } while(!succeed);
    nn.stop(); nn = null;
    try {
      nn = startNameNode(conf, checkpointDirs, checkpointEditsDirs,
                          StartupOption.REGULAR);
      assertFalse(nn.getFSImage().storage.isLockSupported(0));
      nn.stop(); nn = null;
    } catch (IOException e) { // expected to fail
      assertTrue(nn == null);
    }

    // Try another secondary in the same directory
    System.out.println("Startup of two secondaries in the same dir.");
    // secondary won't start without primary
    nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                        StartupOption.REGULAR);
    SecondaryNameNode secondary2 = null;
    try {
      secondary2 = startSecondaryNameNode(conf);
      assertFalse(secondary2.getFSImage().storage.isLockSupported(0));
      secondary2.shutdown();
    } catch (IOException e) { // expected to fail
      assertTrue(secondary2 == null);
    }
    nn.stop(); nn = null;
    secondary.shutdown();

    // Import a checkpoint with existing primary image.
    System.out.println("Import a checkpoint with existing primary image.");
    try {
      nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                          StartupOption.IMPORT);
      assertTrue(false);
    } catch (IOException e) { // expected to fail
      assertTrue(nn == null);
    }
    
    // Remove current image and import a checkpoint.
    System.out.println("Import a checkpoint with existing primary image.");
    List<File> nameDirs = (List<File>)DFSTestUtil.getFileStorageDirs(
        NNStorageConfiguration.getNamespaceDirs(conf));
    List<File> nameEditsDirs = (List<File>)DFSTestUtil.getFileStorageDirs(
        NNStorageConfiguration.getNamespaceEditsDirs(conf));
    long fsimageLength = new File(new File(nameDirs.get(0), "current"), 
                                        NameNodeFile.IMAGE.getName()).length();
    for(File dir : nameDirs) {
      if(dir.exists())
        if(!(FileUtil.fullyDelete(dir)))
          throw new IOException("Cannot remove directory: " + dir);
      if (!dir.mkdirs())
        throw new IOException("Cannot create directory " + dir);
    }

    for(File dir : nameEditsDirs) {
      if(dir.exists())
        if(!(FileUtil.fullyDelete(dir)))
          throw new IOException("Cannot remove directory: " + dir);
      if (!dir.mkdirs())
        throw new IOException("Cannot create directory " + dir);
    }
    
    nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                        StartupOption.IMPORT);
    // Verify that image file sizes did not change.
    FSImage image = nn.getFSImage();
    for (Iterator<StorageDirectory> it = 
            image.storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      assertTrue(NNStorage.getStorageFile(it.next(), 
                          NameNodeFile.IMAGE).length() == fsimageLength);
    }
    nn.stop();

    // recover failed checkpoint
    nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                        StartupOption.REGULAR);
    Collection<File> secondaryDirs = DFSTestUtil.getFileStorageDirs(
        NNStorageConfiguration.getCheckpointDirs(conf, null));
    for(File dir : secondaryDirs) {
      Storage.rename(new File(dir, "current"), 
                     new File(dir, "lastcheckpoint.tmp"));
    }
    secondary = startSecondaryNameNode(conf);
    secondary.shutdown();
    for(File dir : secondaryDirs) {
      assertTrue(new File(dir, "current").exists()); 
      assertFalse(new File(dir, "lastcheckpoint.tmp").exists());
    }
    
    // complete failed checkpoint
    for(File dir : secondaryDirs) {
      Storage.rename(new File(dir, "previous.checkpoint"), 
                     new File(dir, "lastcheckpoint.tmp"));
    }
    secondary = startSecondaryNameNode(conf);
    secondary.shutdown();
    for(File dir : secondaryDirs) {
      assertTrue(new File(dir, "current").exists()); 
      assertTrue(new File(dir, "previous.checkpoint").exists()); 
      assertFalse(new File(dir, "lastcheckpoint.tmp").exists());
    }
    nn.stop(); nn = null;
    
    // Check that everything starts ok now.
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
    cluster.waitActive();
    cluster.shutdown();
  }

  NameNode startNameNode( Configuration conf,
                          String imageDirs,
                          String editsDirs,
                          StartupOption start) throws IOException {
    conf.set("fs.default.name", "hdfs://localhost:0");
    conf.set("dfs.http.address", "0.0.0.0:0");  
    conf.set("dfs.name.dir", imageDirs);
    conf.set("dfs.name.edits.dir", editsDirs);
    String[] args = new String[]{start.getName()};
    NameNode nn = NameNode.createNameNode(args, conf);
    assertTrue(nn.isInSafeMode());
    return nn;
  }

  SecondaryNameNode startSecondaryNameNode(Configuration conf
                                          ) throws IOException {
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    return new SecondaryNameNode(conf);
  }

  /**
   * Tests checkpoint in HDFS.
   */
  public void testCheckpoint() throws IOException {
    Path file1 = new Path("checkpoint.dat");
    Path file2 = new Path("checkpoint2.dat");
    Collection<File> namedirs = null;

    Configuration conf = new Configuration();
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    replication = (short)conf.getInt("dfs.replication", 3);  
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();

    try {
      //
      // verify that 'format' really blew away all pre-existing files
      //
      assertTrue(!fileSys.exists(file1));
      assertTrue(!fileSys.exists(file2));
      namedirs = cluster.getNameDirs();

      //
      // Create file1
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);

      //
      // Take a checkpoint
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file1 still exist.
    //
    cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    try {
      // check that file1 still exists
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);

      // create new file file2
      writeFile(fileSys, file2, replication);
      checkFile(fileSys, file2, replication);

      //
      // Take a checkpoint
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file2 exists and
    // file1 does not exist.
    //
    cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
    cluster.waitActive();
    fileSys = cluster.getFileSystem();

    assertTrue(!fileSys.exists(file1));

    try {
      // verify that file2 exists
      checkFile(fileSys, file2, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    // file2 is left behind.
    testSecondaryNamenodeError1(conf);
    testSecondaryNamenodeError2(conf);
    testSecondaryNamenodeError3(conf);
    testNamedirError(conf, namedirs);
    testSecondaryFailsToReturnImage(conf);
    testStartup(conf);
  }

  /**
   * Tests save namepsace.
   */
  public void testSaveNamespace() throws IOException {
    MiniDFSCluster cluster = null;
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
      cluster.waitActive();
      fs = (DistributedFileSystem)(cluster.getFileSystem());

      // Saving image without safe mode should fail
      DFSAdmin admin = new DFSAdmin(conf);
      String[] args = new String[]{"-saveNamespace"};
      try {
        admin.run(args);
      } catch(IOException eIO) {
        assertTrue(eIO.getLocalizedMessage().contains("Safe mode should be turned ON"));
      } catch(Exception e) {
        throw new IOException(e);
      }
      // create new file
      Path file = new Path("namespace.dat");
      writeFile(fs, file, replication);
      checkFile(fs, file, replication);
      // verify that the edits file is NOT empty
      Collection<File> editsDirs = cluster.getNameEditsDirs();
      for(File ed : editsDirs) {
        assertTrue(new File(ed, "current/edits").length() > Integer.SIZE/Byte.SIZE);
      }

      // Saving image in safe mode should succeed
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        admin.run(args);
      } catch(Exception e) {
        throw new IOException(e);
      }
      // verify that the edits file is empty
      for(File ed : editsDirs) {
        assertTrue(new File(ed, "current/edits").length() == Integer.SIZE/Byte.SIZE);
      }
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      // Saving image with -force option
      Path filenew = new Path("namespacenew.dat"); // create new file
      writeFile(fs, filenew, replication);
      // verify that the edits file is NOT empty
      editsDirs = cluster.getNameEditsDirs();
      for(File ed : editsDirs) {
        assertTrue(new File(ed, "current/edits").length() > Integer.SIZE/Byte.SIZE);
      }
      admin = new DFSAdmin(conf);
      args = new String[]{"-saveNamespace", "force"};
      try {
        admin.run(args);
      } catch (Exception e) {
        throw new IOException(e);
      }

      // restart cluster and verify file exists
      cluster.shutdown();
      cluster = null;

      cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
      cluster.waitActive();
      fs = (DistributedFileSystem)(cluster.getFileSystem());
      checkFile(fs, file, replication);
    } finally {
      if(fs != null) fs.close();
      if(cluster!= null) cluster.shutdown();
    }
  }
  
  /**
   * Test that the primary NN will not serve any files to a 2NN who doesn't
   * share its namespace ID, and also will not accept any files from one.
   */
  public void testNamespaceVerifiedOnFileTransfer() throws Exception {
    MiniDFSCluster cluster = null;
    
    Configuration conf = new Configuration();
    try {
      cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
      cluster.waitActive();
      
      NameNode nn = cluster.getNameNode();
      String fsName = NameNode.getHostPortString(
          cluster.getNameNode().getHttpAddress());

      // Make a finalized log on the server side. 
      nn.rollEditLog();      
      NNStorage dstStorage = Mockito.mock(NNStorage.class);
      Collection<URI> dirs = new ArrayList<URI>();
      dirs.add(new URI("file:/tmp/dir"));     
      dstStorage.setStorageDirectories(dirs, dirs);
      Mockito.doReturn(new File[] { new File("/wont-be-written")})
        .when(dstStorage).getFiles(
            Mockito.<NameNodeDirType>anyObject(), Mockito.anyString());
      Mockito.doReturn(new StorageInfo(1, 1, 1).toColonSeparatedString())
        .when(dstStorage).toColonSeparatedString();
      FSImage dstImage = Mockito.mock(FSImage.class);
      dstImage.storage = dstStorage;

      File[] dstFiles = new File[1];
      dstFiles[0] = new File("/tmp/temp");
      
      Mockito.doReturn(new File[] { new File("/wont-be-written")})
      .when(dstImage).getEditsFiles();
      Mockito.doReturn(new File[] { new File("/wont-be-written")})
      .when(dstImage).getEditsNewFiles();

      try {
        TransferFsImage.downloadImageToStorage(fsName, 0, dstImage, false, dstFiles);
        fail("Storage info was not verified");
      } catch (IOException ioe) {
        String msg = StringUtils.stringifyException(ioe);
        assertTrue(msg, msg.contains("but the secondary expected"));
      }

      try {
        TransferFsImage.downloadEditsToStorage(fsName, new RemoteEditLog(), dstImage, false);
        fail("Storage info was not verified");
      } catch (IOException ioe) {
        String msg = StringUtils.stringifyException(ioe);
        assertTrue(msg, msg.contains("but the secondary expected"));
      }

      try {
        InetSocketAddress fakeAddr = new InetSocketAddress(1);
        TransferFsImage.uploadImageFromStorage(fsName, fakeAddr.getHostName(), 
            fakeAddr.getPort(), dstImage.storage, 0);
        fail("Storage info was not verified");
      } catch (IOException ioe) {
        String msg = StringUtils.stringifyException(ioe);
        assertTrue(msg, msg.contains("but the secondary expected"));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }  
  }
  
  class TestCheckpointInjectionHandler extends InjectionHandler {

    private InjectionEvent simulationPoint = null;
    
    void setSimulationPoint(InjectionEvent p) {
      simulationPoint = p;
    }
    
    void clearHandler() {
      simulationPoint = null;
    }

    protected void _processEventIO(InjectionEvent event, Object... args)
        throws IOException {
      if (event == simulationPoint) {
          throw new IOException("Simulating failure " + event);
      }
    }
    
    protected boolean _falseCondition(InjectionEvent event, Object... args) {
      if (event == simulationPoint){
        return true;
      }
      return false;
    }
  }
}
