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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Joiner;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestCheckpoint{
  
  final static Log LOG = LogFactory.getLog(TestCheckpoint.class);
  
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  static final int numDatanodes = 3;
  short replication = 3;
  
  @After
  public void tearDown() {
    File dir = new File(System.getProperty("test.build.data"));
    LOG.info("Cleanup directory: " + dir);
    try {
      FileUtil.fullyDelete(dir);
    } catch (IOException e) {
      LOG.info("Could not remove: " + dir, e);
    }
  }

  public void writeFile(FileSystem fileSys, Path name, int repl)
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
  
  
  public void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    assertTrue(fileSys.exists(name));
    int replication = fileSys.getFileStatus(name).getReplication();
    assertEquals("replication for " + name, repl, replication);
    //We should probably test for more of the file properties.    
  }
  
  public void cleanupFile(FileSystem fileSys, Path name)
    throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * Verify that namenode does not startup if one namedir is bad.
   */
  @Test
  public void testNameDirError() throws IOException {
    LOG.info("Starting testNameDirError");
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);
    
    Collection<File> nameDirs = cluster.getNameDirs(0);
    cluster.shutdown();
    cluster = null;
    
    for (File dir : nameDirs) {    
      try {
        // Simulate the mount going read-only
        dir.setWritable(false);
        cluster = new MiniDFSCluster(conf,0 ,false ,null);
        fail("NN should have failed to start with " + dir + " set unreadable");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(
            "storage directory does not exist or is not accessible",
            ioe);
      } finally {
        if (cluster != null) {
          cluster.shutdown();
          cluster = null;
        }
        dir.setWritable(true);
      }
    }
  }

  /*
   * Simulate namenode crashing after rolling edit log.
   */
  @Test
  public void testSecondaryNamenodeError1()
    throws IOException {
    Configuration conf = new Configuration();
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
    InjectionHandler.set(h);
    
    System.out.println("Starting testSecondaryNamenodeError 1");
    Path file1 = new Path("checkpointxx.dat");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, 
                                                true, null);
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
    long txid = image.storage.getMostRecentCheckpointTxId();
    for (Iterator<StorageDirectory> it = 
             image.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      assertFalse(NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid).exists());
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
  @Test
  public void testSecondaryNamenodeError2()
    throws IOException {
    Configuration conf = new Configuration();
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
    InjectionHandler.set(h);
    
    System.out.println("Starting testSecondaryNamenodeError 21");
    Path file1 = new Path("checkpointyy.dat");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, 
                                                true, null);
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
  @Test
  public void testSecondaryNamenodeError3()
    throws IOException {
    Configuration conf = new Configuration();
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
    InjectionHandler.set(h);
    
    System.out.println("Starting testSecondaryNamenodeError 31");
    Path file1 = new Path("checkpointzz.dat");
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
  @Test
  public void testSecondaryFailsToReturnImage()
    throws IOException {
    Configuration conf = new Configuration();
    TestCheckpointInjectionHandler h = new TestCheckpointInjectionHandler();
    InjectionHandler.set(h);
    
    System.out.println("Starting testSecondaryFailsToReturnImage");
    Path file1 = new Path("checkpointRI.dat");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, 
                                                true, null);
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
  @Test
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
  @Test
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
        System.out.println(StringUtils.stringifyException(e));
        assertTrue(e.getMessage().contains(exceptionSubstring));
      }
      h.clearHandler();
      secondary.shutdown(); // secondary namenode crash!

      // start new instance of secondary 
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
  @Test
  public void testStartup() throws IOException {
    
    Configuration conf = new Configuration();
    
    System.out.println("Startup of the name-node in the checkpoint directory.");
    
    
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    
    SecondaryNameNode secondary = startSecondaryNameNode(conf);
    secondary.doCheckpoint();
    secondary.shutdown();
    
    cluster.shutdown();
    
    LOG.info("Regular checkpoint DONE");
    
    String primaryDirs = conf.get("dfs.name.dir");
    String primaryEditsDirs = conf.get("dfs.name.edits.dir");
    String checkpointDirs = conf.get("fs.checkpoint.dir");
    String checkpointEditsDirs = conf.get("fs.checkpoint.edits.dir");
    
    NameNode nn = startNameNode(conf, checkpointDirs, checkpointEditsDirs,
                                 StartupOption.REGULAR);
    
    // Starting secondary node in the same directory as the primary
    System.out.println("Startup of secondary in the same dir as the primary.");
    secondary = null;
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
        if(!(FileUtil.fullyDelete(dir))) {
          throw new IOException("Cannot remove directory: " + dir);
        } else {
          LOG.info("Deleted: " + dir);
        }
      if (!dir.mkdirs())
        throw new IOException("Cannot create directory " + dir);
    }

    for(File dir : nameEditsDirs) {
      if(dir.exists())
        if(!(FileUtil.fullyDelete(dir))) {
          throw new IOException("Cannot remove directory: " + dir);
        } else {
          LOG.info("Deleted: " + dir);
        }
      if (!dir.mkdirs())
        throw new IOException("Cannot create directory " + dir);
    }
    
    LOG.info("Starting primary with name dirs: " + primaryDirs 
        + " edit dirs: " + primaryEditsDirs);
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
    
    // Check that everything starts ok now.
    nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
        StartupOption.REGULAR);
    nn.stop();
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
  @Test
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
  }

  /**
   * Tests save namepsace.
   */
  @Test
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
      Collection<File> editsDirs = cluster.getNameEditsDirs(0);
      for(File ed : editsDirs) {
        assertTrue(new File(ed, "current/"
                            + NNStorage.getInProgressEditsFileName(0))
                   .length() > Integer.SIZE/Byte.SIZE);
      }

      // Saving image in safe mode should succeed
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        admin.run(args);
      } catch(Exception e) {
        throw new IOException(e);
      }
      
      // the following steps should have happened:
      //   edits_inprogress_0 -> edits_0-6  (finalized)
      //   fsimage_6 created
      //   edits_inprogress_7 created
      //
      for(File ed : editsDirs) {
        File curDir = new File(ed, "current");
        LOG.info("Files in " + curDir + ":\n  " +
            Joiner.on("\n  ").join(curDir.list()));
        // Verify that the first edits file got finalized
        File originalEdits = new File(curDir,
                                      NNStorage.getInProgressEditsFileName(0));
        assertFalse(originalEdits.exists());
        File finalizedEdits = new File(curDir,
            NNStorage.getFinalizedEditsFileName(0,6));
        assertTrue("Finalized edits: " + finalizedEdits + " does not exist",
            finalizedEdits.exists());
        assertTrue(finalizedEdits.length() > Integer.SIZE/Byte.SIZE);

        assertTrue(new File(ed, "current/"
                       + NNStorage.getInProgressEditsFileName(7)).exists());
      }
      
      Collection<File> imageDirs = cluster.getNameDirs(0);
      for (File imageDir : imageDirs) {
        File savedImage = new File(imageDir, "current/"
                                   + NNStorage.getImageFileName(6));
        assertTrue("Should have saved image at " + savedImage,
            savedImage.exists());        
      }

      // restart cluster and verify file exists
      cluster.shutdown();
      cluster = null;

      cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
      cluster.waitActive();
      fs = (DistributedFileSystem)(cluster.getFileSystem());
      checkFile(fs, file, replication);
    } finally {
      try {
        if(fs != null) fs.close();
        if(cluster!= null) cluster.shutdown();
      } catch (Throwable t) {
        System.out.println("Failed to shutdown" + t);
      }
    }
  }
  
  /**
   * Test that the primary NN will not serve any files to a 2NN who doesn't
   * share its namespace ID, and also will not accept any files from one.
   */
  @Test
  public void testNamespaceVerifiedOnFileTransfer() throws Exception {
    MiniDFSCluster cluster = null;
    
    Configuration conf = new Configuration();
    try {
      cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
      cluster.waitActive();
      
      NameNode nn = cluster.getNameNode();
      String fsName = NetUtils.toIpPort(cluster.getNameNode().getHttpAddress());

      // Make a finalized log on the server side. 
      nn.rollEditLog();      
      final NNStorage dstStorage = Mockito.mock(NNStorage.class);
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
      Mockito.doReturn(new Iterator<StorageDirectory>() {
        boolean returned = false;
        @Override
        public boolean hasNext() {
          if (returned)
            return false;
          returned = true;
          return true;
        }
        @Override
        public StorageDirectory next() {
          return dstStorage.new StorageDirectory(new File("/tmp/dir"));
        }
        @Override
        public void remove() { }
      }).when(dstStorage).dirIterator(Mockito.<NameNodeDirType>anyObject());
      
      List<OutputStream> oss = new ArrayList<OutputStream>();
      oss.add(new ByteArrayOutputStream());
      Mockito.doReturn(oss).when(dstImage).getCheckpointImageOutputStreams(Mockito.anyLong());

      FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
      Mockito.doReturn(new ArrayList<JournalManager>()).when(fsEditLog).getNonFileJournalManagers();
      dstImage.editLog = fsEditLog;
      dstImage.imageSet = new ImageSet(dstImage, dirs, null, null);

      try {
        TransferFsImage.downloadImageToStorage(fsName, 0L, dstImage, false);
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
  
  /* Test case to test CheckpointSignature */
  @Test
  public void testCheckpointSignature() throws IOException {

    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    NameNode nn = cluster.getNameNode();

    SecondaryNameNode secondary = startSecondaryNameNode(conf);
    // prepare checkpoint image
    secondary.doCheckpoint();
    CheckpointSignature sig = nn.rollEditLog();
    // manipulate the CheckpointSignature fields
    sig.namespaceID--;

    try {
      sig.validateStorageInfo(nn.getFSImage().storage); // this should fail
      assertTrue("This test is expected to fail.", false);
    } catch (Exception ignored) {
    }

    secondary.shutdown();
    cluster.shutdown();
  }
  
  /**
   * Checks that an IOException in NNStorage.writeTransactionIdFile is handled
   * correctly (by removing the storage directory)
   * See https://issues.apache.org/jira/browse/HDFS-2011
   */
  @Test
  public void testWriteTransactionIdHandlesIOE() throws Exception {
    LOG.info("Check IOException handled correctly by writeTransactionIdFile");
    ArrayList<URI> fsImageDirs = new ArrayList<URI>();
    ArrayList<URI> editsDirs = new ArrayList<URI>();
    File filePath1 =
      new File(System.getProperty("test.build.data","/tmp"), "storageDirToCheck1");
    File filePath2 =
        new File(System.getProperty("test.build.data","/tmp"), "storageDirToCheck2");
    assertTrue("Couldn't create directory storageDirToCheck1",
               filePath1.exists() || filePath1.mkdirs());
    assertTrue("Couldn't create directory storageDirToCheck2",
              filePath2.exists() || filePath2.mkdirs());
    
    File current1 = new File(filePath1, "current");
    File current2 = new File(filePath2, "current");
    
    assertTrue("Couldn't create directory storageDirToCheck1/current",
        current1.exists() || current1.mkdirs());
    assertTrue("Couldn't create directory storageDirToCheck2/current",
        current2.exists() || current2.mkdirs());
    
    fsImageDirs.add(filePath1.toURI());
    editsDirs.add(filePath1.toURI());
    fsImageDirs.add(filePath2.toURI());
    editsDirs.add(filePath2.toURI());
    NNStorage nnStorage = new NNStorage(new Configuration(),
      fsImageDirs, editsDirs, null);
    try {
      assertTrue(
          "List of storage directories didn't have storageDirToCheck1.",
          nnStorage.getEditsDirectories().toString()
              .indexOf("storageDirToCheck1") != -1);
      assertTrue(
          "List of storage directories didn't have storageDirToCheck2.",
          nnStorage.getEditsDirectories().toString()
              .indexOf("storageDirToCheck2") != -1);
      assertTrue("List of removed storage directories wasn't empty", nnStorage
          .getRemovedStorageDirs().isEmpty());
    } finally {
      // Delete storage directory to cause IOException in writeTransactionIdFile
      FileUtil.fullyDelete(filePath1);
    }
    // Just call writeTransactionIdFile using any random number
    nnStorage.writeTransactionIdFileToStorage(1, null);
    List<StorageDirectory> listRsd = nnStorage.getRemovedStorageDirs();
    assertTrue("Removed directory wasn't what was expected",
               listRsd.size() > 0 && listRsd.get(listRsd.size() - 1).getRoot().
               toString().indexOf("storageDirToCheck1") != -1);
  }
  
  class TestCheckpointInjectionHandler extends InjectionHandler {

    private InjectionEvent simulationPoint = null;
    
    void setSimulationPoint(InjectionEvent p) {
      simulationPoint = p;
    }
    
    void clearHandler() {
      simulationPoint = null;
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == simulationPoint) {
          throw new IOException("Simulating failure " + event);
      }
    }
    
    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      if (event == simulationPoint){
        return true;
      }
      return false;
    }
  }
}
