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

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.FastCopy;
import org.apache.hadoop.hdfs.tools.FastCopy.FastFileCopyRequest;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.FastCopy.FastCopyFileStatus;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.AfterClass;

import static org.junit.Assert.*;

public class FastCopySetupUtil {

  private static final Random random = new Random();
  protected static Configuration conf;
  protected static Configuration remoteConf;
  private static MiniDFSCluster cluster;
  protected static DistributedFileSystem fs;
  private static MiniDFSCluster remoteCluster;
  private static DistributedFileSystem remoteFs;
  private static boolean pass = true;
  private static RWThread rwThread;
  public static final int FILESIZE = 1024 * 5; // 5 KB
  private static Map<Integer, DataNode> dnMap = new HashMap<Integer, DataNode>();

  private static Log LOG = LogFactory.getLog(FastCopySetupUtil.class);
  public static final int BLOCK_SIZE = 1024;
  private static final byte[] buffer = new byte[BLOCK_SIZE];
  public static final int TMPFILESIZE = 2048;
  private static final byte[] fileBuffer = new byte[TMPFILESIZE];
  public static final int BYTES_PER_CHECKSUM = 512;
  public static final int COPIES = 5;
  private static String confFile = "build/test/extraconf/core-site.xml";
  private static final int softLeasePeriod = 3 * 1000; // 3 sec
  private static final int hardLeasePeriod = 5 * 1000; // 5 sec

  public static void setUpClass() throws Exception {
    // Require the complete file to be replicated before we return in unit
    // tests.
    setConf("dfs.replication.min", 3);

    // Lower the pending replication timeout to make sure if any of our blocks
    // timeout the unit test catches it.
    setConf("dfs.replication.pending.timeout.sec", 60);

    // Make sure we get multiple blocks.
    setConf("dfs.block.size", BLOCK_SIZE);
    setConf("io.bytes.per.checksum", BYTES_PER_CHECKSUM);

    // Set low soft and hard lease period.
    setConf(FSConstants.DFS_HARD_LEASE_KEY, hardLeasePeriod);
    setConf(FSConstants.DFS_SOFT_LEASE_KEY, softLeasePeriod);

    System.setProperty("test.build.data", "build/test/data1");
    cluster = new MiniDFSCluster(conf, 6, new String[] { "/r1", "/r2",
        "/r1", "/r2", "/r1", "/r2" }, null, true, true);
    for (DataNode dn : cluster.getDataNodes()) {
      dnMap.put(dn.getSelfAddr().getPort(), dn);
    }

    // Writing conf to disk so that the FastCopy tool picks it up.
    FileOutputStream out = new FileOutputStream(confFile);
    conf.writeXml(out);
    fs = (DistributedFileSystem) cluster.getFileSystem();

    System.setProperty("test.build.data", "build/test/data2");
    remoteCluster = new MiniDFSCluster(remoteConf, 6, new String[] { "/r1", "/r2", "/r1", "/r2",
        "/r1", "/r2" }, null, true, true);
    for (DataNode dn : remoteCluster.getDataNodes()) {
      dnMap.put(dn.getSelfAddr().getPort(), dn);
    }

    remoteFs = (DistributedFileSystem) remoteCluster.getFileSystem();
    random.nextBytes(fileBuffer);
    rwThread = new RWThread();
    rwThread.start();
  }

  private static void setConf(String name, int value) {
    conf.setInt(name, value);
    remoteConf.setInt(name, value);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    rwThread.stopRW();
    rwThread.join();
    remoteFs.close();
    remoteCluster.shutdown();
    fs.close();
    cluster.shutdown();
    // Remove the extra conf file.
    new File(confFile).delete();
  }

  private static class RWThread extends Thread {
    private boolean flag = true;
    private byte[] tmpBuffer = new byte[TMPFILESIZE];

    public void run() {
      while (flag) {
        try {
          // Make sure we have no pendingReplicationBlocks
          pass = (0 == cluster.getNameNode().namesystem
              .getPendingReplicationBlocks()) && (0 == remoteCluster.getNameNode().namesystem
              .getPendingReplicationBlocks());
          create_verify_file();
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {

          }
        } catch (IOException e) {
          pass = false;

          LOG.warn("Create Verify Failed", e);
        }
      }
    }

    public void stopRW() {
      flag = false;
    }

    public void create_verify_file() throws IOException {
      String filename = "/create_verify_file" + random.nextInt();
      Path filePath = new Path(filename);

      // Write.
      FSDataOutputStream out = fs.create(filePath, true, 4096);
      out.write(fileBuffer);
      out.close();

      // Read.
      FSDataInputStream in = fs.open(filePath, 4096);
      in.readFully(tmpBuffer);
      in.close();

      // Verify and delete.
      pass = Arrays.equals(tmpBuffer, fileBuffer);
      fs.dfs.delete(filePath.toString(), true);
    }
  }

  /**
   * Generates a file with random data.
   * 
   * @param fs
   *          the FileSystem on which to generate the file
   * 
   * @param filename
   *          the full path name of the file
   */
  protected static void generateRandomFile(FileSystem fs, String filename,
      int filesize)
      throws IOException {
    Path filePath = new Path(filename);
    OutputStream out = fs.create(filePath, true, 4096);
    int bytesWritten = 0;
    while (bytesWritten < filesize) {
      random.nextBytes(buffer);
      out.write(buffer);
      bytesWritten += buffer.length;
    }
    out.close();
  }

  public void testFastCopy(boolean hardlink) throws Exception {
    // Create a source file.
    String src = "/testFastCopySrc" + hardlink;
    generateRandomFile(fs, src, FILESIZE);
    String destination = "/testFastCopyDestination" + hardlink;
    FastCopy fastCopy = new FastCopy(conf);
    NameNode namenode = cluster.getNameNode();
    try {
      for (int i = 0; i < COPIES; i++) {
        fastCopy.copy(src, destination + i, fs, fs);
        assertTrue(verifyCopiedFile(src, destination + i, namenode, namenode,
            fs, fs, hardlink));
        verifyFileStatus(destination + i, namenode, fastCopy);
      }
    } catch (Exception e) {
      LOG.error("Fast Copy failed with exception : ", e);
      fail("Fast Copy failed");
    } finally {
      fastCopy.shutdown();
    }
    assertTrue(pass);
  }

  public void testFastCopyOldAPI(boolean hardlink) throws Exception {
    // Create a source file.
    String src = "/testFastCopySrc" + hardlink;
    generateRandomFile(fs, src, FILESIZE);
    String destination = "/testFastCopyDestination" + hardlink;
    FastCopy fastCopy = new FastCopy(conf, fs, fs);
    NameNode namenode = cluster.getNameNode();
    try {
      for (int i = 0; i < COPIES; i++) {
        fastCopy.copy(src, destination + i);
        assertTrue(verifyCopiedFile(src, destination + i, namenode, namenode,
            fs, fs, hardlink));
        verifyFileStatus(destination + i, namenode, fastCopy);
      }
    } catch (Exception e) {
      LOG.error("Fast Copy failed with exception : ", e);
      fail("Fast Copy failed");
    } finally {
      fastCopy.shutdown();
    }
    assertTrue(pass);
  }

  public void testFastCopyMultiple(boolean hardlink) throws Exception {
    // Create a source file.
    String src = "/testFastCopyMultipleSrc" + hardlink;
    generateRandomFile(fs, src, FILESIZE);
    String destination = "/testFastCopyMultipleDestination" + hardlink;
    FastCopy fastCopy = new FastCopy(conf);
    List<FastFileCopyRequest> requests = new ArrayList<FastFileCopyRequest>();
    for (int i = 0; i < COPIES; i++) {
      requests.add(new FastFileCopyRequest(src, destination + i, fs, fs));
    }
    NameNode namenode = cluster.getNameNode();
    try {
      fastCopy.copy(requests);
      for (FastFileCopyRequest r : requests) {
        assertTrue(verifyCopiedFile(r.getSrc(), r.getDestination(), namenode,
            namenode, fs, fs, hardlink));
        verifyFileStatus(r.getDestination(), namenode, fastCopy);
      }
    } catch (Exception e) {
      LOG.error("Fast Copy failed with exception : ", e);
      fail("Fast Copy failed");
    } finally {
      fastCopy.shutdown();
    }
    assertTrue(pass);
  }


  private void verifyFileStatus(String file, NameNode namenode,
      FastCopy fastCopy) throws Exception {
    LOG.info("Verifying for file : " + file);
    FastCopyFileStatus fstat = fastCopy.getFileStatus(file);
    assertNotNull(fstat);
    int totalBlocks = namenode.getBlockLocations(file, 0,
        Long.MAX_VALUE).locatedBlockCount();
    assertEquals(totalBlocks, fstat.getTotalBlocks());
    assertEquals(fstat.getTotalBlocks(), fstat.getBlocksDone());
    assertEquals(FILESIZE / BLOCK_SIZE, totalBlocks);
  }

  public void testInterFileSystemFastCopy(boolean hardlink) throws Exception {
    // Create a source file.
    setInjectionHandler();
    String src = "/testInterFileSystemFastCopySrc" + hardlink;
    generateRandomFile(fs, src, FILESIZE);
    String destination = "/testInterFileSystemFastCopyDst" + hardlink;
    FastCopy fastCopy = new FastCopy(conf);
    NameNode srcNameNode = cluster.getNameNode();
    NameNode dstNameNode = remoteCluster.getNameNode();
    try {
      for (int i = 0; i < COPIES; i++) {
        fastCopy.copy(src, destination + i, fs, remoteFs);
        assertTrue(verifyCopiedFile(src, destination + i, srcNameNode,
            dstNameNode, fs, remoteFs, hardlink));
        verifyFileStatus(destination + i, dstNameNode, fastCopy);
      }
    } catch (Exception e) {
      LOG.error("Fast Copy failed with exception : ", e);
      fail("Fast Copy failed");
    } finally {
      fastCopy.shutdown();
    }
    assertTrue(pass);
  }

  private static void setInjectionHandler() {
    InjectionHandler.set(new InjectionHandler() {
      protected boolean _trueCondition(InjectionEventI event, Object... args) {
        if (event == InjectionEvent.FSNAMESYSTEM_SKIP_LOCAL_DN_LOOKUP) {
          if (args == null) {
            return true;
          }
          if (args.length == 1) {
            if (args[0] == null || !(args[0] instanceof String)) {
              return true;
            }
            String datanodeIpAddress = (String) args[0];
            return !("127.0.0.1".equals(datanodeIpAddress));
          }
          return true;
        }
        return true;
      }
    });
  }

  public void testInterFileSystemFastCopyMultiple(boolean hardlink)
      throws Exception {
    // Create a source file.
    String src = "/testInterFileSystemFastCopy MultipleSrc" + hardlink;
    generateRandomFile(fs, src, FILESIZE);
    String destination = "/testInterFileSystemFastCopy MultipleDestination"
        + hardlink;
    FastCopy fastCopy = new FastCopy(conf);
    List<FastFileCopyRequest> requests = new ArrayList<FastFileCopyRequest>();
    for (int i = 0; i < COPIES; i++) {
      requests.add(new FastFileCopyRequest(src, destination + i, fs, remoteFs));
    }
    NameNode srcNameNode = cluster.getNameNode();
    NameNode dstNameNode = remoteCluster.getNameNode();
    try {
      fastCopy.copy(requests);
      for (FastFileCopyRequest r : requests) {
        assertTrue(verifyCopiedFile(r.getSrc(), r.getDestination(),
            srcNameNode, dstNameNode, fs, remoteFs, hardlink));
        verifyFileStatus(r.getDestination(), dstNameNode, fastCopy);
      }
    } catch (Exception e) {
      LOG.error("Fast Copy failed with exception : ", e);
      fail("Fast Copy failed");
    } finally {
      fastCopy.shutdown();
    }
    assertTrue(pass);
  }

  public void testFastCopyShellMultiple(boolean hardlink, String extraargs[])
      throws Exception {
    // Create a source file.
    String src = "/testFastCopyShellMultipleSrc" + hardlink;
    List<String> argsList = new ArrayList<String>();
    int i;
    for (i = 0; i < COPIES; i++) {
      generateRandomFile(fs, src + i, TMPFILESIZE); // Create a file
      argsList.add(fs.makeQualified(new Path(src + i)).toString());
    }
    String destination = "/testFastCopyShellMultipleDestination" + hardlink;
    fs.mkdirs(new Path(destination));
    NameNode namenode = cluster.getNameNode();

    argsList.add(fs.makeQualified(new Path(destination)).toString());
    argsList.addAll(Arrays.asList(extraargs));
    String args[] = new String[argsList.size()];
    args = argsList.toArray(args);
    try {
      FastCopy.runTool(args);
      for (i = 0; i < COPIES; i++) {
        String dstPath = destination + src + i;
        assertTrue(fs.exists(new Path(dstPath)));
        assertTrue(verifyCopiedFile(src + i, dstPath, namenode, namenode, fs,
            fs, hardlink));
      }
    } catch (Exception e) {
      LOG.error("Fast Copy failed with exception : ", e);
      fail("Fast Copy failed");
    }
    assertTrue(pass);
  }

  public void testInterFileSystemFastCopyShellMultiple(boolean hardlink,
      String extraargs[]) throws Exception {
    // Create a source file.
    String fsname = new URI(conf.get("fs.default.name")).getAuthority();
    String remoteFsname = new URI(remoteConf.get("fs.default.name"))
        .getAuthority();
    String srcFile = "/testInterFileSystemFastCopyShellMultipleSrc" + hardlink;
    String src = "hdfs://" + fsname + srcFile;
    List<String> argsList = new ArrayList<String>();
    int i;
    for (i = 0; i < COPIES; i++) {
      generateRandomFile(fs, src + i, TMPFILESIZE); // Create a file
      argsList.add(src + i);
    }
    String destDir = "/testInterFileSystemFastCopyShellMultipleDestination"
        + hardlink;
    String destination = "hdfs://" + remoteFsname + destDir;
    remoteFs.mkdirs(new Path(destination));
    NameNode srcNamenode = cluster.getNameNode();
    NameNode dstNamenode = remoteCluster.getNameNode();

    argsList.add(destination);
    argsList.addAll(Arrays.asList(extraargs));
    String args[] = new String[argsList.size()];
    args = argsList.toArray(args);
    FastCopy.runTool(args);
    for (i = 0; i < COPIES; i++) {
      String dstPath = destDir + srcFile + i;
      assertTrue(remoteFs.exists(new Path(dstPath)));
      assertTrue(verifyCopiedFile(srcFile + i, dstPath, srcNamenode,
          dstNamenode, fs, remoteFs, hardlink));
    }
    assertTrue(pass);
  }

  public void testFastCopyShellGlob(boolean hardlink, String[] files,
      String[] args, String srcPrefix, String dstPrefix, boolean isDir)
      throws Exception {
    int i;
    if (isDir) {
      String destination = args[args.length - 1];
      fs.mkdirs(new Path(destination));
    }

    NameNode namenode = cluster.getNameNode();
    try {
      FastCopy.runTool(args);
      for (i = 0; i < files.length; i++) {
        String dstPath = dstPrefix + files[i];
        String srcPath = srcPrefix + files[i];
        LOG.info("srcPath : " + srcPath + " dstPath : " + dstPath);
        assertTrue(fs.exists(new Path(dstPath)));
        assertTrue(verifyCopiedFile(srcPath, dstPath, namenode, namenode, fs,
            fs, hardlink));
      }
    } catch (Exception e) {
      LOG.error("Fast Copy failed with exception : ", e);
      throw e;
    }
    assertTrue(pass);
  }

  public static boolean compareFiles(String src, FileSystem srcFs, String dst,
      FileSystem dstFs) throws Exception {
    Path srcFilePath = new Path(src);
    Path destFilePath = new Path(dst);
    FSDataInputStream srcStream = srcFs.open(srcFilePath, 4096);
    FSDataInputStream destStream = dstFs.open(destFilePath, 4096);
    int counter = 0;
    byte[] buffer1 = new byte[4096]; // 4KB
    byte[] buffer2 = new byte[4096]; // 4KB
    while (true) {
      try {
        srcStream.readFully(buffer1);
      } catch (EOFException e) {
        System.out.println("Src file EOF reached");
        counter++;
      }
      try {
        destStream.readFully(buffer2);
      } catch (EOFException e) {
        System.out.println("Destination file EOF reached");
        counter++;
      }
      if (counter == 1) {
        System.out.println("One file larger than other");
        return false;
      } else if (counter == 2) {
        return true;
      }
      if (!Arrays.equals(buffer1, buffer2)) {
        System.out.println("Files Mismatch");
        return false;
      }
    }
  }

  public boolean verifyCopiedFile(String src, String destination,
      NameNode srcNameNode, NameNode dstNameNode, FileSystem srcFs,
      FileSystem dstFs, boolean hardlink) throws Exception {

    verifyBlockLocations(src, destination, srcNameNode, dstNameNode, hardlink);

    return compareFiles(src, srcFs, destination, dstFs);
  }

  public boolean verifyBlockLocations(String src, String destination,
      NameNode srcNameNode, NameNode dstNameNode, boolean hardlink)
      throws IOException {
    LocatedBlocksWithMetaInfo srcLocatedBlocks = 
      srcNameNode.openAndFetchMetaInfo(src, 0,Long.MAX_VALUE);
    List<LocatedBlock> srcblocks = srcLocatedBlocks.getLocatedBlocks();
    LocatedBlocksWithMetaInfo dstLocatedBlocks =
      dstNameNode.openAndFetchMetaInfo(destination, 0, Long.MAX_VALUE);
    List<LocatedBlock> dstblocks = dstLocatedBlocks.getLocatedBlocks();

    assertEquals(srcblocks.size(), dstblocks.size());

    Iterator<LocatedBlock> srcIt = srcblocks.iterator();
    Iterator<LocatedBlock> dstIt = dstblocks.iterator();
    while (srcIt.hasNext()) {
      LocatedBlock srcBlock = srcIt.next();
      LocatedBlock dstBlock = dstIt.next();
      List<DatanodeInfo> srcLocations = Arrays.asList(srcBlock.getLocations());
      List<DatanodeInfo> dstLocations = Arrays.asList(dstBlock.getLocations());
      
      System.out.println("Locations for src block : " + srcBlock.getBlock()
          + " file : " + src);
      for (DatanodeInfo info : srcLocations) {
        System.out.println("Datanode : " + info.toString() + " rack: " + info.getNetworkLocation());
      }

      System.out.println("Locations for dst block : " + dstBlock.getBlock()
          + " file : " + destination);
      for (DatanodeInfo info : dstLocations) {
        System.out.println("Datanode : " + info.toString() + " rack: " + info.getNetworkLocation());
      }

      assertEquals(srcLocations.size(), dstLocations.size());

      if (srcNameNode.getNameNodeAddress().equals(
          dstNameNode.getNameNodeAddress())) {
        // Same FS copy, verify blocks are machine local.
        assertTrue(srcLocations.containsAll(dstLocations));
        assertTrue(dstLocations.containsAll(srcLocations));
      } else {
        // Since all datanodes are on the same host in a unit test, the inter
        // filesystem copy can have blocks end up on any datanode.
        Iterator<DatanodeInfo> sit = srcLocations.iterator();
        while (sit.hasNext()) {
          DatanodeInfo srcInfo = sit.next();

          // Verify location.
          Iterator<DatanodeInfo> dit = dstLocations.iterator();
          while (dit.hasNext()) {
            DatanodeInfo dstInfo = dit.next();
            if (dstInfo.getHost().equals(srcInfo.getHost())) {
              verifyHardLinks(srcInfo, dstInfo,
                  srcLocatedBlocks.getNamespaceID(), srcBlock.getBlock(),
                  dstLocatedBlocks.getNamespaceID(), dstBlock.getBlock(),
                  hardlink);
            }
          }
        }
      }
    }
    return true;
  }

  private void verifyHardLinks(DatanodeInfo srcInfo, DatanodeInfo dstInfo,
      int srcNamespaceId, Block srcBlock, int dstNamespaceId, Block dstBlock,
      boolean hardlink) throws IOException {
    // Verify hard links.
    DataNode dnSrc = dnMap.get(srcInfo.getPort());
    File blockFileSrc = dnSrc.data.getBlockFile(srcNamespaceId, srcBlock);
    LOG.warn("Link count for : " + blockFileSrc + " is : "
        + HardLink.getLinkCount(blockFileSrc));
    if (hardlink) {
      assertTrue(HardLink.getLinkCount(blockFileSrc) > 1);
    } else {
      assertEquals(1, HardLink.getLinkCount(blockFileSrc));
    }

    DataNode dnDst = dnMap.get(dstInfo.getPort());
    File blockFileDst = dnDst.data.getBlockFile(dstNamespaceId, dstBlock);
    if (hardlink) {
      assertTrue(HardLink.getLinkCount(blockFileDst) > 1);
    } else {
      assertEquals(1, HardLink.getLinkCount(blockFileDst));
    }
  }
}
