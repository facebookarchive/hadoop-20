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

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.zip.CRC32;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.BlockInlineChecksumWriter;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.util.GSet;
import org.apache.hadoop.io.IOUtils;

/**
 */
public class DFSTestUtil extends TestCase {
  
  private static final Log LOG = LogFactory.getLog(DFSTestUtil.class);
  
  private static Random gen = new Random();
  private static String[] dirNames = {
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
  };
  
  private int maxLevels;// = 3;
  private int maxSize;// = 8*1024;
  private int nFiles;
  private MyFile[] files;
  
  /** Creates a new instance of DFSTestUtil
   *
   * @param testName Name of the test from where this utility is used
   * @param nFiles Number of files to be created
   * @param maxLevels Maximum number of directory levels
   * @param maxSize Maximum size for file
   */
  public DFSTestUtil(String testName, int nFiles, int maxLevels, int maxSize) {
    this.nFiles = nFiles;
    this.maxLevels = maxLevels;
    this.maxSize = maxSize;
  }
  
  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private class MyFile {
    
    private String name = "";
    private int size;
    private long seed;
    
    MyFile() {
      int nLevels = gen.nextInt(maxLevels);
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        name = sb.toString();
      }
      long fidx = -1;
      while (fidx < 0) { fidx = gen.nextLong(); }
      name = name + Long.toString(fidx);
      size = gen.nextInt(maxSize) + 1;
      seed = gen.nextLong();
    }
    
    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }

  public void createFiles(FileSystem fs, String topdir) throws IOException {
    createFiles(fs, topdir, (short)3);
  }
  
  /** create nFiles with random names and directory hierarchies
   *  with random (but reproducible) data in them.
   */
  public void createFiles(FileSystem fs, String topdir,
                          short replicationFactor) throws IOException {
    files = new MyFile[nFiles];
    
    for (int idx = 0; idx < nFiles; idx++) {
      files[idx] = new MyFile();
    }
    
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < nFiles; idx++) {
      createFile(fs, new Path(root, files[idx].getName()), files[idx].getSize(),
          replicationFactor, files[idx].getSeed());
    }
  }
  
  public static long createFile(FileSystem fs, Path fileName, long fileLen,
      short replFactor, long seed, InetSocketAddress[] favoredNodes)
          throws IOException {
    return createFile(fs, fileName, fileLen, replFactor, seed, favoredNodes,
        fs.getDefaultBlockSize());
  }
  
  public static long createFile(FileSystem fs, Path fileName, long fileLen,
      short replFactor, long seed, InetSocketAddress[] favoredNodes, long blockSize) 
          throws IOException {
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " + 
                            fileName.getParent().toString());
    }
    CRC32 crc = new CRC32();
    FSDataOutputStream out = null;
    try {
      if (favoredNodes == null && blockSize == fs.getDefaultBlockSize()){
        out = fs.create(fileName, replFactor);
      } else {
        out = ((DistributedFileSystem) fs).create(fileName, FsPermission.getDefault(), true, fs
            .getConf().getInt("io.file.buffer.size", 4096), replFactor, blockSize,
            fs.getConf().getInt("io.bytes.per.checksum", FSConstants.DEFAULT_BYTES_PER_CHECKSUM),
            null, favoredNodes);
      }

      byte[] toWrite = new byte[1024];
      Random rb = new Random(seed);
      long bytesToWrite = fileLen;
      while (bytesToWrite>0) {
        rb.nextBytes(toWrite);
        int bytesToWriteNext = (1024<bytesToWrite)?1024:(int)bytesToWrite;

        out.write(toWrite, 0, bytesToWriteNext);
        crc.update(toWrite, 0, bytesToWriteNext);
        bytesToWrite -= bytesToWriteNext;
      }
      out.close();
      out = null;
      return crc.getValue();
    } finally {
      IOUtils.closeStream(out);
    }
  }
  
  public static long createFile(FileSystem fs, Path fileName, long fileLen,
      short replFactor, long seed) throws IOException {
    return createFile(fs, fileName, fileLen, replFactor, seed, null);
  }

  /** check if the files have been copied correctly. */
  public boolean checkFiles(FileSystem fs, String topdir) throws IOException {
    
    //Configuration conf = new Configuration();
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < nFiles; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      FSDataInputStream in = fs.open(fPath);
      byte[] toRead = new byte[files[idx].getSize()];
      byte[] toCompare = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toCompare);
      in.readFully(0, toRead);
      in.close();
      for (int i = 0; i < toRead.length; i++) {
        if (toRead[i] != toCompare[i]) {
          return false;
        }
      }
      toRead = null;
      toCompare = null;
    }
    
    return true;
  }

  void setReplication(FileSystem fs, String topdir, short value) 
                                              throws IOException {
    Path root = new Path(topdir);
    for (int idx = 0; idx < nFiles; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      fs.setReplication(fPath, value);
    }
  }

  // waits for the replication factor of all files to reach the
  // specified target
  //
  public void waitReplication(FileSystem fs, String topdir, short value) 
                                              throws IOException {
    Path root = new Path(topdir);

    /** wait for the replication factor to settle down */
    for (int idx = 0; idx < nFiles; idx++) {
      waitReplication(fs, new Path(root, files[idx].getName()), value);
    }
  }

  /** return list of filenames created as part of createFiles */
  public String[] getFileNames(String topDir) {
    if (nFiles == 0)
      return new String[]{};
    else {
      String[] fileNames =  new String[nFiles];
      for (int idx=0; idx < nFiles; idx++) {
        fileNames[idx] = topDir + "/" + files[idx].getName();
      }
      return fileNames;
    }
  }
  
  /** wait for the file's replication to be done */
  public static void waitReplication(FileSystem fs, Path fileName, 
      short replFactor)  throws IOException {
    boolean good;
    do {
      good = true;
      BlockLocation locs[] = fs.getFileBlockLocations(
        fs.getFileStatus(fileName), 0, Long.MAX_VALUE);
      for (int j = 0; j < locs.length; j++) {
        String[] loc = locs[j].getHosts();
        if (loc.length != replFactor) {
          System.out.println("File " + fileName + " has replication factor " +
              loc.length);
          good = false;
          try {
            System.out.println("Waiting for replication factor to drain");
            Thread.sleep(100);
          } catch (InterruptedException e) {} 
          break;
        }
      }
    } while(!good);
  }
  
  /** delete directory and everything underneath it.*/
  public void cleanup(FileSystem fs, String topdir) throws IOException {
    Path root = new Path(topdir);
    fs.delete(root, true);
    files = null;
  }
  
  public static Block getFirstBlock(FileSystem fs, Path path) throws IOException {
    DFSDataInputStream in = 
      (DFSDataInputStream) ((DistributedFileSystem)fs).open(path);
    in.readByte();
    return in.getCurrentBlock();
  }  

  static void setLogLevel2All(org.apache.commons.logging.Log log) {
    ((org.apache.commons.logging.impl.Log4JLogger)log
        ).getLogger().setLevel(org.apache.log4j.Level.ALL);
  }
  
  //
  // validates that file matches the crc.
  //
  public static boolean validateFile(FileSystem fileSys, Path name, long length,
                                  long crc) 
    throws IOException {

    long numRead = 0;
    CRC32 newcrc = new CRC32();
    FSDataInputStream stm = fileSys.open(name);
    final byte[] b = new byte[4192];
    int num = 0;
    while (num >= 0) {
      num = stm.read(b);
      if (num < 0) {
        break;
      }
      numRead += num;
      newcrc.update(b, 0, num);
    }
    stm.close();

    if (numRead != length) {
      LOG.info("Number of bytes read " + numRead +
               " does not match file size " + length);
      return false;
    }

    LOG.info(" Newcrc " + newcrc.getValue() + " old crc " + crc);
    if (newcrc.getValue() != crc) {
      LOG.info("CRC mismatch of file " + name + ": " +
               newcrc.getValue() + " vs. " + crc);
      return false;
    }
    return true;
  }

  public static String readFile(File f) throws IOException {
    StringBuilder b = new StringBuilder();
    BufferedReader in = new BufferedReader(new FileReader(f));
    for(int c; (c = in.read()) != -1; b.append((char)c));
    in.close();      
    return b.toString();
  }
  
  public static byte[] loadFile(String filename) throws IOException {
    File file = new File(filename);
    DataInputStream in = new DataInputStream(new FileInputStream(file));
    byte[] content = new byte[(int)file.length()];
    in.readFully(content);
    return content;
  }

  // Returns url content as string.
  public static String urlGet(URL url) throws IOException {
    URLConnection conn = url.openConnection();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    return out.toString();
  }

  public static byte[] generateSequentialBytes(int start, int length) {
    byte[] result = new byte[length];

    for (int i = 0; i < length; i++) {
      result[i] = (byte)((start + i) % 127);
    }
    
    return result;
  }  
  
  static byte[] inBytes = "something".getBytes();

  public static void creatFileAndWriteSomething(DistributedFileSystem dfs,
      String filestr, short replication) throws IOException {
    Path filepath = new Path(filestr);
    
    FSDataOutputStream out = dfs.create(filepath, replication);
    try {
      out.write(inBytes);
    } finally {
      out.close();
    }
  }
  
  public static BlockPathInfo getBlockPathInfo(String filename,
      MiniDFSCluster miniCluster, DFSClient dfsclient) throws IOException {
    LocatedBlocks locations = dfsclient.namenode.getBlockLocations(filename, 0,
        Long.MAX_VALUE);
    assertEquals(1, locations.locatedBlockCount());
    LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
    DataNode datanode = miniCluster.getDataNode(locatedblock.getLocations()[0]
        .getIpcPort());
    assertTrue(datanode != null);

    Block lastblock = locatedblock.getBlock();
    DataNode.LOG.info("newblocks=" + lastblock);

    return datanode.getBlockPathInfo(lastblock);
  }
  

  /**
   * Recursively delete the contents of dir, but not the dir itself. Deletes any
   * subdirectory which is empty after its files are deleted.
   */
  public static int deleteContents(File dir) {
    return deleteContents(dir, true);
  }

  /**
   * Recursively delete the contents of dir, but not the dir itself. If
   * deleteEmptyDirs is true, this deletes any subdirectory which is empty after
   * its files are deleted.
   */
  public static int deleteContents(File dir, boolean deleteEmptyDirs) {
    if (null == dir) {
      throw new IllegalArgumentException("null dir");
    }
    if ((!dir.exists()) || (!dir.canWrite())) {
      return 0;
    }
    if (!dir.isDirectory()) {
      dir.delete();
      return 1;
    }
    String[] fromFiles = dir.list();
    int result = 0;
    for (int i = 0; i < fromFiles.length; i++) {
      String string = fromFiles[i];
      File file = new File(dir, string);
      if (file.isDirectory()) {
        result += deleteContents(file, deleteEmptyDirs);
        if (deleteEmptyDirs && (0 == file.list().length)) {
          file.delete();
        }
      } else {
        file.delete();
        result++;
      }
    }
    return result;
  }
  
  // extracts only the file:// entries from the list of given URIS
  public static Collection<File> getFileStorageDirs(Collection<URI> uris) {
    ArrayList<File> directories = new ArrayList<File>();
    for (URI uri : uris) {
      if (uri.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) == 0) {
        directories.add(new File(uri.getPath()));
      }
    }
    return directories;
  }
  
  // sleep for one second
  public static void waitSecond() {
    try {
      LOG.info("Waiting.....");
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      return;
    }
  }
  
  // sleep for n seconds
  public static void waitNSecond(int n) {
    try {
      LOG.info("Waiting.....");
      Thread.sleep(n * 1000);
    } catch (InterruptedException e) {
      return;
    }
  }
  
  // sleep for n milliseconds
  public static void waitNMilliSecond(int n) {
    try {
      LOG.info("Waiting.....");
      Thread.sleep(n);
    } catch (InterruptedException e) {
      return;
    }
  }
  
  /** Make sure two inode map are the equal */
  public static void assertInodemapEquals(GSet<INode, INode> o1, GSet<INode, INode> o2) {
    Iterator<INode> itr1 = o1.iterator();
    Iterator<INode> itr2 = o2.iterator();
    while (itr1.hasNext()) {
      assertTrue(itr2.hasNext());
      assertEquals(itr1.next(), itr2.next());
    }
    assertFalse(itr2.hasNext());
  }
  
  public static void corruptBlock(Block block, MiniDFSCluster dfs) throws IOException {
    boolean corrupted = false;
    for (int i = 0; i < dfs.getNumDataNodes(); i++) {
      corrupted |= corruptReplica(block, i, dfs);
    }
    assertTrue("could not corrupt block", corrupted);
  }
  
  public static boolean corruptReplica(Block block, int replica, MiniDFSCluster cluster) throws IOException {
    Random random = new Random();
    boolean corrupted = false;
    for (int i=replica*2; i<replica*2+2; i++) {
      File blockFile = new File(cluster.getBlockDirectory("data" + (i+1)), block.getBlockName());
      if (blockFile.exists()) {
        corruptFile(blockFile, random);
        corrupted = true;
        continue;
      }
      File blockFileInlineChecksum = new File(cluster.getBlockDirectory("data"
          + (i + 1)), BlockInlineChecksumWriter.getInlineChecksumFileName(
          block, FSConstants.CHECKSUM_TYPE, cluster.conf.getInt(
              "io.bytes.per.checksum", FSConstants.DEFAULT_BYTES_PER_CHECKSUM)));
      if (blockFileInlineChecksum.exists()) {
        corruptFile(blockFileInlineChecksum, random);
        corrupted = true;
        continue;
      }
    }
    return corrupted;
  }
  
  public static void corruptFile(File file, Random random) throws IOException {
    // Corrupt replica by writing random bytes into replica
    RandomAccessFile raFile = new RandomAccessFile(file, "rw");
    FileChannel channel = raFile.getChannel();
    String badString = "BADBAD";
    int rand = random.nextInt((int)channel.size()/2);
    raFile.seek(rand);
    raFile.write(badString.getBytes());
    raFile.close();    
  }
}
