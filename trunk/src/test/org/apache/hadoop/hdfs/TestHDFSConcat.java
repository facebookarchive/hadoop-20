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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHDFSConcat {
  public static final Log LOG = LogFactory.getLog(TestHDFSConcat.class);

  private static final short REPL_FACTOR = 2;
  
  private MiniDFSCluster cluster;
  private NameNode nn;
  private DistributedFileSystem dfs;

  private static long blockSize = 512;

  
  private static Configuration conf;

  static {
    conf = new Configuration();
    conf.setLong("dfs.block.size", blockSize);
  }
  
  @Before
  public void startUpCluster() throws IOException {
    cluster = new MiniDFSCluster(conf, REPL_FACTOR, true, null);
    assertNotNull("Failed Cluster Creation", cluster);
    cluster.waitClusterUp();
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    assertNotNull("Failed to get FileSystem", dfs);
    nn = cluster.getNameNode();
    assertNotNull("Failed to get NameNode", nn);
  }

  @After
  public void shutDownCluster() throws IOException {
    if(dfs != null) {
      dfs.close();
    }
    if(cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
    }
  }
  
  private void runCommand(DFSAdmin admin, String args[], boolean expectEror)
  throws Exception {
    int val = admin.run(args);
    if (expectEror) {
      assertEquals(val, -1);
    } else {
      assertTrue(val>=0);
    }
  }

  
  /**
   * Concatenates 10 files into one
   * Verifies the final size, deletion of the file, number of blocks
   * @throws IOException
   */
  @Test
  public void testConcatRestricted() throws Exception{
    runTestConcat(true, false);
  }
  
  /**
   * Concatenates 10 files into one
   * Verifies the final size, deletion of the file, number of blocks
   * @throws IOException
   */
  @Test
  public void testConcatUnrestricted() throws Exception{
    runTestConcat(false, false);
  }
  
  /**
   * Violate block sizes for restricted
   * @throws IOException
   */
  @Test
  public void testConcatRestrictedViolated() throws Exception{
    try {
      runTestConcat(true, true);
      fail("Restricted concatenation requires blocks of the same size and should fail otherwise");
    } catch (Exception e) {
      // TODO: handle exception
    }
  } 
  
  
  private void runTestConcat(boolean restricted, boolean violate) throws IOException, InterruptedException {
    LOG.info("RUNNING testConcat restricted= " + restricted + ", violate= "
        + violate);
    final int numFiles = 10;
    long fileLen = blockSize*3;
    FileStatus fStatus;
    FSDataInputStream stm;
    
    String trg = new String("/trg");
    Path trgPath = new Path(trg);
    DFSTestUtil.createFile(dfs, trgPath, fileLen, REPL_FACTOR, 1);
    fStatus  = nn.getFileInfo(trg);
    long trgLen = fStatus.getLen();
    long trgBlocks = nn.getBlockLocations(trg, 0, trgLen).locatedBlockCount();
       
    Path [] files = new Path[numFiles];
    long initLens[] = new long [numFiles];
    Random rand = new Random();
    for (int i = 0; i < numFiles; i++) {
      initLens[i] = fileLen
          + ((restricted && !violate) ? 0 : rand.nextInt((int) blockSize));
    }
    
    byte [] [] bytes = new byte [numFiles+1][];
    bytes[0] = new byte[(int)trgLen];
    for(int i=1; i<=numFiles; i++){
      bytes[i] = new byte[(int)initLens[i-1]];
    }
    
    stm = dfs.open(trgPath);
    stm.readFully(0, bytes[0]);
    stm.close();
    
    LocatedBlocks [] lblocks = new LocatedBlocks[numFiles];
    long [] lens = new long [numFiles];
    
    
    int i = 0;
    for(i=0; i<files.length; i++) {
      files[i] = new Path("/file"+i);
      Path path = files[i];
      DFSTestUtil.createFile(dfs, path, initLens[i], REPL_FACTOR, 1);
    
      fStatus = nn.getFileInfo(path.toUri().getPath());
      lens[i] = fStatus.getLen();
      assertEquals(initLens[i], lens[i]); // file of the same length.
      
      lblocks[i] = nn.getBlockLocations(path.toUri().getPath(), 0, lens[i]);
      
      //read the file
      stm = dfs.open(path);
      stm.readFully(0, bytes[i+1]);
      stm.close();
    }
    
    // check permissions -try the operation with the "wrong" user
    //final UserGroupInformation user1 = UserGroupInformation.createUserForTesting(
    //    "theDoctor", new String[] { "tardis" });
    //DistributedFileSystem hdfs = 
    //  (DistributedFileSystem)DFSTestUtil.getFileSystemAs(user1, conf);
    //try {
     // hdfs.concat(trgPath, files);
    //  fail("Permission exception expected");
    //} catch (IOException ie) {
    //  System.out.println("Got expected exception for permissions:"
    //      + ie.getLocalizedMessage());
      // expected
    //}
     System.out.println("Skipping concat with file permissions");
    
    // check count update
    ContentSummary cBefore = dfs.getContentSummary(trgPath.getParent());
    
    // now concatenate
    dfs.concat(trgPath, files, restricted);
    
    // verify  count
    ContentSummary cAfter = dfs.getContentSummary(trgPath.getParent());
    assertEquals(cBefore.getFileCount(), cAfter.getFileCount()+files.length);
    
    // verify other stuff
    long totalLen = trgLen;
    long totalBlocks = trgBlocks;
    for(i=0; i<files.length; i++) {
      totalLen += lens[i];
      totalBlocks += lblocks[i].locatedBlockCount();
    }
    LOG.info("total len=" + totalLen + "; totalBlocks=" + totalBlocks);
    
    
    fStatus = nn.getFileInfo(trg);
    trgLen  = fStatus.getLen(); // new length
    
    // read the resulting file
    stm = dfs.open(trgPath);
    System.out.println("targetLen "+trgLen);
    byte[] byteFileConcat = new byte[(int)trgLen];
    stm.readFully(0, byteFileConcat);
    stm.close();
    
    trgBlocks = nn.getBlockLocations(trg, 0, trgLen).locatedBlockCount();
    
    //verifications
    // 1. number of blocks
    assertEquals(trgBlocks, totalBlocks); 
        
    // 2. file lengths
    assertEquals(trgLen, totalLen);
    
    // 3. removal of the src file
    for(Path p: files) {
      fStatus = nn.getFileInfo(p.toUri().getPath());
      assertNull("File " + p + " still exists", fStatus); // file shouldn't exist
      // try to create fie with the same name
      DFSTestUtil.createFile(dfs, p, fileLen, REPL_FACTOR, 1); 
    }
  
    // 4. content
    checkFileContent(byteFileConcat, bytes);
    
    // 5. read in parts
    int start = 0;
    
    byte[] total = new byte[(int)trgLen];
    while(start<trgLen){
      byte[] part = new byte[(int)Math.min(1000, trgLen-start)];
      stm = dfs.open(trgPath);
      stm.readFully(start, part);
      System.arraycopy( part, 0, total, start, (int)Math.min(part.length,trgLen-start));
      start+=1000;
      stm.close();
    }
    checkFileContent(total, bytes);
    
    // 6. read by seeking
    start = 0;
    total = new byte[(int)trgLen];
    stm = dfs.open(trgPath);
    while(start<trgLen){
      stm.seek(start);
      total[start++] = stm.readByte();
    }
    stm.close();
    checkFileContent(total, bytes);
    
    // 7. positioned reading
    start = 0;
    total = new byte[(int)trgLen];
    stm = dfs.open(trgPath);
    while(start<trgLen){
      stm.read(start, total, start++, 1);
    }
    stm.close();
    checkFileContent(total, bytes);
    ////////
  
    // add a small file (less then a block)
    Path smallFile = new Path("/sfile");
    int sFileLen = 10;
    DFSTestUtil.createFile(dfs, smallFile, sFileLen, REPL_FACTOR, 1);
    LOG.info("Trying the second concat operation.");
    dfs.concat(trgPath, new Path [] {smallFile}, restricted);
    LOG.info("Second concat operation successful.");
    
    fStatus = nn.getFileInfo(trg);
    trgLen  = fStatus.getLen(); // new length
    
    // check number of blocks
    trgBlocks = nn.getBlockLocations(trg, 0, trgLen).locatedBlockCount();
    assertEquals(trgBlocks, totalBlocks+1);
    
    // and length
    assertEquals(trgLen, totalLen+sFileLen);
    
  }

  // compare content
  private void checkFileContent(byte[] concat, byte[][] bytes ) {
    int idx=0;
    boolean mismatch = false;
    
    for(byte [] bb: bytes) {
      for(byte b: bb) {
        if(b != concat[idx++]) {
          mismatch=true;
          break;
        }
      }
      if(mismatch)
        break;
    }
    assertFalse("File content of concatenated file is different", mismatch);
  }

  // test case when final block is not of a full length
  @Test
  public void testConcatNotCompleteBlockRestricted() throws IOException{
    runTestConcatNotCompleteBlock(true);
  }
  
  // test case when final block is not of a full length
  @Test
  public void testConcatNotCompleteBlockUnrestricted() throws IOException{
    runTestConcatNotCompleteBlock(false);
  }
  
  private void runTestConcatNotCompleteBlock(boolean restricted) throws IOException {
    long trgFileLen = blockSize*3;
    long srcFileLen = blockSize*3+20; // block at the end - not full

    
    // create first file
    String name1="/trg", name2="/src";
    Path filePath1 = new Path(name1);
    DFSTestUtil.createFile(dfs, filePath1, trgFileLen, REPL_FACTOR, 1);
    
    FileStatus fStatus = cluster.getNameNode().getFileInfo(name1);
    long fileLen = fStatus.getLen();
    assertEquals(fileLen, trgFileLen);
    
    //read the file
    FSDataInputStream stm = dfs.open(filePath1);
    byte[] byteFile1 = new byte[(int)trgFileLen];
    stm.readFully(0, byteFile1);
    stm.close();
    
    LocatedBlocks lb1 = cluster.getNameNode().getBlockLocations(name1, 0, trgFileLen);
    
    Path filePath2 = new Path(name2);
    DFSTestUtil.createFile(dfs, filePath2, srcFileLen, REPL_FACTOR, 1);
    fStatus = cluster.getNameNode().getFileInfo(name2);
    fileLen = fStatus.getLen();
    assertEquals(srcFileLen, fileLen);
    
    // read the file
    stm = dfs.open(filePath2);
    byte[] byteFile2 = new byte[(int)srcFileLen];
    stm.readFully(0, byteFile2);
    stm.close();
    
    LocatedBlocks lb2 = cluster.getNameNode().getBlockLocations(name2, 0, srcFileLen);
    
    
    System.out.println("trg len="+trgFileLen+"; src len="+srcFileLen);
    
    // move the blocks
    dfs.concat(filePath1, new Path [] {filePath2}, restricted);
    
    long totalLen = trgFileLen + srcFileLen;
    fStatus = cluster.getNameNode().getFileInfo(name1);
    fileLen = fStatus.getLen();
    
    // read the resulting file
    stm = dfs.open(filePath1);
    byte[] byteFileConcat = new byte[(int)fileLen];
    stm.readFully(0, byteFileConcat);
    stm.close();
    
    LocatedBlocks lbConcat = cluster.getNameNode().getBlockLocations(name1, 0, fileLen);
    
    //verifications
    // 1. number of blocks
    assertEquals(lbConcat.locatedBlockCount(), 
        lb1.locatedBlockCount() + lb2.locatedBlockCount());
    
    // 2. file lengths
    System.out.println("file1 len="+fileLen+"; total len="+totalLen);
    assertEquals(fileLen, totalLen);
    
    // 3. removal of the src file
    fStatus = cluster.getNameNode().getFileInfo(name2);
    assertNull("File "+name2+ "still exists", fStatus); // file shouldn't exist
  
    // 4. content
    checkFileContent(byteFileConcat, new byte [] [] {byteFile1, byteFile2});
  }
  
  
  
  /**
   * test illegal args cases
   */
  @Test
  public void testIllegalArgResticted() throws IOException{
    runTestIllegalArg(true);
  }
  
  /**
   * test illegal args cases
   */
  @Test
  public void testIllegalArgUnresticted() throws IOException{
    runTestIllegalArg(false);
  }  
  
  
  private void runTestIllegalArg(boolean restricted) throws IOException {
    long fileLen = blockSize*3;
    
    Path parentDir  = new Path ("/parentTrg");
    assertTrue(dfs.mkdirs(parentDir));
    Path trg = new Path(parentDir, "trg");
    DFSTestUtil.createFile(dfs, trg, fileLen, REPL_FACTOR, 1);

    // must be in the same dir
    {
      // create first file
      Path dir1 = new Path ("/dir1");
      assertTrue(dfs.mkdirs(dir1));
      Path src = new Path(dir1, "src");
      DFSTestUtil.createFile(dfs, src, fileLen, REPL_FACTOR, 1);
      
      try {
        dfs.concat(trg, new Path [] {src}, restricted);
        fail("didn't fail for src and trg in different directories");
      } catch (Exception e) {
        // expected
      }
    }
    // non existing file
    try {
      dfs.concat(trg, new Path [] {new Path("test1/a")}, restricted); // non existing file
      fail("didn't fail with invalid arguments");
    } catch (Exception e) {
      //expected
    }
    // empty arg list
    try {
      dfs.concat(trg, new Path [] {}, restricted); // empty array
      fail("didn't fail with invalid arguments");
    } catch (Exception e) {
      // expected
    }
 
  }
}
