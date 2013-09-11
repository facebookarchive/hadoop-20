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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestFileHardLink {
    {
      ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
      ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
      ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    }
    
  @Test
  public void testHardLinkAfterRestart() throws Exception {
    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    try {
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, new Path("/f1"), 1, (short) 1, 0);
      DFSTestUtil.createFile(fs, new Path("/f2"), 1, (short) 1, 0);
      fs.hardLink(new Path("/f1"), new Path("/dst/f1"));
      cluster.getNameNode().saveNamespace(true, false);
      cluster.restartNameNode();
      fs.hardLink(new Path("/f2"), new Path("/dst/f2"));
      long hid1 = cluster.getNameNode().namesystem.dir.getHardLinkId("/f1");
      long hid2 = cluster.getNameNode().namesystem.dir.getHardLinkId("/f2");
      assertEquals(0, hid1);
      assertEquals(1, hid2);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testHardLinkWithSameFilename() throws Exception {  
    final Configuration conf = new Configuration(); 
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null); 
    final FileSystem fs = cluster.getFileSystem();  
    assertTrue("Not a HDFS: "+ fs.getUri(), fs instanceof DistributedFileSystem); 
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;  
    final Path p1 = new Path("/d1/f");
    final Path p2 = new Path("/d2/f"); 
    try { 
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, p1, 1);
      stm1.sync();
      stm1.close();
      
      dfs.hardLink(p1, p2); 
      dfs.delete(p2, true);
    } catch (Exception e) {
      Assert.fail("testHardLinkWithSameFilename is failed due to " + e.getMessage());
    } finally { 
      cluster.shutdown(); 
    } 
  }

  @Test
  public void testHardLinkFiles() throws IOException {
    Configuration conf = new Configuration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.safemode.threshold.pct", 1);
    conf.setBoolean("dfs.support.append", true);
    conf.setLong("dfs.blockreport.intervalMsec", 0);

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    //final DFSClient dfsClient = new DFSClient(NameNode.getAddress(conf), conf);
    FileSystem fs = null;
    long dirOverHead = 0;
    boolean result;
    NameNode nameNode = cluster.getNameNode();
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();

      try {
        cluster.getNameNode().namesystem.dir.getHardLinkId("/nonexistentfile");
        fail("Did not throw exception for getHardLinkId() on nonexistent file");
      } catch (IOException ie) {
        System.out.println("Expected exception : " + ie);
      }

      // open /user/dir1/file1 and write
      Path dir1 = new Path("/user/dir1");
      fs.mkdirs(dir1);
      dirOverHead = fs.getContentSummary(dir1).getLength();
      System.out.println("The dir overhead is " + dirOverHead);
      
      // write file into file1
      Path file1 = new Path(dir1, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      byte[] content = TestFileCreation.writeFile(stm1);
      stm1.sync();
      stm1.close();
      
      try {
        cluster.getNameNode().namesystem.dir.getHardLinkId(file1.toUri()
            .getPath());
        fail("Did not throw exception for getHardLinkId() on non hardlinked file file");
      } catch (IOException ie) {
        System.out.println("Expected exception : " + ie);
      }

      // Verify links.
      String[] links = fs.getHardLinkedFiles(file1);
      Assert.assertEquals(0, links.length);

      /* du /user/dir1 -> dirLength1
       * du /user/dir1/file1 -> fileLength1
       * dirLenght1 = fileLenght1 + dirOverhead 
       */
      long dirLength1 = fs.getContentSummary(dir1).getLength();
      long fileLength1 = fs.getContentSummary(file1).getLength();
      Assert.assertEquals(dirOverHead, dirLength1 - fileLength1);
      FileStatus fStatus1 = fs.getFileStatus(file1);
      Assert.assertTrue(fStatus1.getBlockSize() > 0 );
      System.out.println("dir1 length: " + dirLength1 + " file1 length: " + fileLength1 +
          " block size " + fStatus1.getBlockSize());
      
      // create /user/dir2
      Path dir2 = new Path("/user/dir2");
      fs.mkdirs(dir2);
      Path file2 = new Path(dir2, "file2");
      // ln /user/dir1/ /user/dir2/ [error: cannot hard link a directory]
      result = fs.hardLink(dir1, dir2);
      Assert.assertFalse(result);
      
      // ln /user/dir1/file1 /user/dir2/file2
      result = fs.hardLink(file1, file2);

      // Verify links.
      links = fs.getHardLinkedFiles(file1);
      Assert.assertEquals(1, links.length);
      Assert.assertEquals(file2, new Path(links[0]));

      Assert.assertTrue(result);
      verifyLinkedFileIdenticial(fs, nameNode, fStatus1, fs.getFileStatus(file1), content);
      // Verify all the linked files shared the same file properties such as modification time
      long current = System.currentTimeMillis();
      String testUserName = "testUser";
      String testGroupName = "testGroupName";
      FsPermission pemission = new FsPermission(FsAction.READ_WRITE,
          FsAction.READ_WRITE, FsAction.READ_WRITE);
      short newReplication = (short) (fs.getFileStatus(file1).getReplication() + 1);
      
      fs.setTimes(file2, current, current);
      fs.setOwner(file2, testUserName, testGroupName);
      fs.setPermission(file1, pemission);
      
      // increase replication
      fs.setReplication(file2, newReplication);
      
      // make sure the file is replicated
      DFSTestUtil.waitReplication(fs, file2, (short) newReplication);
      DFSTestUtil.waitReplication(fs, file1, (short) newReplication);
      
      // decrease replication
      newReplication--;
      fs.setReplication(file2, newReplication);
      
      // make sure the file is replicated
      DFSTestUtil.waitReplication(fs, file2, (short) newReplication);
      DFSTestUtil.waitReplication(fs, file1, (short) newReplication);

      fStatus1 = fs.getFileStatus(file1);
      FileStatus fStatus2 = fs.getFileStatus(file2);
      
      assertEquals(current, fStatus1.getModificationTime());
      assertEquals(current, fStatus2.getModificationTime());
      
      assertEquals(testUserName, fStatus1.getOwner());
      assertEquals(testUserName, fStatus2.getOwner());
      
      assertEquals(testGroupName, fStatus1.getGroup());
      assertEquals(testGroupName, fStatus2.getGroup());
      
      assertEquals(pemission, fStatus1.getPermission());
      assertEquals(pemission, fStatus2.getPermission());
      
      assertEquals(newReplication, fStatus1.getReplication());
      assertEquals(newReplication, fStatus2.getReplication());
      
      /* 
       * du /user/dir2 -> dirLength2
       * du /user/dir2/file2 -> fileLength2
       * dirLength2 == dirLenght1
       * fileLenght2 == fileLength1 
       */
      Assert.assertTrue(fStatus2.getBlockSize() > 0 );
      long dirLength2 = fs.getContentSummary(dir2).getLength();
      long fileLength2 = fs.getContentSummary(file2).getLength();
      Assert.assertEquals(dirOverHead, dirLength2 - fileLength2);
      Assert.assertEquals(fileLength1, fileLength2);
      Assert.assertEquals(dirLength1, dirLength2);
      // verify file1 and file2 are identical
      verifyLinkedFileIdenticial(fs, nameNode, fStatus1, fStatus2, content);
      System.out.println("dir2 length: " + dirLength2 + " file2 length: " + fileLength2);
      
      // ln /user/dir1/file1 /user/dir2/file2 
      // client can retry the hardlink operation without error
      result = fs.hardLink(file1, file2);
      Assert.assertTrue(result);
      
      Path dir3= new Path("/user/dir3/dir33/dir333");
      Path file3 = new Path(dir3, "file3");
      
      // ln /user/dir2/file2 /user/dir3/dir33/dir333/file3 [create the intermediate dirs on the fly]
      result = fs.hardLink(file2, file3);
      Assert.assertTrue(result);
 
      // ln /user/dir2/file2 /user/dir3/file3
      fs.mkdirs(dir3);
      result = fs.hardLink(file2, file3);

      // Verify links, now 3 links file1, file2, file3
      links = fs.getHardLinkedFiles(file1);
      Assert.assertEquals(2, links.length);
      for (String link : links) {
        if (!(file2.equals(new Path(link)) || file3.equals(new Path(link)))) {
          fail("Could not find " + file1 + " or " + file2
              + " in the list of links");
        }
      }

      Assert.assertTrue(result);
      FileStatus fStatus3 = fs.getFileStatus(file3);
      
      long dirLength3 = fs.getContentSummary(dir3).getLength();
      long fileLength3 = fs.getContentSummary(file3).getLength();
      Assert.assertEquals(dirOverHead, dirLength3 - fileLength3);
      Assert.assertEquals(fileLength2, fileLength3);
      Assert.assertEquals(dirLength2, dirLength3);
      
      // verify that file3 is identical to file 2 and file 1
      Assert.assertTrue(fStatus3.getBlockSize() > 0 );
      verifyLinkedFileIdenticial(fs, nameNode, fStatus1, fStatus3, content);
      verifyLinkedFileIdenticial(fs, nameNode, fStatus2, fStatus3, content);
      System.out.println("dir3 length: " + dirLength3 + " file3 length: " + fileLength3);
      
      /*  
       * du /user -> totalDU  
       * totalDU = du1 + du2 + du3 - file1 - file2  
       * totalDU = 3 * dirOverhead + file1  
       */ 
      long totalDU = fs.getContentSummary(new Path("/user")).getLength(); 
      System.out.println("Total DU for /user is " + totalDU); 
      Assert.assertEquals(totalDU, dirLength3 + dirLength2 + dirLength1 
          - fileLength1 - fileLength2 );  
      Assert.assertEquals(totalDU, 3 * dirOverHead + fileLength1);
      
      /* start to test the delete operation
      * delete /user/dir1/file1
      * verify no file1 any more and verify there is no change for file2 and file3
      */
      fs.delete(file1, true);

      // Verify links, now 2 links file2, file3
      links = fs.getHardLinkedFiles(file2);
      Assert.assertEquals(1, links.length);
      Assert.assertEquals(file3, new Path(links[0]));

      Assert.assertFalse(fs.exists(file1)) ;
      Assert.assertEquals(dirOverHead, fs.getContentSummary(dir1).getLength());
      Assert.assertEquals(fileLength2, fs.getContentSummary(file2).getLength());
      Assert.assertEquals(fileLength3, fs.getContentSummary(file3).getLength());
      Assert.assertEquals(dirLength2, fs.getContentSummary(dir2).getLength());
      Assert.assertEquals(dirLength3, fs.getContentSummary(dir3).getLength());
      verifyLinkedFileIdenticial(fs, nameNode, fStatus2, fs.getFileStatus(file2), content);
      verifyLinkedFileIdenticial(fs, nameNode, fStatus3, fs.getFileStatus(file3), content);
      
      /* 
      * delete /user/dir2/
      * verify no file2 or dir2 any more and verify there is no change for file3
      */
      fs.delete(dir2, true);
      // Verify links, now only 1 links file3
      links = fs.getHardLinkedFiles(file3);
      Assert.assertEquals(0, links.length);

      Assert.assertFalse(fs.exists(file2));
      Assert.assertFalse(fs.exists(dir2));
      Assert.assertEquals(fileLength3, fs.getContentSummary(file3).getLength());
      Assert.assertEquals(dirLength3, fs.getContentSummary(dir3).getLength());
      verifyLinkedFileIdenticial(fs, nameNode, fStatus3, fs.getFileStatus(file3), content);
      
      /* 
       * delete /user/dir3/
       * verify no file3 or dir3 any more and verify the total DU
       */
       fs.delete(dir3, true);
       Assert.assertFalse(fs.exists(file3));
       Assert.assertFalse(fs.exists(dir3));
       totalDU = fs.getContentSummary(new Path("/user")).getLength();  
       Assert.assertEquals(totalDU, dirOverHead);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testHardLinkWithDirDeletion() throws Exception {  
    final Configuration conf = new Configuration(); 
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null); 
    final FileSystem fs = cluster.getFileSystem();  
    assertTrue("Not a HDFS: "+ fs.getUri(), fs instanceof DistributedFileSystem); 
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;  
    final int NUM_FILES = 1000; 
    try { 
      // 1: create directories: /root/dir1 and /root/dir2 
      final Path root = new Path("/root/"); 
      assertTrue(dfs.mkdirs(root)); 
        
      final Path dir1 = new Path(root, "dir1"); 
      assertTrue(dfs.mkdirs(dir1)); 
      final Path dir2 = new Path(root, "dir2"); 
      assertTrue(dfs.mkdirs(dir2)); 
        
      ContentSummary rootSummary, c1, c2; 
      rootSummary = dfs.getContentSummary(root);  
      Assert.assertEquals(0, rootSummary.getFileCount()); 
      Assert.assertEquals(3, rootSummary.getDirectoryCount());  
      // There are 4 directories left: root, dir1, dir2 and HDFS's root directory.  
      Assert.assertEquals(4, cluster.getNameNode().getNamesystem().getFilesAndDirectoriesTotal());  
        
      // 2: create NUM_FILES files under dir1 and create their hardlink files in both dir1 and dir2.  
      // And check the files count as well. 
      for (int i = 1; i <= NUM_FILES; i++) {  
        final Path target = new Path(dir1, "file-" + i);  
        final Path hardLink1 = new Path(dir1, "hardlink-file-" + i);  
        final Path hardLink2 = new Path(dir2, "hardlink-file-" + i);  
          
        DFSTestUtil.createFile(dfs, target, 1L, (short)3, 0L);  
        fs.hardLink(target, hardLink1); 
        fs.hardLink(target, hardLink2); 
          
        c1 = dfs.getContentSummary(dir1); 
        Assert.assertEquals(2 * i, c1.getFileCount());  
          
        c2 = dfs.getContentSummary(dir2); 
        Assert.assertEquals(i, c2.getFileCount());  
      } 
        
      rootSummary = dfs.getContentSummary(root);  
      Assert.assertEquals(3 * NUM_FILES, rootSummary.getFileCount()); 
      Assert.assertEquals(3, rootSummary.getDirectoryCount());  
        
      // 3: delete dir1 directly (skipping trash) and check the files count again 
      dfs.getClient().delete(dir1.toUri().getPath(), true); 
        
      c2 = dfs.getContentSummary(dir2); 
      Assert.assertEquals(NUM_FILES, c2.getFileCount());  
        
      rootSummary = dfs.getContentSummary(root);  
      Assert.assertEquals(NUM_FILES, rootSummary.getFileCount()); 
      Assert.assertEquals(2, rootSummary.getDirectoryCount());  
        
      // There are 3 directories left: root, dir1 and HDFS's root directory.  
      Assert.assertEquals(NUM_FILES + 3,  
          cluster.getNameNode().getNamesystem().getFilesAndDirectoriesTotal()); 
    } finally { 
      cluster.shutdown(); 
    } 
  }
  
  @Test
  public void testHardLinkWithFileOverwite() throws Exception {
    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+ fs.getUri(), fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    try {
      // 1: create the root dir
      ContentSummary c;
      final Path root = new Path("/root/");
      assertTrue(dfs.mkdirs(root));
      
      // 2: create file1 and its hardlink file: file1-hardlink-orig
      final Path file1 =  new Path(root, "file1");
      DFSTestUtil.createFile(dfs, file1, 1L, (short)3, 0L);
      
      final Path file1HardLinkOrig =  new Path(root, "file1-hardlink-orig");
      fs.hardLink(file1, file1HardLinkOrig);
      Assert.assertEquals(1, fs.getHardLinkedFiles(file1).length);
      
      c = dfs.getContentSummary(root);
      Assert.assertEquals(2, c.getFileCount());
      Assert.assertEquals(4, cluster.getNameNode().getNamesystem().getFilesAndDirectoriesTotal());
      
      // 3: overwrite the file1
      DFSTestUtil.createFile(dfs, file1, 2L, (short)3, 0L);
      Assert.assertEquals(0, fs.getHardLinkedFiles(file1).length);
      Assert.assertEquals(0, fs.getHardLinkedFiles(file1HardLinkOrig).length);
      
      c = dfs.getContentSummary(root);
      Assert.assertEquals(2, c.getFileCount());
      Assert.assertEquals(4, cluster.getNameNode().getNamesystem().getFilesAndDirectoriesTotal());
      
      // 4: create a new hardlink file for the file1: file1-hardlink-new
      final Path file1HardLinkNew =  new Path(root, "file1-hardlink-new");
      fs.hardLink(file1, file1HardLinkNew);
      Assert.assertEquals(1, fs.getHardLinkedFiles(file1).length);
      Assert.assertEquals(1, fs.getHardLinkedFiles(file1HardLinkNew).length);
      Assert.assertEquals(0, fs.getHardLinkedFiles(file1HardLinkOrig).length);
      
      c = dfs.getContentSummary(root);
      Assert.assertEquals(3, c.getFileCount());
      Assert.assertEquals(5, cluster.getNameNode().getNamesystem().getFilesAndDirectoriesTotal());
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testHardLinkWithNNRestart() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    NameNode nameNode = cluster.getNameNode();
    assertTrue("Not a HDFS: "+ fs.getUri(), fs instanceof DistributedFileSystem);
    try {
      // Create a new file
      Path root = new Path("/user/");
      Path file10 = new Path(root, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file10, 1);
      byte[] content = TestFileCreation.writeFile(stm1);
      stm1.close();
      
      Path file11 =  new Path(root, "file-11");
      Path file12 =  new Path(root, "file-12");
      fs.hardLink(file10, file11);
      fs.hardLink(file11, file12);
      
      verifyLinkedFileIdenticial(fs, nameNode, fs.getFileStatus(file10), fs.getFileStatus(file11),
          content);
      verifyLinkedFileIdenticial(fs, nameNode, fs.getFileStatus(file10), fs.getFileStatus(file12),
          content);
      
      // Restart the cluster
      cluster.restartNameNode();
      nameNode = cluster.getNameNode();
      
      // Verify all the linked files are the same
      verifyLinkedFileIdenticial(fs, nameNode, fs.getFileStatus(file10), fs.getFileStatus(file11),
          content);
      verifyLinkedFileIdenticial(fs, nameNode, fs.getFileStatus(file10), fs.getFileStatus(file12),
          content);
      
      // Delete file10
      fs.delete(file10, true);
      assertFalse(fs.exists(file10));
      assertTrue(fs.exists(file11));
      assertTrue(fs.exists(file12));
      
      // Restart the cluster
      cluster.restartNameNode();
      nameNode = cluster.getNameNode();
      
      // Verify the deleted files
      assertFalse(fs.exists(file10));
      assertTrue(fs.exists(file11));
      assertTrue(fs.exists(file12));
      verifyLinkedFileIdenticial(fs, nameNode, fs.getFileStatus(file11), fs.getFileStatus(file12),
          content);
      
      // Delete file11
      fs.delete(file11, true);
      assertFalse(fs.exists(file11));
      assertTrue(fs.exists(file12));
      
      // Restart the cluster
      cluster.restartNameNode();
      assertFalse(fs.exists(file11));
      assertTrue(fs.exists(file12));
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testHardLinkWithNSQuota() throws Exception {
    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+ fs.getUri(), fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    try {
      // 1: create directories
      final Path root = new Path("/user/");
      final Path dir1 = new Path("/user/dir1/");
      final Path dir2 = new Path("/user/dir1/dir2/");
      assertTrue(dfs.mkdirs(dir2));
      
      // 2: set the ns quota 
      dfs.setQuota(root, 6, FSConstants.QUOTA_DONT_SET);
      dfs.setQuota(dir1, 4, FSConstants.QUOTA_DONT_SET);
      dfs.setQuota(dir2, 2, FSConstants.QUOTA_DONT_SET);
      
      verifyNSQuotaSetting(dfs, root, 6, 3);
      verifyNSQuotaSetting(dfs, dir1, 4, 2);
      verifyNSQuotaSetting(dfs, dir2, 2, 1);
      
      // 3: create a regular file: /user/dir1/dir2/file-10 and check the ns quota
      final Path file10 =  new Path(dir2, "file-10");
      DFSTestUtil.createFile(dfs, file10, 1L, (short)3, 0L);
      verifyNSQuotaSetting(dfs, root, 6 ,4);
      verifyNSQuotaSetting(dfs, dir1, 4 ,3);
      verifyNSQuotaSetting(dfs, dir2, 2, 2);
      
      // 4: create a regular file: /user/dir1/dir2/file-11 and catch the NSQuotaExceededException
      final Path file11 =  new Path(dir2, "file-11");
      try {
        DFSTestUtil.createFile(dfs, file11, 1L, (short)3, 0L);
        Assert.fail("Expect an NSQuotaExceededException thrown out");
      } catch (NSQuotaExceededException e) {}

      verifyNSQuotaSetting(dfs, root, 6 ,4);
      verifyNSQuotaSetting(dfs, dir1, 4 ,3);
      verifyNSQuotaSetting(dfs, dir2, 2, 2); 
      
      // 5: ln /user/dir1/dir2/file-10 /user/dir1/dir2/file-11 and catch the NSQuotaExceededException
      try {
        fs.hardLink(file10, file11);
        Assert.fail("Expect an NSQuotaExceededException thrown out");
      } catch (NSQuotaExceededException e) {}
      verifyNSQuotaSetting(dfs, root, 6 ,4);
      verifyNSQuotaSetting(dfs, dir1, 4 ,3);
      verifyNSQuotaSetting(dfs, dir2, 2, 2);
      
      // 6: ln /user/dir1/dir2/file-10 /user/dir1/file-12 and verify the quota
      final Path file12 =  new Path(dir1, "file-12");
      assertTrue(fs.hardLink(file10, file12));
      verifyNSQuotaSetting(dfs, root, 6 ,5);
      verifyNSQuotaSetting(dfs, dir1, 4 ,4);
      verifyNSQuotaSetting(dfs, dir2, 2, 2);
      
      // 7: ln /user/dir1/dir2/file-10 /user/dir1/file-13 and catch the NSQuotaExceededException
      final Path file13 = new Path(dir1, "file-13");
      try {
        fs.hardLink(file10, file13);
        Assert.fail("Expect an NSQuotaExceededException thrown out");
      } catch (NSQuotaExceededException e) {}
      verifyNSQuotaSetting(dfs, root, 6 ,5);
      verifyNSQuotaSetting(dfs, dir1, 4 ,4);
      verifyNSQuotaSetting(dfs, dir2, 2, 2);
      
      // 8: ln /user/dir1/dir2/file-10 /user/file-14 and verify the quota
      final Path file14 =  new Path(root, "file-14");
      assertTrue(fs.hardLink(file10, file14));
      verifyNSQuotaSetting(dfs, root, 6 ,6);
      verifyNSQuotaSetting(dfs, dir1, 4 ,4);
      verifyNSQuotaSetting(dfs, dir2, 2, 2);
      
      // 9: ln /user/dir1/dir2/file-10 /user/file-15 and catch the NSQuotaExceededException
      final Path file15 = new Path(root, "file-15");
      try {
        fs.hardLink(file10, file15);
        Assert.fail("Expect an NSQuotaExceededException thrown out");
      } catch (NSQuotaExceededException e) {}
      
      verifyNSQuotaSetting(dfs, root, 6 ,6);
      verifyNSQuotaSetting(dfs, dir1, 4 ,4);
      verifyNSQuotaSetting(dfs, dir2, 2, 2);
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testHardLinkWithDSQuota() throws Exception {
    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+ fs.getUri(), fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    int fileSize = 256;
    long diskSize = 0;
    try {
      // 1: create directories
      final Path root = new Path("/user/");
      final Path dir1 = new Path("/user/dir1/");
      final Path dir2 = new Path("/user/dir1/dir2/");
      assertTrue(dfs.mkdirs(dir2));
      
      // 2 create the files and get the diskSize
      final Path file10 =  new Path(dir2, "file-10");
      DFSTestUtil.createFile(dfs, file10, (long)fileSize, (short)1, 0L);
      final Path file11 =  new Path(root, "file-11");
      DFSTestUtil.createFile(dfs, file11, (long)fileSize, (short)1, 0L);

      ContentSummary c1 = dfs.getContentSummary(file10);
      diskSize = c1.getSpaceConsumed();
      assertEquals(fileSize, diskSize);
      ContentSummary c2 = dfs.getContentSummary(file11);
      diskSize = c2.getSpaceConsumed();
      assertEquals(fileSize, diskSize);
      
      // 3: set the ds quota
      dfs.setQuota(root, FSConstants.QUOTA_DONT_SET, 3 * diskSize);
      dfs.setQuota(dir1, FSConstants.QUOTA_DONT_SET, 2 * diskSize);
      dfs.setQuota(dir2, FSConstants.QUOTA_DONT_SET, 1 * diskSize);
      
      verifyDSQuotaSetting(dfs, root, 3 * diskSize, 2 * diskSize);
      verifyDSQuotaSetting(dfs, dir1, 2 * diskSize, 1 * diskSize);
      verifyDSQuotaSetting(dfs, dir2, 1 * diskSize, 1 * diskSize);

      // 4: ln /user/dir1/dir2/file-10 /user/dir1/dir2/file-12
      final Path file12 =  new Path(dir2, "file-12");
      assertTrue(fs.hardLink(file10, file12));
      verifyDSQuotaSetting(dfs, root, 3 * diskSize, 2 * diskSize);
      verifyDSQuotaSetting(dfs, dir1, 2 * diskSize, 1 * diskSize);
      verifyDSQuotaSetting(dfs, dir2, 1 * diskSize, 1 * diskSize);

      // 6: ln /user/dir1/dir2/file-10 /user/dir1/file-13
      final Path file13 =  new Path(dir1, "file-13");
      assertTrue(fs.hardLink(file10, file13));
      verifyDSQuotaSetting(dfs, root, 3 * diskSize, 2 * diskSize);
      verifyDSQuotaSetting(dfs, dir1, 2 * diskSize, 1 * diskSize);
      verifyDSQuotaSetting(dfs, dir2, 1 * diskSize, 1 * diskSize);
      
      // 7: ln /user/file-11 /user/dir1/file-14
      final Path file14 =  new Path(dir1, "file-14");
      assertTrue(fs.hardLink(file11, file14));
      verifyDSQuotaSetting(dfs, root, 3 * diskSize, 2 * diskSize);
      verifyDSQuotaSetting(dfs, dir1, 2 * diskSize, 2 * diskSize);
      verifyDSQuotaSetting(dfs, dir2, 1 * diskSize, 1 * diskSize);
      
      // 8: ln /user/file-11 /user/dir1/dir2/file-15
      final Path file15 =  new Path(dir2, "file-15");
      try {
        fs.hardLink(file11, file15);
        Assert.fail("Expect a DSQuotaExceededException thrown out");
      } catch (DSQuotaExceededException e) {}
      verifyDSQuotaSetting(dfs, root, 3 * diskSize, 2 * diskSize);
      verifyDSQuotaSetting(dfs, dir1, 2 * diskSize, 2 * diskSize);
      verifyDSQuotaSetting(dfs, dir2, 1 * diskSize, 1 * diskSize);
    } finally {
      cluster.shutdown();
    }
  }

  public static void verifyLinkedFileIdenticial(FileSystem fs, NameNode nameNode, FileStatus f1,
      FileStatus f2, byte[] content) throws IOException {
    Assert.assertEquals(f1.getBlockSize(), f2.getBlockSize());
    Assert.assertEquals(f1.getAccessTime(), f2.getAccessTime());
    Assert.assertEquals(f1.getChildrenCount(), f2.getChildrenCount());
    Assert.assertEquals(f1.getLen(), f2.getLen());
    Assert.assertEquals(f1.getModificationTime(), f2.getModificationTime());
    Assert.assertEquals(f1.getReplication(), f2.getReplication());
    Assert.assertEquals(f1.getPermission(), f2.getPermission());

    checkBlocks(
        nameNode.getBlockLocations(f1.getPath().toUri().getPath(), 0, f1.getLen()).getLocatedBlocks(),
        nameNode.getBlockLocations(f2.getPath().toUri().getPath(), 0, f2.getLen()).getLocatedBlocks());
    
    checkFile(fs, f1.getPath(), (int) f1.getLen(), content);
    checkFile(fs, f2.getPath(), (int) f2.getLen(), content);
  }

  private static void checkBlocks(List<LocatedBlock> blocks1, List<LocatedBlock> blocks2) {
    Assert.assertEquals(blocks1.size(), blocks2.size());
    for (int i = 0; i < blocks1.size(); i++) {
      Assert.assertEquals(blocks1.get(i).getBlock().getBlockId(), 
                          blocks2.get(i).getBlock().getBlockId());
    }
  }
  
  private static void checkFile(FileSystem fs, Path name, int len, byte[] content)
      throws IOException {
    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[len];
    stm.readFully(0, actual);
    checkData(actual, 0, content, "check the content of the hard linked file "
        + name + " :");
    stm.close();
  }

  private static void checkData(byte[] actual, int from, byte[] expected,
      String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message + " byte " + (from + idx) + " differs. expected "
          + expected[from + idx] + " actual " + actual[idx], expected[from
          + idx], actual[idx]);
      actual[idx] = 0;
    }
  }
  
  private static void verifyNSQuotaSetting(DistributedFileSystem dfs, Path path, int nsQuota,
      int nsComsumed) throws IOException {
    ContentSummary c = dfs.getContentSummary(path);
    assertEquals(nsQuota, c.getQuota());
    assertEquals(nsComsumed, c.getFileCount() + c.getDirectoryCount());
  }
  
  private static void verifyDSQuotaSetting(DistributedFileSystem dfs, Path path, long dsCount,
      long diskComsumed) throws IOException {
    ContentSummary c = dfs.getContentSummary(path);
    assertEquals(dsCount, c.getSpaceQuota());
    assertEquals(diskComsumed, c.getSpaceConsumed());
  }
}
