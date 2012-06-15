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

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;

  public class TestFileHardLink extends junit.framework.TestCase {
    {
      ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
      ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
      ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    }

    /**
     * mkdir /user/dir1/
     * du /user/dir1/ -> dirOverhead
     * 
     * open /user/dir1/file1 and write
     * du /user/dir1 -> dirLength1
     * du /user/dir1/file1 -> fileLength1
     * dirLenght1 = fileLenght1 + dirOverhead
     * 
     * mkdir /user/dir2/
     * ln /user/dir1/file1 /user/dir2/file2
     * verify file1 is identical to file2
     * 
     * du /user/dir2 -> dirLength2
     * du /user/dir2/file2 -> fileLength2
     * dirLength2 == dirLenght1
     * fileLenght2 == fileLength1
     * 
     * ln /user/dir1/file1 /user/dir2/file2
     * Client can retry the hardlink operation without problems
     * ln /user/dir2/file2 /user/dir3/file3 [error dir3 is not existed]
     * 
     * mkdir /user/dir3
     * ln /user/dir2/file2 /user/dir3/file3 
     * verify file3 is identical to file2 and file1
     * 
     * du /user/dir3 -> dirLength3
     * du /user/dir3/file3 -> fileLength3
     * dirLength3 == dirLenght2
     * fileLenght3 == fileLength2
     * 
     * delete /user/dir1/file1
     * verify no file1 anymore and verify there is no change for file2 and file3
     * 
     * delete /user/dir2/
     * verify no file2 or dir2 any more and verify there is no change for file3
     * 
     * delete /user/dir3/
     * verify no file3 or dir3 any more
     */
    public void testHardLinkFiles() throws IOException {
      Configuration conf = new Configuration();
      final int MAX_IDLE_TIME = 2000; // 2s
      conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
      conf.setInt("heartbeat.recheck.interval", 1000);
      conf.setInt("dfs.heartbeat.interval", 1);
      conf.setInt("dfs.safemode.threshold.pct", 1);
      conf.setBoolean("dfs.support.append", true);

      // create cluster
      MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
      //final DFSClient dfsClient = new DFSClient(NameNode.getAddress(conf), conf);
      FileSystem fs = null;
      long dirOverHead = 0;
      boolean result;
      try {
        cluster.waitActive();
        fs = cluster.getFileSystem();

        // open /user/dir1/file1 and write
        Path dir1 = new Path("/user/dir1");
        fs.mkdirs(dir1);
        dirOverHead = fs.getContentSummary(dir1).getLength();
        System.out.println("The dir overhead is " + dirOverHead);
        
        // write file into file1
        Path file1 = new Path(dir1, "file1");
        FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
        System.out.println("testFileCreationDeleteParent: "
            + "Created file " + file1);
        byte[] content = TestFileCreation.writeFile(stm1);
        stm1.sync();
        stm1.close();
        
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
        Assert.assertTrue(result);
        verifyLinkedFileIdenticial(fs, fStatus1, fs.getFileStatus(file1), content);
        
        /* 
         * du /user/dir2 -> dirLength2
         * du /user/dir2/file2 -> fileLength2
         * dirLength2 == dirLenght1
         * fileLenght2 == fileLength1 
         */
        FileStatus fStatus2 = fs.getFileStatus(file2);
        Assert.assertTrue(fStatus2.getBlockSize() > 0 );
        long dirLength2 = fs.getContentSummary(dir2).getLength();
        long fileLength2 = fs.getContentSummary(file2).getLength();
        Assert.assertEquals(dirOverHead, dirLength2 - fileLength2);
        Assert.assertEquals(fileLength1, fileLength2);
        Assert.assertEquals(dirLength1, dirLength2);
        // verify file1 and file2 are identical
        verifyLinkedFileIdenticial(fs, fStatus1, fStatus2, content);
        System.out.println("dir2 length: " + dirLength2 + " file2 length: " + fileLength2);
        
        // ln /user/dir1/file1 /user/dir2/file2 
        // client can retry the hardlink operation without error
        result = fs.hardLink(file1, file2);
        Assert.assertTrue(result);
        
        Path dir3= new Path("/user/dir3");
        Path file3 = new Path(dir3, "file3");
        
        // ln /user/dir2/file2 /user/dir3/file3 [error: dir3 does not exist]
        result = fs.hardLink(file2, file3);
        Assert.assertFalse(result);
   
        // ln /user/dir2/file2 /user/dir3/file3
        fs.mkdirs(dir3);
        result = fs.hardLink(file2, file3);

        Assert.assertTrue(result);
        FileStatus fStatus3 = fs.getFileStatus(file3);
        
        long dirLength3 = fs.getContentSummary(dir3).getLength();
        long fileLength3 = fs.getContentSummary(file3).getLength();
        Assert.assertEquals(dirOverHead, dirLength3 - fileLength3);
        Assert.assertEquals(fileLength2, fileLength3);
        Assert.assertEquals(dirLength2, dirLength3);
        
        // verify that file3 is identical to file 2 and file 1
        Assert.assertTrue(fStatus3.getBlockSize() > 0 );
        verifyLinkedFileIdenticial(fs, fStatus1, fStatus3, content);
        verifyLinkedFileIdenticial(fs, fStatus2, fStatus3, content);
        System.out.println("dir3 length: " + dirLength3 + " file3 length: " + fileLength3);
        
        
        /* start to test the delete operation
        * delete /user/dir1/file1
        * verify no file1 any more and verify there is no change for file2 and file3
        */
        fs.delete(file1, true);
        Assert.assertFalse(fs.exists(file1)) ;
        Assert.assertEquals(dirOverHead, fs.getContentSummary(dir1).getLength());
        Assert.assertEquals(fileLength2, fs.getContentSummary(file2).getLength());
        Assert.assertEquals(fileLength3, fs.getContentSummary(file3).getLength());
        Assert.assertEquals(dirLength2, fs.getContentSummary(dir2).getLength());
        Assert.assertEquals(dirLength3, fs.getContentSummary(dir3).getLength());
        verifyLinkedFileIdenticial(fs, fStatus2, fs.getFileStatus(file2), content);
        verifyLinkedFileIdenticial(fs, fStatus3, fs.getFileStatus(file3), content);
        
        /* 
        * delete /user/dir2/
        * verify no file2 or dir2 any more and verify there is no change for file3
        */
        fs.delete(dir2, true);
        Assert.assertFalse(fs.exists(file2));
        Assert.assertFalse(fs.exists(dir2));
        Assert.assertEquals(fileLength3, fs.getContentSummary(file3).getLength());
        Assert.assertEquals(dirLength3, fs.getContentSummary(dir3).getLength());
        verifyLinkedFileIdenticial(fs, fStatus3, fs.getFileStatus(file3), content);
        
        /* 
         * delete /user/dir3/
         * verify no file3 or dir3 any more and verify the total DU
         */
         fs.delete(dir3, true);
         Assert.assertFalse(fs.exists(file3));
         Assert.assertFalse(fs.exists(dir3));
      } finally {
        fs.close();
        cluster.shutdown();
      }
    }
    
  private void verifyLinkedFileIdenticial(FileSystem fs, FileStatus f1,
      FileStatus f2, byte[] content) throws IOException {
    Assert.assertEquals(f1.getBlockSize(), f2.getBlockSize());
    Assert.assertEquals(f1.getAccessTime(), f2.getAccessTime());
    Assert.assertEquals(f1.getChildrenCount(), f2.getChildrenCount());
    Assert.assertEquals(f1.getLen(), f2.getLen());
    Assert.assertEquals(f1.getModificationTime(), f2.getModificationTime());
    Assert.assertEquals(f1.getReplication(), f2.getReplication());
    Assert.assertEquals(f1.getPermission(), f2.getPermission());

    checkFile(fs, f1.getPath(), (int) f1.getLen(), content);
    checkFile(fs, f2.getPath(), (int) f2.getLen(), content);
  }

  private void checkFile(FileSystem fs, Path name, int len, byte[] content)
      throws IOException {
    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[len];
    stm.readFully(0, actual);
    checkData(actual, 0, content, "check the content of the hard linked file "
        + name + " :");
    stm.close();
  }

  private void checkData(byte[] actual, int from, byte[] expected,
      String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message + " byte " + (from + idx) + " differs. expected "
          + expected[from + idx] + " actual " + actual[idx], expected[from
          + idx], actual[idx]);
      actual[idx] = 0;
    }
  }

}