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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import junit.framework.TestCase;

public class TestFileStatusCache extends TestCase{

  private int fileStatusCacheExpireTime;
  private int fileStatusCacheSize;
  private int fileSize = 16384;
  private int blockSize = 8192;
  private int seed;
  
  private void writeFile(FileSystem fileSys, Path name, int repl,
                         int fileSize, int blockSize)
  throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
                             fileSys.getConf().getInt("io.file.buffer.size", 4096),
                             (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  public void testFileStatusCache() throws IOException {
    Configuration conf = new Configuration();
    
    fileStatusCacheExpireTime = 1000;
    conf.setInt("dfs.filestatus.cache.expiretime", fileStatusCacheExpireTime);
    fileStatusCacheSize = 100;
    conf.setInt("dfs.filestatus.cache.size", fileStatusCacheSize);
    
    conf.setBoolean("dfs.filestatus.cache.enable", true);
    
    conf.setInt("dfs.ls.limit", 2);

    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    
    try {
      cluster = new MiniDFSCluster(conf, 1, true, null);
      fs = cluster.getFileSystem();
      final DFSClient dfsClient = new DFSClient(NameNode.getClientProtocolAddress(conf), conf);

      // Make sure getFileInfo returns null for files which do not exist
      FileStatus fileInfo = dfsClient.getFileInfo("/");
      
      Path file1 = new Path("/filestatus.dat");

      writeFile(fs, file1, 1, fileSize, blockSize);
      
      fileInfo = dfsClient.getFileInfo("/filestatus.dat");
      assertTrue(file1 + " should be a file", fileInfo.isDir() == false);
      assertTrue(fileInfo.getBlockSize() == blockSize);
      assertTrue(fileInfo.getReplication() == 1);
      assertTrue(fileInfo.getLen() == fileSize);
      
      fileInfo = dfsClient.getFileInfo("/");
      // Make sure the null cache works
      assertTrue(fileInfo.getChildrenCount() == 0);
      
      // Make sure the cache expires
      Thread.currentThread().sleep(fileStatusCacheExpireTime);
      
      fileInfo = dfsClient.getFileInfo("/");
      
      // Make sure the cache expires and update correctly
      assertTrue(fileInfo.getChildrenCount() == 1);
          
      for(int i = 1; i <= fileStatusCacheSize; i++) {
        file1 = new Path("/TestFileStatus.dat" + new Integer(i).toString());
        writeFile(fs, file1, 1, fileSize, blockSize);
        
        fileInfo = dfsClient.getFileInfo("/TestFileStatus.dat" + new Integer(i).toString());
        assertTrue(file1 + " should be a file", fileInfo.isDir() == false);
        assertTrue(fileInfo.getBlockSize() == blockSize);
        assertTrue(fileInfo.getReplication() == 1);
        assertTrue(fileInfo.getLen() == fileSize);
      }
      
      fileInfo = dfsClient.getFileInfo("/");
      // Make sure the oldest entry is replaced
      assertTrue(fileInfo.getChildrenCount() == fileStatusCacheSize + 1);        
    }
    catch(InterruptedException ie) {
      System.out.println("Fails to sleep!");
    }
    finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }      
    }
  } 
}
