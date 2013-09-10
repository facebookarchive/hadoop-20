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
import java.net.URI;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DeleteUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.log4j.Level;
import org.junit.Assert;

public class TestDistributedFileSystem extends junit.framework.TestCase {
  private static final Random RAN = new Random();
  
  public static void createFile(FileSystem fs, Path path) throws IOException {
    FSDataOutputStream stm = fs
        .create(path, true, 4096, (short) 1, (long) 1024);
    stm.close();
  }

  
  public void testDeleteSkipTrash() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);

    try {
      FileSystem fs = cluster.getFileSystem();
      TestCase.assertNotNull(DFSUtil.convertToDFS(fs));
      
      createFile(fs, new Path("/test/testDelete"));
      try {
        fs.delete(new Path("/test"), false, true);
        TestCase.fail();
      } catch (IOException e) {
        
      }
      TestCase.assertTrue(fs.delete(new Path("/test"), true, true));

      createFile(fs, new Path("/test1/testDelete"));
      TestCase
          .assertTrue(fs.delete(new Path("/test1/testDelete"), false, true));
      TestCase.assertTrue(fs.delete(new Path("/test1"), false, true));
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }    
  }
  
  public void testDeleteUsingTrash() throws IOException {
    Configuration conf = new Configuration();
    conf.setFloat("fs.trash.interval", 60);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);

    try {
      FileSystem fs = cluster.getFileSystem();
      TestCase.assertNotNull(DFSUtil.convertToDFS(fs));
      
      createFile(fs, new Path("/test/testDelete"));
      TestCase.assertTrue(fs.delete(new Path("/test/testDelete"), false));
      TestCase.assertTrue(fs.undelete(new Path("/test/testDelete"),
          System.getProperty("user.name")));

      fs.mkdirs(new Path("/tmp"));
      TestCase.assertTrue(fs.delete(new Path("/tmp"), false));
      TestCase.assertFalse(fs.undelete(new Path("/tmp"),
          System.getProperty("user.name")));

      // temp directory automatically skips trash.
      createFile(fs, new Path("/tmp/testDelete"));
      TestCase.assertTrue(fs.delete(new Path("/tmp/testDelete"), false));
      TestCase.assertFalse(fs.undelete(new Path("/tmp/testDelete"),
          System.getProperty("user.name")));

      fs.getConf().set("fs.trash.tmp.path.pattern", "{/tmp,/dir/,/dir1/*/dir3//}");
      createFile(fs, new Path("/dir1/dir2/dir3/dir5/file1"));
      TestCase.assertTrue(fs.delete(new Path("/dir1/dir2/dir3/dir5/file1"),
          false));
      TestCase.assertFalse(fs.undelete(new Path("/dir1/dir2/dir3/dir5/file1"),
          System.getProperty("user.name")));
      fs.mkdirs(new Path("/dir1/dir5/dir3/"));
      TestCase.assertTrue(fs.delete(new Path("/dir1/dir5/dir3/"),
          true));
      TestCase.assertFalse(fs.undelete(new Path("/dir1/dir5/dir3/"),
          System.getProperty("user.name")));
      

      createFile(fs, new Path("/tmp/testDelete"));
      TestCase.assertTrue(fs.delete(new Path("/tmp/testDelete"), false));
      TestCase.assertFalse(fs.undelete(new Path("/tmp/testDelete"),
          System.getProperty("user.name")));

      createFile(fs, new Path("/test/tmp/testDelete"));
      TestCase.assertTrue(fs.delete(new Path("/test/tmp/testDelete"), false));
      TestCase.assertTrue(fs.undelete(new Path("/test/tmp/testDelete"),
          System.getProperty("user.name")));
      
      // Test another delete() overload method with skipTrash=false
      fs.delete(new Path("/tmp"), true, true);
      fs.delete(new Path("/test"), true, true);

      createFile(fs, new Path("/test/testDelete"));
      // non empty directory is not moved if recursive is not set
      TestCase.assertFalse(fs.delete(new Path("/test"), false, false));

      // Files are expected to move to trash.
      TestCase.assertTrue(fs.delete(new Path("/test"), true, false));
      TestCase.assertTrue(fs.undelete(new Path("/test"),
          System.getProperty("user.name")));
      createFile(fs, new Path("/test1/testDelete"));
      TestCase.assertTrue(fs
          .delete(new Path("/test1/testDelete"), false, false));
      TestCase.assertTrue(fs.delete(new Path("/test1"), false, false));
      TestCase.assertTrue(fs.undelete(new Path("/test1"),
          System.getProperty("user.name")));
      TestCase.assertTrue(fs.undelete(new Path("/test1/testDelete"),
          System.getProperty("user.name")));

      // temp directory automatically skips trash.
      createFile(fs, new Path("/tmp/testDelete"));
      TestCase.assertTrue(fs.delete(new Path("/tmp/testDelete"), false, false));
      TestCase.assertFalse(fs.undelete(new Path("/tmp/testDelete"),
          System.getProperty("user.name")));
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }



  public void testFileSystemCloseAll() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);
    URI address = FileSystem.getDefaultUri(conf);

    try {
      FileSystem.closeAll();

      conf = new Configuration();
      FileSystem.setDefaultUri(conf, address);
      FileSystem.get(conf);
      FileSystem.get(conf);
      FileSystem.closeAll();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Tests DFSClient.close throws no ConcurrentModificationException if 
   * multiple files are open.
   */
  public void testDFSClose() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fileSys = cluster.getFileSystem();

    try {
      // create two files
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.create(new Path("/test/dfsclose/file-1"));

      fileSys.close();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  public void testDFSClient() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final Path filepath = new Path("/test/LeaseChecker/foo");
      final long millis = System.currentTimeMillis();

      {
        DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
        assertFalse(dfs.dfs.isLeaseCheckerStarted());
  
        //create a file
        FSDataOutputStream out = dfs.create(filepath);
        assertTrue(dfs.dfs.isLeaseCheckerStarted());
  
        //write something and close
        out.writeLong(millis);
        assertTrue(dfs.dfs.isLeaseCheckerStarted());
        out.close();
        assertTrue(dfs.dfs.isLeaseCheckerStarted());
        dfs.close();
      }

      {
        DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
        assertFalse(dfs.dfs.isLeaseCheckerStarted());

        //open and check the file
        FSDataInputStream in = dfs.open(filepath);
        assertFalse(dfs.dfs.isLeaseCheckerStarted());
        assertEquals(millis, in.readLong());
        assertFalse(dfs.dfs.isLeaseCheckerStarted());
        in.close();
        assertFalse(dfs.dfs.isLeaseCheckerStarted());
        dfs.close();
      }
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  public void testFileChecksum() throws IOException {
    testFileChecksumInternal(false);
    testFileChecksumInternal(false);
  }

  public void testFileChecksumInlineChecksum() throws IOException {
    testFileChecksumInternal(true);
    testFileChecksumInternal(true);
  }

  private void testFileChecksumInternal(boolean inlineChecksum) throws IOException {
    MiniDFSCluster cluster = null; 
    try {
      ((Log4JLogger)HftpFileSystem.LOG).getLogger().setLevel(Level.ALL);
  
      final long seed = RAN.nextLong();
      System.out.println("seed=" + seed);
      RAN.setSeed(seed);
  
      final Configuration conf = new Configuration();
      conf.set(FSConstants.SLAVE_HOST_NAME, "localhost");
      conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);
   
      cluster = new MiniDFSCluster(conf, 2, true, null);;
      final FileSystem hdfs = cluster.getFileSystem();
      final String hftpuri = "hftp://" + conf.get("dfs.http.address");
      System.out.println("hftpuri=" + hftpuri);
      final FileSystem hftp = new Path(hftpuri).getFileSystem(conf);

      final String dir = "/filechecksum";
      final int block_size = 1024;
      final int buffer_size = conf.getInt("io.file.buffer.size", 4096);
      conf.setInt("io.bytes.per.checksum", 512);

      // Check non-existent file
      final Path nonExistentPath = new Path("/non_existent");
      assertFalse(hdfs.exists(nonExistentPath));
      try {
        hdfs.getFileChecksum(nonExistentPath);
        Assert.fail("GetFileChecksum should fail on non-existent file");
      } catch (IOException e) {
        assertTrue(e.getMessage().startsWith(
          "Null block locations, mostly because non-existent file"));
      }
      //try different number of blocks
      for(int n = 0; n < 5; n++) {
        //generate random data
        final byte[] data = new byte[RAN.nextInt(block_size/2-1)+n*block_size+1];
        RAN.nextBytes(data);
        System.out.println("data.length=" + data.length);
  
        //write data to a file
        final Path foo = new Path(dir, "foo" + n);
        {
          final FSDataOutputStream out = hdfs.create(foo, false, buffer_size,
               (short)2, block_size);
          out.write(data);
          out.close();
        }
      
        //compute checksum
        final FileChecksum hdfsfoocs = hdfs.getFileChecksum(foo);
        System.out.println("hdfsfoocs=" + hdfsfoocs);

        final FileChecksum hftpfoocs = hftp.getFileChecksum(foo);
        System.out.println("hftpfoocs=" + hftpfoocs);


        final Path qualified = new Path("hftp://127.0.0.1"
            + hftpuri.substring(hftpuri.lastIndexOf(':')) + dir, "foo" + n);
        final FileChecksum qfoocs = hftp.getFileChecksum(qualified);
        System.out.println("qfoocs=" + qfoocs);

        // write another file
        final Path bar = new Path(dir, "bar" + n);
        {
          final FSDataOutputStream out = hdfs.create(bar, false, buffer_size,
              (short) 2, block_size);
          out.write(data);
          out.close();
        }

        { // verify checksum
          final FileChecksum barcs = hdfs.getFileChecksum(bar);
          final int barhashcode = barcs.hashCode();
          assertEquals(hdfsfoocs.hashCode(), barhashcode);
          assertEquals(hdfsfoocs, barcs);

          assertEquals(hftpfoocs.hashCode(), barhashcode);
          assertEquals(hftpfoocs, barcs);

          assertEquals(qfoocs.hashCode(), barhashcode);
          assertEquals(qfoocs, barcs);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  public void testUnfavoredNodes() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.client.block.location.renewal.enabled", false);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);

    try {
      FileSystem fs = cluster.getFileSystem();
      DistributedFileSystem dfs = DFSUtil.convertToDFS(fs);
      TestCase.assertNotNull(dfs);

      Path path = new Path("/testUnfavoredNodes");
      FSDataOutputStream stm = fs
          .create(path, true, 4096, (short) 2, (long) 2048);
      stm.write(new byte[4096]);
      stm.close();
      
      FSDataInputStream is = fs.open(path);
      DFSDataInputStream dis = (DFSDataInputStream) is;
      TestCase.assertNotNull(dis);

      is.read(new byte[1024]);
      DatanodeInfo currentDn1 = dis.getCurrentDatanode();
      dis.setUnfavoredNodes(Arrays.asList(new DatanodeInfo[] { currentDn1 }));

      is.read(new byte[512]);
      DatanodeInfo currentDn2 = dis.getCurrentDatanode();
      TestCase.assertTrue(!currentDn2.equals(currentDn1));
      dis.setUnfavoredNodes(Arrays.asList(new DatanodeInfo[] { currentDn1, currentDn2 }));

      is.read(new byte[512]);
      TestCase.assertEquals(currentDn1, dis.getCurrentDatanode());
      
      is.read(new byte[1024]);
      
      TestCase.assertEquals(dis.getAllBlocks().get(1).getLocations()[0],
          dis.getCurrentDatanode());
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }    
  }
}
