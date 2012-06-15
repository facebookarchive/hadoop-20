package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import junit.framework.TestCase;

public class TestDFSIsUnderConstruction extends TestCase {

  Configuration conf;
  MiniDFSCluster cluster;
  FileSystem fs;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 2, true, new String[]{"/rack1", "/rack2"});
    cluster.waitClusterUp();
    fs = cluster.getFileSystem();
  }

  @Override
  protected void tearDown() throws Exception {
    fs.close();
    cluster.shutdown();
    super.tearDown();
  }  

  public void testDFSIsUnderConstruction() throws Exception {
    
    Path growingFile = new Path("/growingFile"); 
    FSDataOutputStream fos = fs.create(growingFile);
    // fos.write("abcdefg".getBytes("UTF-8"));
    
    {
      FSDataInputStream fis = fs.open(growingFile);
      assertTrue("InputStream should be under construction.", fis.isUnderConstruction());
      fis.close();
    }
    
    fos.close();
    
    {
      FSDataInputStream fis = fs.open(growingFile);
      assertFalse("InputStream should NOT be under construction.", fis.isUnderConstruction());
      fis.close();
    }
  }
  
  public void testSecondLastBlockNotReceived() throws Exception {
    String fileName = "/testSecondLastBlockNotReceived";
    Path growingFile = new Path(fileName); 
    FSDataInputStream fis = null;
    FSDataOutputStream fos = fs.create(growingFile, false, 1024, (short)1, 1024);
    try {
      int fileLength = 2096;
      AppendTestUtil.write(fos, 0, fileLength);
      fos.sync();

      fis = fs.open(growingFile);
      for (int i = 0; i < fileLength; i++) {
        fis.read();
      }
      fis.close();

      FSNamesystem fsns = cluster.getNameNode().namesystem;
      INode[] inodes = fsns.dir.getExistingPathINodes(fileName);
      BlockInfo[] bis = ((INodeFile) (inodes[inodes.length - 1])).getBlocks();
      bis[bis.length - 2].setNumBytes(1);

      try {
        fis = fs.open(growingFile);
        TestCase.fail();
      } catch (IOException e) {
      }
      bis[bis.length - 2].setNumBytes(1024);

      bis[bis.length - 1].setNumBytes(1);
      fis = fs.open(growingFile);
      for (int i = 0; i < fileLength; i++) {
        fis.read();
      }
    } finally {
      if (fos != null) {
        fos.close();
      }
      if (fis != null) {
        fis.close();
      }
    }
  }
}
