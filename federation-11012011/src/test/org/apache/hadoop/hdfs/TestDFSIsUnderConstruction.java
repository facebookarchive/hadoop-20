package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
}
