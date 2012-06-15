package org.apache.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestOpenFilesInfo {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private static final int MAX_BLOCKS = 10;
  private static final int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  private static final Random random = new Random();

  @Before
  public void setUpBefore() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDownAfterClass() throws Exception {
    fs.close();
    cluster.shutdown();
  }

  private static void createOpenFiles(int nFiles, String prefix) throws Exception {
    for (int i = 0; i < nFiles; i++) {
      FSDataOutputStream out = fs.create(new Path("/" + prefix + random.nextInt()));
      byte[] buffer = new byte[random.nextInt(MAX_FILE_SIZE)];
      random.nextBytes(buffer);
      out.write(buffer);
      out.sync();
    }
  }

  private static void verifyLease(OpenFilesInfo info) {
    String clientName = ((DistributedFileSystem) fs).getClient()
        .getClientName();
    for (FileStatusExtended stat : info.getOpenFiles()) {
      assertEquals(clientName, stat.getHolder());
    }
  }

  @Test
  public void testBasic() throws Exception {
    createOpenFiles(10, "testBasic");
    FSNamesystem ns = cluster.getNameNode().namesystem;
    OpenFilesInfo info = ns.getOpenFiles();
    assertNotNull(info);
    assertEquals(10, info.getOpenFiles().size());
    assertEquals(ns.getGenerationStamp(), info.getGenStamp());
    verifyLease(info);
  }

  @Test
  public void testSerialize() throws Exception {
    createOpenFiles(10, "testSerialize");
    FSNamesystem ns = cluster.getNameNode().namesystem;
    OpenFilesInfo info = ns.getOpenFiles();
    // Serialize object
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bout);
    info.write(out);
    
    // Deserialize object.
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    DataInputStream in = new DataInputStream(bin);
    OpenFilesInfo info1 = new OpenFilesInfo(); 
    info1.readFields(in);
    
    // Verify and cleanup.
    verifyLease(info);
    assertEquals(info, info1);
    bout.close();
    bin.close();
    out.close();
    in.close();
  }
}
