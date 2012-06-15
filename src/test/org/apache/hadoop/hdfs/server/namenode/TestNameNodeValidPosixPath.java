package org.apache.hadoop.hdfs.server.namenode;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.util.DefaultPathNameChecker;
import org.apache.hadoop.hdfs.util.PathNameChecker;
import org.apache.hadoop.hdfs.util.PosixPathNameChecker;

import java.io.IOException;

/**
 * Test that the create function throws and IOException if called with a
 * NonCompliant path
 */
public class TestNameNodeValidPosixPath extends TestCase {

  public void testCreateNonPosixPath() throws IOException{
    Configuration conf = new Configuration();
    conf.setClass("dfs.util.pathname.checker.class", PosixPathNameChecker
      .class, PathNameChecker.class);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    NameNode namenode = cluster.getNameNode();
    String wrongPath = "/hdfs:///abs/ddd.dd";
    long blockSize = 16;
    short replication = 3;
    boolean exceptionThrown = false;

    try {
      namenode.create(wrongPath, FsPermission.getDefault(), null, true, true,
        replication, blockSize);
    } catch (IOException e) {
      exceptionThrown = true;
      System.out.println(e.getMessage());
    }
    assertTrue("Exception Thrown", exceptionThrown);

    String correctPath = "/ddddd/ab.c";
    exceptionThrown = false;
    try {
      namenode.create(correctPath, FsPermission.getDefault(), "testclient",
        true, true, replication, blockSize);
    } catch (IOException ex) {
      exceptionThrown = true;
      System.out.println(ex.getMessage());
    }
    System.out.println(exceptionThrown);
    assertFalse("No Exception Thrown",exceptionThrown);

  }
}
