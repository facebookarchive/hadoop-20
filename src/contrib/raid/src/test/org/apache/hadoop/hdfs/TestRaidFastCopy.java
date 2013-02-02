package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.FastCopy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestRaidFastCopy {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static String fileName = "core-site.xml";
  private static String confFile = null;
  private static String[] dirs = { "build/contrib/raid/test/",
      "../../../build/contrib/raid/test/" };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", 1024);
    conf.setClass("fs.hdfs.impl", DistributedRaidFileSystem.class,
        FileSystem.class);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
    // Writing conf to disk so that the FastCopy tool picks it up.
    boolean flag = false;
    for (String dir : dirs) {
      if (new File(dir).exists()) {
        confFile = dir + fileName;
        String tmpConfFile = confFile + ".tmp";
        FileOutputStream out = new FileOutputStream(tmpConfFile);
        conf.writeXml(out);
        out.close();
        //rename the xml 
        (new File(tmpConfFile)).renameTo(new File(confFile));
        flag = true;
      }
    }
    if (!flag) {
      throw new Exception("Could not write conf file");
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
    if (confFile != null) {
      new File(confFile).delete();
    }
  }

  @Test
  public void testRaidFastCopyCLI() throws Exception {
    String fileName = "/testRaidFastCopyCLI";
    String dst = fileName + "dst";
    DFSTestUtil.createFile(fs, new Path(fileName),
        20 * 1024, (short) 3, System.currentTimeMillis());
    String args[] = { fileName, dst };
    FastCopy.runTool(args);
    assertTrue(FastCopySetupUtil.compareFiles(fileName, fs, dst, fs));
  }
}
