package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.BeforeClass;
import org.junit.After;
import org.junit.AfterClass;

public class AvatarSetupUtil {

  protected static final String FILE_PATH = "/dir1/dir2/myfile";
  private static final long FILE_LEN = 512L * 1024L;

  protected Configuration conf;
  protected MiniAvatarCluster cluster;
  protected DistributedAvatarFileSystem dafs;
  protected Path path;

  @BeforeClass
  public static void setUpStatic() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp(boolean federation) throws Exception {
    setUp(federation, new Configuration(), false);
  }

  public void setUp(boolean federation, Configuration conf) throws Exception {
    setUp(federation, conf, false);
  }

  public void setUp(boolean federation, boolean shortFBR) throws Exception {
    setUp(federation, new Configuration(), shortFBR);
  }

  public void setUp(boolean federation, Configuration conf, boolean shortFBR)
    throws Exception {
    this.conf = conf;
    if (shortFBR) {
      conf.setInt("dfs.datanode.fullblockreport.delay", 1000);
    }
    if (!federation) {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null);
      dafs = cluster.getFileSystem();
    } else {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null, 1, true);
      dafs = cluster.getFileSystem(0);
    }

    path = new Path(FILE_PATH);
    DFSTestUtil.createFile(dafs, path, FILE_LEN, (short) 1, 0L);
  }

  @After
  public void shutDown() throws Exception {
    dafs.close();
    cluster.shutDown();
  }

  @AfterClass
  public static void shutDownStatic() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  static int blocksInFile(FileSystem fs, Path path, long len)
      throws IOException {
    FileStatus f = fs.getFileStatus(path);
    return fs.getFileBlockLocations(f, 0L, len).length;
  }

  protected int blocksInFile() throws IOException {
    return blocksInFile(dafs, path, FILE_LEN);
  }
}
