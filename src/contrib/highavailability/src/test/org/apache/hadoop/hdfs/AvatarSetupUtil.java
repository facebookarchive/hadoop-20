package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.BeforeClass;
import org.junit.After;
import org.junit.AfterClass;

public class AvatarSetupUtil {
  
  final static Log LOG = LogFactory.getLog(AvatarSetupUtil.class);

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

  public void setUp(boolean federation, String name) throws Exception {
    setUp(federation, new Configuration(), name);
  }

  public void setUp(boolean federation, Configuration conf, String name)
    throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");   
    this.conf = conf;
    if (!federation) {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null);
      dafs = cluster.getFileSystem();
    } else {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null, 1, true);
      dafs = cluster.getFileSystem(0);
    }

    path = new Path(FILE_PATH);
    DFSTestUtil.createFile(dafs, path, FILE_LEN, (short) 1, 0L);
    Path hardlink1 = new Path("/hardlink1");
    Path hardlink2 = new Path("/hardlink2");
    DFSTestUtil.createFile(dafs, hardlink1, FILE_LEN, (short) 1,
        0L);
    dafs.hardLink(hardlink1, hardlink2);
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
