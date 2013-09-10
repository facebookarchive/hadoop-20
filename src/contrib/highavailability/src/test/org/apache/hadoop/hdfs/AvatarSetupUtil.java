package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.Ingest;
import org.apache.hadoop.hdfs.server.namenode.Standby;
import org.apache.hadoop.util.InjectionHandler;

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

  public void setUp(boolean federation, String name, int blockSize)
      throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("dfs.block.size", blockSize);
    conf.setBoolean("fs.ha.retrywrites", true);
    setUp(federation, conf, name, false);
  }

  public void setUp(boolean federation, String name) throws Exception {
    setUp(federation, name, new Configuration());
  }

  public void setUp(boolean federation, String name, Configuration conf)
      throws Exception {
    conf.setLong(FSEditLog.CONF_ROLL_TIMEOUT_MSEC, 500);
    setUp(federation, conf, name);
  }
  
  public void setUp(boolean federation, Configuration conf, String name)
      throws Exception {
    setUp(federation, conf, name, true);
  }

  public void setUp(boolean federation, Configuration conf, String name,
      boolean createFiles) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");   
    this.conf = conf;
    if (!federation) {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null);
      dafs = cluster.getFileSystem();
    } else {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null, 1, true);
      dafs = cluster.getFileSystem(0);
    }

    if (createFiles) {
      path = new Path(FILE_PATH);
      DFSTestUtil.createFile(dafs, path, FILE_LEN, (short) 1, 0L);
      Path hardlink1 = new Path("/hardlink1");
      Path hardlink2 = new Path("/hardlink2");
      DFSTestUtil.createFile(dafs, hardlink1, FILE_LEN, (short) 1,
          0L);
      dafs.hardLink(hardlink1, hardlink2);
    }
  }

  @After
  public void shutDown() throws Exception {
    if (dafs != null) {
      dafs.close();
    }
    if (cluster != null) {
      cluster.shutDown();
    }
    InjectionHandler.clear();
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
  
  /**
   * Check if ingest of the given node is running
   */
  public static boolean isIngestAlive(AvatarNode node) throws IOException {
    try {
      if (node.reportAvatar() == Avatar.ACTIVE) {
        return false;
      }
      Standby s = node.getStandby();
      Field ingestThreadField;

      ingestThreadField = Standby.class.getDeclaredField("ingestThread");
      ingestThreadField.setAccessible(true);
      Thread ingest = (Thread) ingestThreadField.get(s);
      
      return ingest.isAlive();
    } catch (Throwable t) {
      LOG.warn("Exception: ", t);
      throw new IOException("Failed to check ingest state");
    }
  }
}
