package org.apache.hadoop.hdfs;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.ipc.RemoteException;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCloseFile {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static int BLOCK_SIZE = 1024;
  private static int BLOCKS = 20;
  private static int FILE_LEN = BLOCK_SIZE * BLOCKS;
  private static FileSystem fs;
  private static Random random = new Random();
  private static volatile boolean pass = true;
  private static Log LOG = LogFactory.getLog(TestCloseFile.class);
  private static int CLOSE_FILE_TIMEOUT = 20000;

  @BeforeClass
    public static void setUpBeforeClass() throws Exception {
      conf = new Configuration();
      conf.setBoolean("dfs.permissions", false);
      conf.setInt("dfs.client.closefile.timeout", CLOSE_FILE_TIMEOUT);
      cluster = new MiniDFSCluster(conf, 3, true, null);
      fs = cluster.getFileSystem();
    }

  @AfterClass
    public static void tearDownAfterClass() throws Exception {
      cluster.shutdown();
    }

  public void testRestartNameNode(boolean waitSafeMode) throws Exception {
    String file = "/testRestartNameNode" + waitSafeMode;

    // Create a file and write data.
    FSDataOutputStream out = fs.create(new Path(file));
    String clientName = ((DistributedFileSystem) fs).getClient().getClientName();
    byte[] buffer = new byte[FILE_LEN];
    random.nextBytes(buffer);
    out.write(buffer);
    ((DFSOutputStream) out.getWrappedStream()).sync();

    // Now shutdown the namenode and try to close the file.
    cluster.shutdownNameNode(0);
    Thread closeThread = new CloseThread(out, file, clientName);
    closeThread.start();
    Thread.sleep(CLOSE_FILE_TIMEOUT / 4);

    // Restart the namenode and verify the close file worked.
    if (!waitSafeMode) {
      cluster.restartNameNode(0, new String[]{}, false);
      cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    } else {
      cluster.restartNameNode(0);
    }
    closeThread.join(5000);
    assertTrue(pass);
  }

  @Test
  public void testRestartNameNodeWithSafeMode() throws Exception {
    testRestartNameNode(true);
  }

  @Test
  public void testRestartNameNodeWithoutSafeMode() throws Exception {
    testRestartNameNode(false);
  }

  private class CloseThread extends Thread {
    private final FSDataOutputStream out;
    private final String file;
    private final String clientName;

    public CloseThread(FSDataOutputStream out, String file, String clientName) {
      this.out = out;
      this.file = file;
      this.clientName = clientName;
    }

    public void run() {
      try {
        out.close();
      } catch (Exception e) {
        LOG.warn("Close failed", e);
        while (true) {
          try {
            Thread.sleep(1000);
            cluster.getNameNode().recoverLease(file, clientName);
            break;
          } catch (SafeModeException se) {
            LOG.warn("Retrying lease recovery for failed close", se);
          } catch (Exception ee) {
            LOG.warn("Lease recovery failed", ee);
            pass = false;
          }
        }
      }
    }
  }
}
