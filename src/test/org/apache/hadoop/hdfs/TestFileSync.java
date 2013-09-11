package org.apache.hadoop.hdfs;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ClientAdapter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progressable;

public class TestFileSync extends TestCase {
  public static final Log LOG =
    LogFactory.getLog(TestDFSClientRetries.class.getName());
  static final int BLOCK_SIZE = 1024;
  static final int BYTES_PER_CHECKSUM = 512;
  static final int BUFFER_SIZE = 512;
  static final int SINGLE_BYTE = 1;
  static final short REPLICATION_NUM = (short)3;
  private MiniDFSCluster cluster;
  private Configuration conf;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setInt("io.bytes.per.checksum", BYTES_PER_CHECKSUM);
    conf.setBoolean("dfs.support.append", true);
    conf.setBoolean("dfs.datanode.synconclose", true);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();

    super.tearDown();
  }

  /**
   * Test when enable forceSync, make sure the sync happens on the disk
   */
  public void testFileForceSync() throws Exception {
    DistributedFileSystem fileSystem =
	(DistributedFileSystem) cluster.getFileSystem();
    String filename = "/testFileForceSync";
    boolean forceSync = true;
    DFSClient dfsClient = ((DistributedFileSystem) fileSystem).getClient();
    DFSOutputStream out = (DFSOutputStream)dfsClient.create(
        filename, FsPermission.getDefault(), true, true, REPLICATION_NUM, BLOCK_SIZE,
        new Progressable() {
          @Override
          public void progress() {
          }
        },
        BUFFER_SIZE,
        forceSync,
        false     // doParallelWrites
      );

    //make sure it is an empty file at beginning
    long fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(0,fileSize);

    //write 1 byte size
    out.write(DFSTestUtil.generateSequentialBytes(0, SINGLE_BYTE));
    out.sync();

    //make sure that the data has been synced to data node disk
    fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(SINGLE_BYTE,fileSize);

    //write buffer size data to file
    out.write(DFSTestUtil.generateSequentialBytes(0, BUFFER_SIZE));
    out.sync();

    //make sure that the data has been synced to data node disk
    fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(SINGLE_BYTE+BUFFER_SIZE,fileSize);

    //write block size data to file; it will cross block boundary.
    out.write(DFSTestUtil.generateSequentialBytes(0, BLOCK_SIZE));
    out.sync();

	  //make sure that the data has been synced to data node disk
    fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(SINGLE_BYTE+BLOCK_SIZE+BUFFER_SIZE,fileSize);
    out.close();
  }

  /**
   * Test using forceSync on a FilterFileSystem; make sure the sync happens on the disk
   */
  public void testFilterFileSystemForceSync() throws Exception {
    DistributedFileSystem fileSystem =
	(DistributedFileSystem) cluster.getFileSystem();
    FilterFileSystem filterFS = new FilterFileSystem(fileSystem);
    String filename = "/testFileForceSync";
    Path path = new Path(filename);
    boolean forceSync = true;
    DFSClient dfsClient = ((DistributedFileSystem) fileSystem).getClient();
    FSDataOutputStream out = filterFS.create(
        path, FsPermission.getDefault(), true,
        BUFFER_SIZE, REPLICATION_NUM, (long)BLOCK_SIZE,
        BYTES_PER_CHECKSUM,
        new Progressable() {
          @Override
          public void progress() {
          }
        },
        forceSync
      );

    //make sure it is an empty file at beginning
    long fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(0,fileSize);

    //write 1 byte size
    out.write(DFSTestUtil.generateSequentialBytes(0, SINGLE_BYTE));
    out.sync();

    //make sure that the data has been synced to data node disk
    fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(SINGLE_BYTE,fileSize);

    //write buffer size data to file
    out.write(DFSTestUtil.generateSequentialBytes(0, BUFFER_SIZE));
    out.sync();

    //make sure that the data has been synced to data node disk
    fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(SINGLE_BYTE+BUFFER_SIZE,fileSize);

    //write block size data to file; it will cross block boundary.
    out.write(DFSTestUtil.generateSequentialBytes(0, BLOCK_SIZE));
    out.sync();

	  //make sure that the data has been synced to data node disk
    fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(SINGLE_BYTE+BLOCK_SIZE+BUFFER_SIZE,fileSize);
    out.close();
  }

  /**
   * Test writing all replicas of a file to datanodes in parallel.
   */
  public void testFileParallelWrites() throws Exception {
    DistributedFileSystem fileSystem =
	(DistributedFileSystem) cluster.getFileSystem();
    String filename = "/testFileParallelWrite";
    boolean doParallelWrites = true;
    DFSClient dfsClient = ((DistributedFileSystem) fileSystem).getClient();
    DFSOutputStream out = (DFSOutputStream)dfsClient.create(
        filename, FsPermission.getDefault(), true, true, REPLICATION_NUM, BLOCK_SIZE,
        new Progressable() {
          @Override
          public void progress() {
          }
        },
        BUFFER_SIZE,
        false,    // forceSync
        doParallelWrites
      );

    //make sure it is an empty file at beginning
    long fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(0,fileSize);

    //write 4 blocks into file
    out.write(DFSTestUtil.generateSequentialBytes(0, 4 * BLOCK_SIZE));
    out.close();

    //make sure that the data has been synced to data node disk
    fileSize = dfsClient.open(filename).getFileLength();
    assertEquals(4 * BLOCK_SIZE, fileSize);
  }
}
