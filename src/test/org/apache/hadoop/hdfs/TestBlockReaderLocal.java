package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * BlockReaderLocal Tests
 */
public class TestBlockReaderLocal {

  private static final Random random = new Random();
  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_SIZE = 4000;

  // writes a random file of given seed, size and path in the FileSystem fs
  // and returns the written bytes
  private byte[] writeRandomFile(FileSystem fs, Path path, int seed, int size)
      throws IOException {
    byte[] fileData = new byte[size];
    new Random(seed).nextBytes(fileData);

    FSDataOutputStream outputStream = fs.create(path, true,
        fs.getConf().getInt("io.file.buffer.size", 4096),
        (short) 1, BLOCK_SIZE);
    try {
      outputStream.write(fileData);
    } finally {
      outputStream.close();
    }
    return fileData;
  }

  private void assertDir(String path, FileSystem fs, FileSystem... rest) 
      throws IOException {
    Path p = new Path(path);
    assertTrue(path + " should be a directory", fs.getFileStatus(p).isDir());
    for (FileSystem fileSystem : rest) {
      assertTrue(path + " should be a directory", 
          fileSystem.getFileStatus(p).isDir());
    }
  }

  public void assertFile(FileSystem fs, Path path, int fileSize, 
                         byte[] fileData) throws IOException {
    
    assertTrue(path + " is not a file", fs.isFile(path));
    assertEquals("file size do not match for " + path, fileSize, 
        fs.getContentSummary(path).getLength());
    
    FSDataInputStream inputStream = fs.open(path);
    try {
      byte[] readFileData = new byte[fileSize];
      inputStream.readFully(readFileData);
      assertArrayEquals("file data do not match for " +
          path, fileData, readFileData);
    } finally {
      inputStream.close();
    }
  }

  private Configuration buildConfigurationFor(String dfsClusterId) {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.read.shortcircuit", true);
    conf.setBoolean("dfs.client.read.shortcircuit.skip.checksum", false);
    if (dfsClusterId != null) {
      conf.set(MiniDFSCluster.DFS_CLUSTER_ID, dfsClusterId);
    }
    return conf;
  }

  private MiniDFSCluster newColocatedMiniDFSCluster(String dfsClusterId) 
      throws IOException {
    
    Configuration conf = buildConfigurationFor(dfsClusterId);
    return new MiniDFSCluster(conf, 2, true, null);
  }

  private MiniDFSCluster newFederatedMiniDFSCluster(String dfsClusterId)
      throws IOException {
    
    Configuration conf = buildConfigurationFor(dfsClusterId);
    return new MiniDFSCluster(conf, 2, true, null, 2);
  }

  private void initBlockIdRandomizer(long seed) {

    Class klass = FSNamesystem.class;
    String randomizerFieldName = "randBlockId";
    try {
      Field field = klass.getDeclaredField(randomizerFieldName);
      field.setAccessible(true);
      field.set(null, new Random(seed));
    } catch (Exception e) {
      Assert.fail("Could not reset Block randomizer field -> "
          + klass.getSimpleName() + "." + randomizerFieldName +
          " " + e.getClass().getName() + ": " + e.getLocalizedMessage());
    }

  }

  /**
   * Test to ensure that namespace id is used for caching block path. 
   * We use an identical random seed for generating block id sequences for two
   * file systems in two different cluster. Then we store two different files 
   * in each filesystem and assert that they we get the files stored.
   * Observe that the block ids for each of these files are same. So, to 
   * retrieve the random bits we
   * stored, namesapce of each filesystem must be accounted for caching blocks.
   *
   * sequences
   * @throws IOException
   */
  @Test
  public void testColocatedNamespaceId() throws IOException {
    String prefix = getClass().getSimpleName() + "-colocated-";
    MiniDFSCluster cluster1 = newColocatedMiniDFSCluster(prefix +
        random.nextInt(Integer.MAX_VALUE));
    MiniDFSCluster cluster2 = newColocatedMiniDFSCluster(prefix +
        random.nextInt(Integer.MAX_VALUE));


    FileSystem fs1 = cluster1.getFileSystem();
    FileSystem fs2 = cluster2.getFileSystem();
    try {
      // check that / exists
      assertDir("/", fs1, fs2);
      Path file = new Path("file.dat");
      initBlockIdRandomizer(0xcafebabe);
      byte[] fileData1 = writeRandomFile(fs1, file, 0xABADCAFE, FILE_SIZE);

      initBlockIdRandomizer(0xcafebabe);
      byte[] fileData2 = writeRandomFile(fs2, file, 0xABADCAFE, FILE_SIZE);

      assertFile(fs1, file, FILE_SIZE, fileData1);
      assertFile(fs2, file, FILE_SIZE, fileData2);
    } finally {
      fs1.close();
      cluster1.shutdown();
      fs2.close();
      cluster2.shutdown();
    }
  }

  /**
   * We store two files in two different HDFS clusters at the same path and
   * ensure that we can retrieve them successfully.
   *
   * @throws IOException
   */
  @Test
  public void testColocatedClustersIpcPorts() throws IOException {
    String prefix = getClass().getSimpleName() + "-colocated-";
    MiniDFSCluster cluster1 = newColocatedMiniDFSCluster(prefix +
        random.nextInt(Integer.MAX_VALUE));
    MiniDFSCluster cluster2 = newColocatedMiniDFSCluster(prefix +
        random.nextInt(Integer.MAX_VALUE));


    FileSystem fs1 = cluster1.getFileSystem();
    FileSystem fs2 = cluster2.getFileSystem();
    try {
      // check that / exists
      assertDir("/", fs1, fs2);

      Path file = new Path("file.dat");
      byte[] fileData1 = writeRandomFile(fs1, file, 0x1234, FILE_SIZE);
      byte[] fileData2 = writeRandomFile(fs2, file, 0xabcd, FILE_SIZE);

      assertFile(fs1, file, FILE_SIZE, fileData1);
      assertFile(fs2, file, FILE_SIZE, fileData2);
    } finally {
      fs1.close();
      cluster1.shutdown();
      fs2.close();
      cluster2.shutdown();
    }
  }

  //Federated tests
  @Test
  public void testFederatedNamespaceId() throws IOException {
    String prefix = getClass().getSimpleName() + "-federated-";
    MiniDFSCluster cluster = newFederatedMiniDFSCluster(prefix +
        random.nextInt(Integer.MAX_VALUE));

    FileSystem fs1 = cluster.getFileSystem(0);
    FileSystem fs2 = cluster.getFileSystem(1);
    try {
      // check that / exists
      assertDir("/", fs1, fs2);
      Path file = new Path("file.dat");
      initBlockIdRandomizer(0xcafebabe);
      byte[] fileData1 = writeRandomFile(fs1, file, 0xABADCAFE, FILE_SIZE);

      initBlockIdRandomizer(0xcafebabe);
      byte[] fileData2 = writeRandomFile(fs2, file, 0xABADCAFE, FILE_SIZE);

      assertFile(fs1, file, FILE_SIZE, fileData1);
      assertFile(fs2, file, FILE_SIZE, fileData2);
    } finally {
      fs1.close();
      cluster.shutdown();
      fs2.close();
    }
  }

  @Test
  public void testFederatedClustersIpcPorts() throws IOException {
    String prefix = getClass().getSimpleName() + "-federated-";
    MiniDFSCluster cluster = newFederatedMiniDFSCluster(prefix +
        random.nextInt(Integer.MAX_VALUE));


    FileSystem fs1 = cluster.getFileSystem(0);
    FileSystem fs2 = cluster.getFileSystem(1);
    try {
      // check that / exists
      assertDir("/", fs1, fs2);

      Path file = new Path("file.dat");
      byte[] fileData1 = writeRandomFile(fs1, file, 0x1234, FILE_SIZE);
      byte[] fileData2 = writeRandomFile(fs2, file, 0xabcd, FILE_SIZE);

      assertFile(fs1, file, FILE_SIZE, fileData1);
      assertFile(fs2, file, FILE_SIZE, fileData2);
    } finally {
      fs1.close();
      cluster.shutdown();
      fs2.close();
    }
  }

}

