package org.apache.hadoop.hdfs.server.datanode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;

public class TestParallelBlockScan extends TestCase {
  MiniDFSCluster cluster;
  static final int BASE_BLOCK = 512;
  static final int BLOCKS_PER_FILE = 150;

  private MiniDFSCluster startMiniDFSWithScanThreads(int parallelism,
      boolean format) throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("dfs.datanode.blockscanner.threads", parallelism);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, format, null);
    return cluster;
  }

  public void testParallelBlockScan() throws IOException {
    cluster = startMiniDFSWithScanThreads(1, true);

    FileSystem fs = cluster.getFileSystem();
    writeSomeFiles(fs);

    FSDataset dataset = (FSDataset) cluster.getDataNodes().get(0).data;
    int nsId = cluster.getNameNode().getNamespaceID();
    Block[] blockReportSequential = dataset.getBlockReport(nsId);
    cluster.shutdown();
    cluster = startMiniDFSWithScanThreads(2, false); // Reuse the data written
    // in #1
    dataset = (FSDataset) cluster.getDataNodes().get(0).data;
    nsId = cluster.getNameNode().getNamespaceID();
    Block[] blockReportParallel = dataset.getBlockReport(nsId);

    assertEquals(blockReportSequential.length, blockReportParallel.length);
    for (int i = 0; i < blockReportSequential.length; i++) {
      assertEquals(blockReportSequential[i], blockReportSequential[i]);
    }
    
    cluster.shutdown();
  }

  private void writeSomeFiles(FileSystem fs) throws IOException {
    for (int id = 1; id <= 3; id++) {
      DataOutputStream outStream = fs.create(new Path("/tmp/file" + id), false,
          1024, (short) 1, blockLen(id));
      Random rand = new Random();

      byte[] buff = new byte[(int)blockLen(id)];
      
      for (int i = 0; i < BLOCKS_PER_FILE; i++) {
        rand.nextBytes(buff);
        outStream.write(buff);
      }
      outStream.close();
    }
  }

  long blockLen(int id) {
    return BASE_BLOCK * id;
  }

}
