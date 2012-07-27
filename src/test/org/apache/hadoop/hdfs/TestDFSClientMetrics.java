package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.metrics.DFSClientMetrics;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.conf.Configuration;
import junit.framework.TestCase;

public class TestDFSClientMetrics extends TestCase {
	private static Configuration CONF = new Configuration();
	private static final Path TEST_ROOT_DIR_PATH = 
		new Path(System.getProperty("test.build.data", "build/test/data"));
	private static final int FILE_LEN = 100;
	private static final int TEST_DIR_NUM = 5;
	private static final int TEST_FILE_NUM = 2;
	private MiniDFSCluster cluster;
	private DistributedFileSystem fs;
	private DFSClientMetrics metrics;
	private static Path getTestPath(String dirName) {
		return new Path(TEST_ROOT_DIR_PATH, dirName);
	}
	@Override
	protected void setUp() throws Exception{
	  CONF.setBoolean("dfs.client.metrics.enable", true);
		cluster = new MiniDFSCluster(CONF, 1, true, null);
		cluster.waitActive();
		fs = (DistributedFileSystem) cluster.getFileSystem();
		metrics = fs.getClient().getDFSClientMetrics();
	}

	@Override
	protected void tearDown() throws Exception {
		cluster.shutdown();
	}

	public void testCreateWriteDelete() throws Exception{
		for(int i = 0; i < TEST_DIR_NUM; ++i) {
			String path = "testDirectory" + i;
			fs.mkdirs(getTestPath(path));
			fs.delete(getTestPath(path), true);
		}
		assertEquals(TEST_DIR_NUM, 
				metrics.numCreateDirOps.getCurrentIntervalValue());

		for(int i = 0; i < TEST_FILE_NUM; ++i) {
			String file = "/tmp" + i +".txt";
			DFSTestUtil.createFile(fs, new Path(file), FILE_LEN, (short)1, 1L);
			fs.delete(new Path(file), false);
		}
		assertEquals(TEST_FILE_NUM, 
				metrics.writeOps.getCurrentIntervalValue());
		assertEquals(FILE_LEN * TEST_FILE_NUM, 
				metrics.writeSize.getCurrentIntervalValue());
		assertEquals(TEST_FILE_NUM, 
				metrics.numCreateFileOps.getCurrentIntervalValue());
	}

	public void testRead() throws Exception{
		for(int i = 0; i < TEST_FILE_NUM; ++i) {
			String file = "/tmp" + i +".txt";
			DFSTestUtil.createFile(fs, new Path(file), FILE_LEN, (short)5, 1L);
			
			DFSDataInputStream in = (DFSDataInputStream)fs.open(new Path(file));
			int numOfRead = 0;
			while(in.read() > 0){ 
				numOfRead ++;
			}
			assertEquals(FILE_LEN * (i+1), 
					metrics.readSize.getCurrentIntervalValue());
			assertEquals(numOfRead * (i+1), 
					metrics.readOps.getCurrentIntervalValue());			
		}
	}
}
