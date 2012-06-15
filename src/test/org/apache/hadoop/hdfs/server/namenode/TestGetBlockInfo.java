package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithFileName;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import junit.framework.TestCase;

public class TestGetBlockInfo extends TestCase {
	
	/**
	 * Test the get block info
	 * 
	 * @throws IOException
	 */
	public void testGetBlockInfo1() throws IOException {
		Configuration conf = new Configuration();
    MiniDFSCluster cluster =
      new MiniDFSCluster(conf, 5, true, null);
    final String pathStr = "/test/testGetBlockInfo1";
    final int FILE_LEN = 123;

    long nonExistedBlockId = 20120330;
    try {
    	FileSystem fs = cluster.getFileSystem();
    	Path path = new Path(pathStr);
    	
    	cluster.waitActive();
    	
    	DFSTestUtil.createFile(fs, path, FILE_LEN, (short)3, 0);
    	
    	DFSTestUtil.waitReplication(fs, path, (short)3);
      NameNode nn = cluster.getNameNode();
      LocatedBlocks located = nn.getBlockLocations(pathStr, 0, FILE_LEN);
      
      // Get the original block locations
      List<LocatedBlock> blocks = located.getLocatedBlocks();
      LocatedBlock firstBlock = blocks.get(0);
      long blockId = firstBlock.getBlock().getBlockId();      

      LocatedBlockWithFileName locatedBlockWithFileName = 
      		nn.getBlockInfo(blockId);
      
      assertEquals(pathStr, locatedBlockWithFileName.getFileName());
      assertEquals(3, locatedBlockWithFileName.getLocations().length);

      // Test the deleted block id
      fs.delete(path, true);
      assertNull(nn.getBlockInfo(blockId));

      // Test a non existed block id
      assertNull(nn.getBlockInfo(nonExistedBlockId));

    } finally {
    	cluster.shutdown();
    }
	}
}	
