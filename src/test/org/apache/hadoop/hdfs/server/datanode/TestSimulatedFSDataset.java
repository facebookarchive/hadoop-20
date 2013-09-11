/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.util.DataChecksum;

/**
 * this class tests the methods of the  SimulatedFSDataset.
 *
 */

public class TestSimulatedFSDataset extends TestCase {
  
  Configuration conf = null;
  

  
  static final int NUMBLOCKS = 20;
  static final int BLOCK_LENGTH_MULTIPLIER = 79;
  private static final int DUMMY_NAMESPACE_ID = SimulatedFSDataset.DUMMY_NAMESPACE_ID;

  protected void setUp() throws Exception {
    super.setUp();
      conf = new Configuration();
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
 
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  long blockIdToLen(long blkid) {
    return blkid*BLOCK_LENGTH_MULTIPLIER;
  }
  
  int addSomeBlocks(FSDatasetInterface fsdataset, int startingBlockId) throws IOException {
    int bytesAdded = 0;
    for (int i = startingBlockId; i < startingBlockId+NUMBLOCKS; ++i) {
      Block b = new Block(i, 0, 0); // we pass expected len as zero, - fsdataset should use the sizeof actual data written
      BlockDataFile.Writer dataOut = ((SimulatedFSDataset.SimulatedBlockInlineChecksumFileWriter) fsdataset
          .writeToBlock(0, b, b, false, false, -1, -1)).getBlockDataFile()
          .getWriter(0);
      assertEquals(0, fsdataset.getFinalizedBlockLength(0,b));
      for (int j=1; j <= blockIdToLen(i); ++j) {
        dataOut.write(new byte[] {(byte)j});
        assertEquals(j, fsdataset.getFinalizedBlockLength(0,b)); // correct length even as we write
        bytesAdded++;
      }
      dataOut.close();
      b.setNumBytes(blockIdToLen(i));
      fsdataset.finalizeBlock(0,b);
      assertEquals(blockIdToLen(i), fsdataset.getFinalizedBlockLength(0,b));
    }
    return bytesAdded;  
  }
  int addSomeBlocks(FSDatasetInterface fsdataset ) throws IOException {
    return addSomeBlocks(fsdataset, 1);
  }

  public void testStorageUsage() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    assertEquals(fsdataset.getDfsUsed(), 0);
    assertEquals(fsdataset.getRemaining(), fsdataset.getCapacity());
    int bytesAdded = addSomeBlocks(fsdataset);
    assertEquals(bytesAdded, fsdataset.getDfsUsed());
    assertEquals(fsdataset.getCapacity()-bytesAdded,  fsdataset.getRemaining());
    
  }



  void  checkBlockDataAndSize(FSDatasetInterface fsdataset, 
              Block b, long expectedLen) throws IOException { 
    ReplicaToRead replica = fsdataset.getReplicaToRead(0, b);
    InputStream input = replica.getBlockInputStream(null, 0);
    long lengthRead = 0;
    int data;
    int count = 0;
    while ((data = input.read()) != -1) {
      if (count++ < BlockInlineChecksumReader.getHeaderSize()) {
        continue;
      }
      assertEquals(SimulatedFSDataset.DEFAULT_DATABYTE, data);
      lengthRead++;
    }
    assertEquals(expectedLen, lengthRead);
  }
  
  public void testWriteRead() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    addSomeBlocks(fsdataset);
    for (int i=1; i <= NUMBLOCKS; ++i) {
      Block b = new Block(i, 0, 0);
      assertTrue(fsdataset.isValidBlock(0, b, false));
      assertEquals(blockIdToLen(i), fsdataset.getFinalizedBlockLength(0,b));
      checkBlockDataAndSize(fsdataset, b, blockIdToLen(i));
    }
  }



  public void testGetBlockReport() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    Block[] blockReport = fsdataset.getBlockReport(0);
    assertEquals(0, blockReport.length);
    int bytesAdded = addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport(0);
    assertEquals(NUMBLOCKS, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
  }
  public void testInjectionEmpty() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    Block[] blockReport = fsdataset.getBlockReport(0);
    assertEquals(0, blockReport.length);
    int bytesAdded = addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport(0);
    assertEquals(NUMBLOCKS, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
    
    // Inject blocks into an empty fsdataset
    //  - injecting the blocks we got above.
  
   
    SimulatedFSDataset sfsdataset = new SimulatedFSDataset(conf);
    sfsdataset.injectBlocks(DUMMY_NAMESPACE_ID, blockReport);
    blockReport = sfsdataset.getBlockReport(0);
    assertEquals(NUMBLOCKS, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
      assertEquals(blockIdToLen(b.getBlockId()), sfsdataset.getFinalizedBlockLength(0,b));
    }
    assertEquals(bytesAdded, sfsdataset.getDfsUsed());
    assertEquals(sfsdataset.getCapacity()-bytesAdded, sfsdataset.getRemaining());
  }

  public void testInjectionNonEmpty() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    
    Block[] blockReport = fsdataset.getBlockReport(0);
    assertEquals(0, blockReport.length);
    int bytesAdded = addSomeBlocks(fsdataset);
    blockReport = fsdataset.getBlockReport(0);
    assertEquals(NUMBLOCKS, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
    }
    fsdataset = null;
    
    // Inject blocks into an non-empty fsdataset
    //  - injecting the blocks we got above.
  
   
    SimulatedFSDataset sfsdataset = new SimulatedFSDataset(conf);
    // Add come blocks whose block ids do not conflict with
    // the ones we are going to inject.
    bytesAdded += addSomeBlocks(sfsdataset, NUMBLOCKS+1);
    Block[] blockReport2 = sfsdataset.getBlockReport(0);
    assertEquals(NUMBLOCKS, blockReport.length);
    blockReport2 = sfsdataset.getBlockReport(0);
    assertEquals(NUMBLOCKS, blockReport.length);
    sfsdataset.injectBlocks(DUMMY_NAMESPACE_ID, blockReport);
    blockReport = sfsdataset.getBlockReport(0);
    assertEquals(NUMBLOCKS*2, blockReport.length);
    for (Block b: blockReport) {
      assertNotNull(b);
      assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
      assertEquals(blockIdToLen(b.getBlockId()), sfsdataset.getFinalizedBlockLength(0,b));
    }
    assertEquals(bytesAdded, sfsdataset.getDfsUsed());
    assertEquals(sfsdataset.getCapacity()-bytesAdded,  sfsdataset.getRemaining());
    
    
    // Now test that the dataset cannot be created if it does not have sufficient cap

    conf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY, 10);
 
    try {
      sfsdataset = new SimulatedFSDataset(conf);
      sfsdataset.injectBlocks(DUMMY_NAMESPACE_ID, blockReport);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }

  }

  public void checkInvalidBlock(Block b) throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    assertFalse(fsdataset.isValidBlock(0, b, false));
    try {
      fsdataset.getFinalizedBlockLength(0,b);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    
    try {
      ReplicaToRead replica = fsdataset.getReplicaToRead(0, b);
      if (replica != null) {
        InputStream input = replica.getBlockInputStream(null, 0);
        assertTrue("Expected an IO exception", false);
      }
    } catch (IOException e) {
      // ok - as expected
    }
    
    try {
      fsdataset.finalizeBlock(0,b);
      assertTrue("Expected an IO exception", false);
    } catch (IOException e) {
      // ok - as expected
    }
    
  }
  
  public void testInValidBlocks() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    Block b = new Block(1, 5, 0);
    checkInvalidBlock(b);
    
    // Now check invlaid after adding some blocks
    addSomeBlocks(fsdataset);
    b = new Block(NUMBLOCKS + 99, 5, 0);
    checkInvalidBlock(b);
    
  }

  public void testInvalidate() throws IOException {
    FSDatasetInterface fsdataset = new SimulatedFSDataset(conf); 
    int bytesAdded = addSomeBlocks(fsdataset);
    Block[] deleteBlocks = new Block[2];
    deleteBlocks[0] = new Block(1, 0, 0);
    deleteBlocks[1] = new Block(2, 0, 0);
    fsdataset.invalidate(0,deleteBlocks);
    checkInvalidBlock(deleteBlocks[0]);
    checkInvalidBlock(deleteBlocks[1]);
    long sizeDeleted = blockIdToLen(1) + blockIdToLen(2);
    assertEquals(bytesAdded-sizeDeleted, fsdataset.getDfsUsed());
    assertEquals(fsdataset.getCapacity()-bytesAdded+sizeDeleted,  fsdataset.getRemaining());
    
    
    
    // Now make sure the rest of the blocks are valid
    for (int i=3; i <= NUMBLOCKS; ++i) {
      Block b = new Block(i, 0, 0);
      assertTrue(fsdataset.isValidBlock(0, b, false));
    }
  }

}
