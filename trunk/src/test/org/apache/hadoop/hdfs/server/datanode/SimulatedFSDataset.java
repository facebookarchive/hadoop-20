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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.naming.OperationNotSupportedException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryInfo;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * This class implements a simulated FSDataset.
 * 
 * Blocks that are created are recorded but their data (plus their CRCs) are
 *  discarded.
 * Fixed data is returned when blocks are read; a null CRC meta file is
 * created for such data.
 * 
 * This FSDataset does not remember any block information across its
 * restarts; it does however offer an operation to inject blocks
 *  (See the TestInectionForSImulatedStorage()
 * for a usage example of injection.
 * 
 * Note the synchronization is coarse grained - it is at each method. 
 */

public class SimulatedFSDataset  implements FSConstants, FSDatasetInterface, Configurable{
  
  public static final String CONFIG_PROPERTY_SIMULATED =
                                    "dfs.datanode.simulateddatastorage";
  public static final String CONFIG_PROPERTY_CAPACITY =
                            "dfs.datanode.simulateddatastorage.capacity";
  
  public static final long DEFAULT_CAPACITY = 2L<<40; // 1 terabyte
  public static final byte DEFAULT_DATABYTE = 9; // 1 terabyte
  public static final int DUMMY_NAMESPACE_ID = 0;
  public static final String DUMMY_NS_DIR = "/tmp/Simulated/NS";
  byte simulatedDataByte = DEFAULT_DATABYTE;
  Configuration conf = null;
  
  static byte[] nullCrcFileData;

  {
    DataChecksum checksum = DataChecksum.newDataChecksum( DataChecksum.
                              CHECKSUM_NULL, 16*1024 );
    byte[] nullCrcHeader = checksum.getHeader();
    nullCrcFileData =  new byte[2 + nullCrcHeader.length];
    nullCrcFileData[0] = (byte) ((FSDataset.METADATA_VERSION >>> 8) & 0xff);
    nullCrcFileData[1] = (byte) (FSDataset.METADATA_VERSION & 0xff);
    for (int i = 0; i < nullCrcHeader.length; i++) {
      nullCrcFileData[i+2] = nullCrcHeader[i];
    }
  }
  
  private class BInfo implements ReplicaBeingWritten{ // information about a single block
    Block theBlock;
    private boolean finalized = false; // if not finalized => ongoing creation
    SimulatedOutputStream oStream = null;
    BInfo(int namespaceId, Block b, boolean forWriting) throws IOException {
      theBlock = new Block(b);
      if (theBlock.getNumBytes() < 0) {
        theBlock.setNumBytes(0);
      }
      if (!storage.alloc(namespaceId, theBlock.getNumBytes())) { // expected length - actual length may
                                          // be more - we find out at finalize
        DataNode.LOG.warn("Lack of free storage on a block alloc");
        throw new IOException("Creating block, no free space available");
      }

      if (forWriting) {
        finalized = false;
        oStream = new SimulatedOutputStream();
      } else {
        finalized = true;
        oStream = null;
      }
    }

    synchronized long getGenerationStamp() {
      return theBlock.getGenerationStamp();
    }

    synchronized void updateBlock(Block b) {
      theBlock.setGenerationStamp(b.getGenerationStamp());
      setlength(b.getNumBytes());
    }
    
    synchronized long getlength() {
      if (!finalized) {
         return oStream.getLength();
      } else {
        return theBlock.getNumBytes();
      }
    }

    synchronized void setlength(long length) {
      if (!finalized) {
         oStream.setLength(length);
      } else {
        theBlock.setNumBytes(length);
      }
    }

    @Override
    public void setBytesOnDisk(long length) {
      setlength(length);
    }

    @Override
    public void setBytesAcked(long length) {
      setlength(length);
    }
    
    synchronized SimulatedInputStream getIStream() throws IOException {
      if (!finalized) {
        // throw new IOException("Trying to read an unfinalized block");
         return new SimulatedInputStream(oStream.getLength(), DEFAULT_DATABYTE);
      } else {
        return new SimulatedInputStream(theBlock.getNumBytes(), DEFAULT_DATABYTE);
      }
    }
    
    synchronized void finalizeBlock(int namespaceId, long finalSize) throws IOException {
      if (finalized) {
        throw new IOException(
            "Finalizing a block that has already been finalized" + 
            theBlock.getBlockId());
      }
      if (oStream == null) {
        DataNode.LOG.error("Null oStream on unfinalized block - bug");
        throw new IOException("Unexpected error on finalize");
      }

      if (oStream.getLength() != finalSize) {
        DataNode.LOG.warn("Size passed to finalize (" + finalSize +
                    ")does not match what was written:" + oStream.getLength());
        throw new IOException(
          "Size passed to finalize does not match the amount of data written");
      }
      // We had allocated the expected length when block was created; 
      // adjust if necessary
      long extraLen = finalSize - theBlock.getNumBytes();
      if (extraLen > 0) {
        if (!storage.alloc(namespaceId, extraLen)) {
          DataNode.LOG.warn("Lack of free storage on a block alloc");
          throw new IOException("Creating block, no free space available");
        }
      } else {
        storage.free(namespaceId, -extraLen);
      }
      theBlock.setNumBytes(finalSize);  

      finalized = true;
      oStream = null;
      return;
    }
    
    SimulatedInputStream getMetaIStream() {
      return new SimulatedInputStream(nullCrcFileData);  
    }

    synchronized boolean isFinalized() {
      return finalized;
    }
  }
  
  static private class SimulatedNSStorage {
    private long used;    // in bytes
    
    synchronized long getUsed() {
      return used;
    }
    
    synchronized void alloc(long amount) {
      used += amount;
    }
    
    synchronized void free(long amount) {
      used -= amount;
    }
    
    SimulatedNSStorage() {
      used = 0;
    }
  }
  
  static private class SimulatedStorage {
    private long capacity;
    private HashMap<Integer, SimulatedNSStorage> storageMap = new 
        HashMap<Integer, SimulatedNSStorage>();
    
    SimulatedStorage(long cap) {
      this.capacity = cap;
    }
    
    synchronized long getFree() {
      return capacity - getUsed();
    }
    
    synchronized long getCapacity() {
      return capacity;
    }
    
    synchronized long getUsed() {
      long used = 0;
      for (SimulatedNSStorage storage: storageMap.values()) {
        used += storage.getUsed();
      }
      return used;
    }
    
    synchronized boolean alloc(int namespaceId, long amount) throws IOException{
      if (getFree() >= amount) {
        getStorage(namespaceId).alloc(amount);
        return true;
      } else {
        return false;    
      }
    }
    
    synchronized void free(int namespaceId, long amount) throws IOException{
      getStorage(namespaceId).free(amount);
    }
    
    synchronized void addStorage(int namespaceId) {
      SimulatedNSStorage storage = storageMap.get(namespaceId);
      if (storage != null) {
        return;
      }
      storage = new SimulatedNSStorage();
      storageMap.put(namespaceId, storage);
    }
    
    synchronized void removeStorage(int namespaceId) {
      storageMap.remove(namespaceId);
    }
    
    synchronized SimulatedNSStorage getStorage(int namespaceId) throws IOException{
      SimulatedNSStorage storage = storageMap.get(namespaceId);
      if (storage == null) {
        throw new IOException("The storage for namespace " + namespaceId + " is not found."); 
      }
      return storage;
    }
  }
  
  private HashMap<Integer, HashMap<Block, BInfo>> blockMap = null;
  private SimulatedStorage storage = null;
  private String storageId;
  
  public SimulatedFSDataset(Configuration conf) throws IOException {
    setConf(conf);
    this.addNamespace(this.DUMMY_NAMESPACE_ID, this.DUMMY_NS_DIR, conf);
  }
  
  private SimulatedFSDataset() { // real construction when setConf called.. Uggg
  }
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration iconf)  {
    conf = iconf;
    storageId = conf.get("StorageId", "unknownStorageId" +
                                        new Random().nextInt());
    registerMBean(storageId);
    storage = new SimulatedStorage(
        conf.getLong(CONFIG_PROPERTY_CAPACITY, DEFAULT_CAPACITY));
    //DataNode.LOG.info("Starting Simulated storage; Capacity = " + getCapacity() + 
    //    "Used = " + getDfsUsed() + "Free =" + getRemaining());

    blockMap = new HashMap<Integer, HashMap<Block,BInfo>>(); 
  }

  public synchronized void injectBlocks(int namespaceId, Block[] injectBlocks)
                                            throws IOException {
    if (injectBlocks != null) {
      for (Block b: injectBlocks) { // if any blocks in list is bad, reject list
        if (b == null) {
          throw new NullPointerException("Null blocks in block list");
        }
        if (isValidBlock(namespaceId, b, false)) {
          throw new IOException("Block already exists in  block list");
        }
      }
      HashMap<Block, BInfo> blkMap = blockMap.get(namespaceId);
      if (blkMap == null) {
        blkMap = new HashMap<Block, BInfo>();
        blockMap.put(namespaceId, blkMap);
      }
      for (Block b: injectBlocks) {
          BInfo binfo = new BInfo(namespaceId, b, false);
          blkMap.put(b, binfo);
      }
    }
  }
  
  public synchronized HashMap<Block, BInfo> getBlockMap(int namespaceId) throws IOException{
    HashMap<Block, BInfo> blkMap = blockMap.get(namespaceId);
    if (blkMap == null) {
      throw new IOException("BlockMap for namespace " + namespaceId + " is not found.");
    }
    return blkMap;
  }

  @Override
  public void finalizeBlock(int namespaceId, Block b) throws IOException {
    finalizeBlockInternal(namespaceId, b, false);
  }

  @Override
  public void finalizeBlockIfNeeded(int namespaceId, Block b) throws IOException {
    finalizeBlockInternal(namespaceId, b, true);    
  }

  private synchronized void finalizeBlockInternal(int namespaceId, Block b, boolean refinalizeOk) 
    throws IOException {
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    binfo.finalizeBlock(namespaceId, b.getNumBytes());

  }

  public synchronized void unfinalizeBlock(int namespaceId, Block b) throws IOException {
    if (isBeingWritten(namespaceId, b)) {
      getBlockMap(namespaceId).remove(b);
    }
  }

  public synchronized Block[] getBlockReport(int namespaceID) throws IOException {
    HashMap<Block, BInfo> blkMap = getBlockMap(namespaceID);
    Block[] blockTable = new Block[blkMap.size()];
    int count = 0;
    for (BInfo b : blkMap.values()) {
      if (b.isFinalized()) {
        blockTable[count++] = b.theBlock;
      }
    }
    if (count != blockTable.length) {
      blockTable = Arrays.copyOf(blockTable, count);
    }
    return blockTable;
  }

  public synchronized Block[] getBlocksBeingWrittenReport(int namespaceId) {
    return null;
  }

  public long getCapacity() throws IOException {
    return storage.getCapacity();
  }

  public long getDfsUsed() throws IOException {
    return storage.getUsed();
  }
  
  public long getNSUsed(int namespaceId) throws IOException {
    return storage.getStorage(namespaceId).getUsed();
  }

  public long getRemaining() throws IOException {
    return storage.getFree();
  }

  public synchronized long getFinalizedBlockLength(int namespaceId, Block b) throws IOException {
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      throw new IOException("Block " + b + " is not found in namespace " + namespaceId);
    }
    return binfo.getlength();
  }

  @Override
  public long getVisibleLength(int namespaceId, Block b) throws IOException {
    return getFinalizedBlockLength(namespaceId, b);
  }

  @Override
  public long getOnDiskLength(int namespaceId, Block b) throws IOException {
    return getFinalizedBlockLength(namespaceId, b);
  }

  @Override
  public boolean isBlockFinalized(int namespaceId, Block b)
      throws IOException {
    return isValidBlock(namespaceId, b, false);
  }

  @Override
  public ReplicaBeingWritten getReplicaBeingWritten(int namespaceId, Block b)
      throws IOException {
    return getBlockMap(namespaceId).get(b);
  }

  /** {@inheritDoc} */
  public Block getStoredBlock(int namespaceId, long blkid) throws IOException {
    Block b = new Block(blkid);
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      return null;
    }
    b.setGenerationStamp(binfo.getGenerationStamp());
    b.setNumBytes(binfo.getlength());
    return b;
  }

  /** {@inheritDoc} */
  public void updateBlock(int namespaceId, Block oldblock, Block newblock) throws IOException {
    BInfo binfo = getBlockMap(namespaceId).get(newblock);
    if (binfo == null) {
      throw new IOException("BInfo not found, b=" + newblock);
    }
    binfo.updateBlock(newblock);
  }

  public synchronized void invalidate(int namespaceId, Block[] invalidBlks) throws IOException {
    boolean error = false;
    if (invalidBlks == null) {
      return;
    }
    HashMap<Block, BInfo> blkMap = getBlockMap(namespaceId);
    for (Block b: invalidBlks) {
      if (b == null) {
        continue;
      }
      BInfo binfo = blkMap.get(b);
      if (binfo == null) {
        error = true;
        DataNode.LOG.warn("Invalidate: Missing block");
        continue;
      }
      storage.free(namespaceId, binfo.getlength());
      blkMap.remove(b);
    }
      if (error) {
          throw new IOException("Invalidate: Missing blocks.");
      }
  }

  public synchronized boolean isValidBlock(int namespaceId, Block b, boolean checkSize) throws IOException{
    // return (blockMap.containsKey(b));
    HashMap<Block, BInfo> blkMap = getBlockMap(namespaceId);
    BInfo binfo = blkMap.get(b);
    if (binfo == null) {
      return false;
    }
    return binfo.isFinalized();
  }

  /* check if a block is created but not finalized */
  private synchronized boolean isBeingWritten(int namespaceId, Block b) throws IOException{
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      return false;
    }
    return !binfo.isFinalized();  
  }
  
  public String toString() {
    return getStorageInfo();
  }

  public synchronized BlockWriteStreams writeToBlock(int namespaceId, Block b, 
                                            boolean isRecovery,
                                            boolean isReplicationRequest)
                                            throws IOException {
    if (isValidBlock(namespaceId, b, false)) {
          throw new BlockAlreadyExistsException("Block " + b + 
              " is valid, and cannot be written to.");
      }
    if (isBeingWritten(namespaceId, b)) {
        throw new BlockAlreadyExistsException("Block " + b + 
            " is being written, and cannot be written to.");
    }
      BInfo binfo = new BInfo(namespaceId, b, true);
      getBlockMap(namespaceId).put(b, binfo);
      SimulatedOutputStream crcStream = new SimulatedOutputStream();
      return new BlockWriteStreams(binfo.oStream, crcStream);
  }

  public synchronized InputStream getBlockInputStream(int namespaceId, Block b)
                                            throws IOException {
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    
    //DataNode.LOG.info("Opening block(" + b.blkid + ") of length " + b.len);
    return binfo.getIStream();
  }
  
  public synchronized InputStream getBlockInputStream(int namespaceId, Block b, long seekOffset)
                              throws IOException {
    InputStream result = getBlockInputStream(namespaceId, b);
    result.skip(seekOffset);
    return result;
  }

  /** Not supported */
  public BlockInputStreams getTmpInputStreams(int namespaceId, Block b, long blkoff, long ckoff
      ) throws IOException {
    throw new IOException("Not supported");
  }

  /** No-op */
  public void validateBlockMetadata(int namespaceId, Block b) {
  }

  /**
   * Returns metaData of block b as an input stream
   * @param b - the block for which the metadata is desired
   * @return metaData of block b as an input stream
   * @throws IOException - block does not exist or problems accessing
   *  the meta file
   */
  private synchronized InputStream getMetaDataInStream(int namespaceId, Block b)
                                              throws IOException {
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b + 
          " is being written, its meta cannot be read");
    }
    return binfo.getMetaIStream();
  }

  public synchronized long getMetaDataLength(int namespaceId, Block b) throws IOException {
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );  
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b +
          " is being written, its metalength cannot be read");
    }
    return binfo.getMetaIStream().getLength();
  }
  
  public MetaDataInputStream getMetaDataInputStream(int namespaceId, Block b)
  throws IOException {

       return new MetaDataInputStream(getMetaDataInStream(namespaceId, b),
                                                getMetaDataLength(namespaceId, b));
  }

  public synchronized boolean metaFileExists(int namespaceId, Block b) throws IOException {
    if (!isValidBlock(namespaceId, b, false)) {
          throw new IOException("Block " + b +
              " is valid, and cannot be written to.");
      }
    return true; // crc exists for all valid blocks
  }

  public void checkDataDir() throws DiskErrorException {
    // nothing to check for simulated data set
  }

  public synchronized long getChannelPosition(int namespaceId, Block b, 
                                              BlockWriteStreams stream)
                                              throws IOException {
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );
    }
    return binfo.getlength();
  }

  public synchronized void setChannelPosition(int namespaceId, Block b, BlockWriteStreams stream, 
                                              long dataOffset, long ckOffset)
                                              throws IOException {
    BInfo binfo = getBlockMap(namespaceId).get(b);
    if (binfo == null) {
      throw new IOException("No such Block " + b );
    }
    binfo.setlength(dataOffset);
  }

  /** 
   * Simulated input and output streams
   *
   */
  static private class SimulatedInputStream extends java.io.InputStream {
    

    byte theRepeatedData = 7;
    long length; // bytes
    int currentPos = 0;
    byte[] data = null;
    
    /**
     * An input stream of size l with repeated bytes
     * @param l
     * @param iRepeatedData
     */
    SimulatedInputStream(long l, byte iRepeatedData) {
      length = l;
      theRepeatedData = iRepeatedData;
    }
    
    /**
     * An input stream of of the supplied data
     * 
     * @param iData
     */
    SimulatedInputStream(byte[] iData) {
      data = iData;
      length = data.length;
      
    }
    
    /**
     * 
     * @return the lenght of the input stream
     */
    long getLength() {
      return length;
    }

    @Override
    public int read() throws IOException {
      if (currentPos >= length)
        return -1;
      if (data !=null) {
        return data[currentPos++];
      } else {
        currentPos++;
        return theRepeatedData;
      }
    }
    
    @Override
    public int read(byte[] b) throws IOException { 

      if (b == null) {
        throw new NullPointerException();
      }
      if (b.length == 0) {
        return 0;
      }
      if (currentPos >= length) { // EOF
        return -1;
      }
      int bytesRead = (int) Math.min(b.length, length-currentPos);
      if (data != null) {
        System.arraycopy(data, currentPos, b, 0, bytesRead);
      } else { // all data is zero
        for (int i : b) {  
          b[i] = theRepeatedData;
        }
      }
      currentPos += bytesRead;
      return bytesRead;
    }
  }
  
  /**
   * This class implements an output stream that merely throws its data away, but records its
   * length.
   *
   */
  static private class SimulatedOutputStream extends OutputStream {
    long length = 0;
    
    /**
     * constructor for Simulated Output Steram
     */
    SimulatedOutputStream() {
    }
    
    /**
     * 
     * @return the length of the data created so far.
     */
    long getLength() {
      return length;
    }

    /**
     */
    void setLength(long length) {
      this.length = length;
    }
    
    @Override
    public void write(int arg0) throws IOException {
      length++;
    }
    
    @Override
    public void write(byte[] b) throws IOException {
      length += b.length;
    }
    
    @Override
    public void write(byte[] b,
              int off,
              int len) throws IOException  {
      length += len;
    }
  }
  
  private ObjectName mbeanName;


  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
   *  We use storage id for MBean name since a minicluster within a single
   * Java VM may have multiple Simulated Datanodes.
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;

    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeanUtil.registerMBean("DataNode",
          "FSDatasetState-" + storageId, bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
 
    DataNode.LOG.info("Registered FSDatasetStatusMBean");
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }

  public String getStorageInfo() {
    return "Simulated FSDataset-" + storageId;
  }
  
  public boolean hasEnoughResource() {
    return true;
  }

  public File getBlockFile(int namespaceId, Block blk) throws IOException {
    throw new IOException("getBlockFile not supported.");
  }

  @Override
  public BlockRecoveryInfo startBlockRecovery(int namespaceId, long blockId)
      throws IOException {
    Block stored = getStoredBlock(namespaceId, blockId);
    return new BlockRecoveryInfo(stored, false);
  }

  @Override
  public void copyBlockLocal(String srcFileSystem, File srcBlockFile,
      int srcNamespaceId, Block srcBlock, int dstNamespaceId, Block dstBlock)
    throws IOException {
    throw new IOException("copyBlockLocal not supported.");
  }

  @Override
  public String getFileSystemForBlock(int namesapceId, Block block)
  throws IOException {
    throw new IOException("getFileSystemForBlock not supported.");
  }

  @Override
  public void addNamespace(int namespaceId, String nsDir, Configuration conf)
      throws IOException {
    HashMap<Block, BInfo> blk = new HashMap<Block, BInfo>();
    blockMap.put(namespaceId, blk);
    this.storage.addStorage(namespaceId);
  }

  @Override
  public void removeNamespace(int namespaceId) {
    blockMap.remove(namespaceId);
    this.storage.removeStorage(namespaceId);
  }

  @Override
  public void initialize(DataStorage storage) throws IOException {
    
  }

  @Override
  public long size(int namespaceId) {
    HashMap<Block, BInfo> map = blockMap.get(namespaceId);
    return map != null ? map.size() : 0;
  }
}
