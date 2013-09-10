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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ClientCompatibleImplementation;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.OpenRequest;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.PingRequest;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.OpenResponse;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.PingResponse;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;

import junit.framework.TestCase;
import static org.mockito.Mockito.*;

import org.apache.hadoop.util.StringUtils;
import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;


/**
 * These tests make sure that DFSClient retries fetching data from DFS
 * properly in case of errors.
 */
public class TestDFSClientRetries extends TestCase {
  public static final Log LOG =
    LogFactory.getLog(TestDFSClientRetries.class.getName());
  
  // writes 'len' bytes of data to out.
  private static void writeData(OutputStream out, int len) throws IOException {
    byte [] buf = new byte[4096*16];
    while(len > 0) {
      int toWrite = Math.min(len, buf.length);
      out.write(buf, 0, toWrite);
      len -= toWrite;
    }
  }
  
  /**
   * This makes sure that when DN closes clients socket after client had
   * successfully connected earlier, the data can still be fetched.
   */
  public void testWriteTimeoutAtDataNode() throws IOException,
                                                  InterruptedException { 
    Configuration conf = new Configuration();
    
    final int writeTimeout = 100; //milliseconds.
    // set a very short write timeout for datanode, so that tests runs fast.
    conf.setInt("dfs.datanode.socket.write.timeout", writeTimeout); 
    // set a smaller block size
    final int blockSize = 10*1024*1024;
    conf.setInt("dfs.block.size", blockSize);
    conf.setInt("dfs.client.max.block.acquire.failures", 1);
    // set a small buffer size
    final int bufferSize = 4096;
    conf.setInt("io.file.buffer.size", bufferSize);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    
    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
    
      Path filePath = new Path("/testWriteTimeoutAtDataNode");
      OutputStream out = fs.create(filePath, true, bufferSize);
    
      // write a 2 block file.
      writeData(out, 2*blockSize);
      out.close();
      
      byte[] buf = new byte[1024*1024]; // enough to empty TCP buffers.
      
      InputStream in = fs.open(filePath, bufferSize);
      
      //first read a few bytes
      IOUtils.readFully(in, buf, 0, bufferSize/2);
      //now read few more chunks of data by sleeping in between :
      for(int i=0; i<10; i++) {
        Thread.sleep(2*writeTimeout); // force write timeout at the datanode.
        // read enough to empty out socket buffers.
        IOUtils.readFully(in, buf, 0, buf.length); 
      }
      // successfully read with write timeout on datanodes.
      in.close();
    } finally {
      cluster.shutdown();
    }
  }
  
  // more tests related to different failure cases can be added here.
  
  class TestNameNode extends ClientCompatibleImplementation
  {
    int num_calls = 0;
    
    // The total number of calls that can be made to addBlock
    // before an exception is thrown
    int num_calls_allowed; 
    public final String ADD_BLOCK_EXCEPTION = "Testing exception thrown from"
                                             + "TestDFSClientRetries::"
                                             + "TestNameNode::addBlock";
    public final String RETRY_CONFIG
          = "dfs.client.block.write.locateFollowingBlock.retries";
          
    public TestNameNode(Configuration conf) throws IOException
    {
      // +1 because the configuration value is the number of retries and
      // the first call is not a retry (e.g., 2 retries == 3 total
      // calls allowed)
      this.num_calls_allowed = conf.getInt(RETRY_CONFIG, 5) + 1;
    }

    public long getProtocolVersion(String protocol, 
                                     long clientVersion)
    throws IOException
    {
      return ClientProtocol.versionID;
    }
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }

    public LocatedBlock addBlock(String src, String clientName)
    throws IOException
    {
      num_calls++;
      if (num_calls > num_calls_allowed) { 
        throw new IOException("addBlock called more times than "
                              + RETRY_CONFIG
                              + " allows.");
      } else {
          throw new RemoteException(NotReplicatedYetException.class.getName(),
                                    ADD_BLOCK_EXCEPTION);
      }
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, DatanodeInfo[] excludedNodes) throws
        IOException {
      return addBlock(src, clientName);
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, DatanodeInfo[] excludedNode,
        DatanodeInfo[] favoredNodes) throws IOException {
      return addBlock(src, clientName);
    }

    public VersionedLocatedBlock addBlockAndFetchVersion(String src,
        String clientName, DatanodeInfo[] excludedNodes) throws IOException {
      return (VersionedLocatedBlock) addBlock(src, clientName);
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
        String clientName, DatanodeInfo[] excludedNodes) throws IOException {
      return addBlockAndFetchMetaInfo(src, clientName, excludedNodes, null);
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
        String clientName, DatanodeInfo[] excludedNodes,
        DatanodeInfo[] favoredNodes) throws IOException {
      return addBlockAndFetchMetaInfo(src, clientName, excludedNodes, null, -1);
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
        String clientName, DatanodeInfo[] excludedNodes, long pos)
        throws IOException {
      return addBlockAndFetchMetaInfo(src, clientName, excludedNodes, null, pos, null);
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
        String clientName, DatanodeInfo[] excludedNodes,
        DatanodeInfo[] favoredNodes, long pos) throws IOException {
      return addBlockAndFetchMetaInfo(src, clientName, excludedNodes, favoredNodes, pos, null);
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
      String clientName, DatanodeInfo[] excludedNodes,
      DatanodeInfo[] favoredNodes, long pos, Block lastBlockId) throws IOException   {
      return (LocatedBlockWithMetaInfo) addBlock(src, clientName);
    }

    // The following methods are stub methods that are not needed by this mock class

    public LocatedBlocks  getBlockLocations(String src, long offset, long length) throws IOException { return null; }

    @Deprecated
    public void create(String src, FsPermission masked, String clientName, boolean overwrite, short replication, long blockSize) throws IOException {}

    public void create(String src, FsPermission masked, String clientName, boolean overwrite, boolean createparent, short replication, long blockSize) throws IOException {}

    public LocatedBlock append(String src, String clientName) throws IOException { return null; }

    public LocatedBlockWithMetaInfo appendAndFetchMetaInfo(String src, String clientName) throws IOException { return null; }
    
    public LocatedBlockWithOldGS appendAndFetchOldGS(String src, String clientName) throws IOException { return null; }

    public boolean setReplication(String src, short replication) throws IOException { return false; }

    public void setPermission(String src, FsPermission permission) throws IOException {}

    public void setOwner(String src, String username, String groupname) throws IOException {}

    public void abandonBlock(Block b, String src, String holder) throws IOException {}

    public void abandonFile(String src, String holder) throws IOException {}

    public boolean complete(String src, String clientName) throws IOException { return false; }

    public boolean complete(String src, String clientName, long fileLen) throws IOException { return false; }

    public boolean complete(String src, String clientName, long fileLen, Block lastBlock) throws IOException { return false; }

    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {}

    public void concat(String trg, String[] srcs) throws IOException {  }
    
    public void concat(String trg, String[] srcs, boolean restricted) throws IOException {  }

    public boolean hardLink(String src, String dst) throws IOException { return false; }
    
    public void merge(String parity, String source, String codecId, int[] checksums)
        throws IOException {}

    public String[] getHardLinkedFiles(String src) throws IOException {
      return new String[] {};
    }

    public boolean rename(String src, String dst) throws IOException { return false; }

    public boolean delete(String src) throws IOException { return false; }

    public boolean delete(String src, boolean recursive) throws IOException { return false; }

    public boolean mkdirs(String src, FsPermission masked) throws IOException { return false; }
    
    public boolean raidFile(String src, String codecId, short expectedSourceRepl) 
        throws IOException { return false;}

    public OpenFileInfo[] iterativeGetOpenFiles(
      String prefix, int millis, String start) throws IOException {
      throw new IOException("iterativeGetOpenFiles() not supported by TestDFSClientRetries");
    }

    public FileStatus[] getListing(String src) throws IOException { return null; }

    public HdfsFileStatus[] getHdfsListing(String src) throws IOException {
      return null; }

    public DirectoryListing getPartialListing(String src, byte[] startAfter)
    throws IOException {
      return null; }
    
    public LocatedDirectoryListing getLocatedPartialListing(String src,
        byte[] startAfter) throws IOException {
      return null; }
    
    public void renewLease(String clientName) throws IOException {}

    public long[] getStats() throws IOException { return null; }

    public DatanodeInfo[] getDatanodeReport(FSConstants.DatanodeReportType type) throws IOException { return null; }

    public long getPreferredBlockSize(String filename) throws IOException { return 0; }

    public boolean setSafeMode(FSConstants.SafeModeAction action) throws IOException { return false; }

    public void saveNamespace() throws IOException {}
    public void saveNamespace(boolean force, boolean uncompressed) throws IOException {}

    public void rollEditLogAdmin() throws IOException {}
    
    public boolean restoreFailedStorage(String arg) throws AccessControlException { return false; }

    public void refreshNodes() throws IOException {}

    public void finalizeUpgrade() throws IOException {}

    public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) throws IOException { return null; }

    public void metaSave(String filename) throws IOException {}

    public void blockReplication(boolean isEnable) throws IOException {}

    public FileStatus getFileInfo(String src) throws IOException { return null; }

    public HdfsFileStatus getHdfsFileInfo(String src) throws IOException {
      return null; }

    public ContentSummary getContentSummary(String path) throws IOException { return null; }

    public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws IOException {}

    public void fsync(String src, String client) throws IOException {}

    public void setTimes(String src, long mtime, long atime) throws IOException {}

    @Override @Deprecated
    public FileStatus[] getCorruptFiles()
      throws AccessControlException, IOException {
      return null;
    }

    @Override
    public CorruptFileBlocks
      listCorruptFileBlocks(String path, String cookie)
      throws IOException {
      return null;
    }

    @Override
    public boolean closeRecoverLease(String src, String clientName,
        boolean discardlastBlock)
        throws IOException { return false;}

    @Override
    public int getDataTransferProtocolVersion() {return -1;}

    @Override
    public void recoverLease(String src, String clientName) throws IOException {}

    @Override
    public boolean closeRecoverLease(String src, String clientName)
        throws IOException { return false;}

    public VersionedLocatedBlocks open(String src, long offset, long length)
        throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public LocatedBlocksWithMetaInfo openAndFetchMetaInfo(String src,
        long offset, long length) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

		@Override
		public LocatedBlockWithFileName getBlockInfo(long blockId) 
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

    @Override
    public String getClusterName() throws IOException {
      throw new IOException("unimplemented");
    }

    @Override
    public void recount() throws IOException {
    }

    @Override
    public void updatePipeline(String clientName, Block oldBlock,
        Block newBlock, DatanodeID[] newNodes) throws IOException {
      throw new IOException("unimplemented");
    }

    @Override
    public long nextGenerationStamp(Block block, boolean fromNN)
        throws IOException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void commitBlockSynchronization(Block block,
        long newgenerationstamp, long newlength, boolean closeFile,
        boolean deleteblock, DatanodeID[] newtargets) throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public PingResponse ping(PingRequest req) throws IOException {
      return null;
    }
  }

  
  public void testNotYetReplicatedErrors() throws IOException
  {   
    Configuration conf = new Configuration();
    
    // allow 1 retry (2 total calls)
    conf.setInt("dfs.client.block.write.locateFollowingBlock.retries", 1);
        
    TestNameNode tnn = new TestNameNode(conf);
    final DFSClient client = new DFSClient(null, tnn, conf, null);
    OutputStream os = client.create("testfile", true);
    os.write(20); // write one random byte
    
    try {
      os.close();
    } catch (Exception e) {
      assertTrue("Retries are not being stopped correctly: " + StringUtils.stringifyException(e),
          e.getMessage().equals(tnn.ADD_BLOCK_EXCEPTION));
    }
  }

  /**
   * This tests that DFSInputStream failures are counted for a given read
   * operation, and not over the lifetime of the stream. It is a regression
   * test for HDFS-127.
   */
  public void testFailuresArePerOperation() throws Exception
  {
    long fileSize = 4096;
    Path file = new Path("/testFile");

    Configuration conf = new Configuration();
    int maxBlockAcquires = DFSClient.getMaxBlockAcquireFailures(conf);
    assertTrue(maxBlockAcquires > 0);
    int[] numDataNodes = new int[] {1, maxBlockAcquires, maxBlockAcquires + 1};
    
    for (int numDataNode : numDataNodes) {
      MiniDFSCluster cluster = null;
      try {
        cluster = new MiniDFSCluster(conf, numDataNode, true, null);
        cluster.waitActive();
        FileSystem fs = cluster.getFileSystem();
        NameNode preSpyNN = cluster.getNameNode();
        NameNode spyNN = spy(preSpyNN);
        
        DFSClient client = new DFSClient(null, spyNN, conf, null);
  
        DFSTestUtil.createFile(fs, file, fileSize, 
            (short)numDataNode, 12345L /*seed*/);
  
        // If the client will retry maxBlockAcquires times, then if we fail
        // any more than that number of times, the operation should entirely
        // fail.
        FailNTimesAnswer answer1 = new FailNTimesAnswer(preSpyNN, numDataNode,
            Math.min(maxBlockAcquires, numDataNode) + 1);
        doAnswer(answer1).when(spyNN).openAndFetchMetaInfo(anyString(), anyLong(), anyLong());
        doAnswer(answer1).when(spyNN).open(any(OpenRequest.class));
        try {
          IOUtils.copyBytes(client.open(file.toString()), new IOUtils.NullOutputStream(), conf,
                            true);
          fail("Didn't get exception");
        } catch (IOException ioe) {
          DFSClient.LOG.info("Got expected exception", ioe);
        }
  
        // If we fail exactly that many times, then it should succeed.
        FailNTimesAnswer answer2 = new FailNTimesAnswer(preSpyNN, numDataNode,
            Math.min(maxBlockAcquires, numDataNode));
        doAnswer(answer2).when(spyNN).openAndFetchMetaInfo(anyString(), anyLong(), anyLong());
        doAnswer(answer2).when(spyNN).open(any(OpenRequest.class));
        IOUtils.copyBytes(client.open(file.toString()), new IOUtils.NullOutputStream(), conf, true);
  
        DFSClient.LOG.info("Starting test case for failure reset");
  
        // Now the tricky case - if we fail a few times on one read, then succeed,
        // then fail some more on another read, it shouldn't fail.
        FailNTimesAnswer answer3 = new FailNTimesAnswer(preSpyNN, numDataNode,
            Math.min(maxBlockAcquires, numDataNode));
        doAnswer(answer3).when(spyNN).openAndFetchMetaInfo(anyString(), anyLong(), anyLong());
        doAnswer(answer3).when(spyNN).open(any(OpenRequest.class));
        DFSInputStream is = client.open(file.toString());
        byte buf[] = new byte[10];
        IOUtils.readFully(is, buf, 0, buf.length);
  
        DFSClient.LOG.info("First read successful after some failures.");
  
        // Further reads at this point will succeed since it has the good block locations.
        // So, force the block locations on this stream to be refreshed from bad info.
        // When reading again, it should start from a fresh failure count, since
        // we're starting a new operation on the user level.
        FailNTimesAnswer answer4 = new FailNTimesAnswer(preSpyNN, numDataNode,
            Math.min(maxBlockAcquires, numDataNode));
        doAnswer(answer4).when(spyNN).openAndFetchMetaInfo(anyString(), anyLong(), anyLong());
        doAnswer(answer4).when(spyNN).open(any(OpenRequest.class));
        is.openInfo();
        // Seek to beginning forces a reopen of the BlockReader - otherwise it'll
        // just keep reading on the existing stream and the fact that we've poisoned
        // the block info won't do anything.
        is.seek(0);
        IOUtils.readFully(is, buf, 0, buf.length);
  
      } finally {
        if (null != cluster) {
          cluster.shutdown();
        }
      }
    }
  }

  /**
   * Mock Answer implementation of NN.getBlockLocations that will return
   * a poisoned block list a certain number of times before returning
   * a proper one.
   */
  private static class FailNTimesAnswer implements Answer {
    private int failuresLeft;
    private NameNode realNN;
    private int numDataNode;

    public FailNTimesAnswer(NameNode realNN, int numDataNode, int timesToFail) {
      failuresLeft = timesToFail;
      this.realNN = realNN;
      this.numDataNode = numDataNode;
    }

    public Object answer(InvocationOnMock invocation) throws IOException {
      Object args[] = invocation.getArguments();
      if (args.length == 1) {
        LocatedBlocksWithMetaInfo realAnswer = realNN.open((OpenRequest) args[0]).get();
        return new OpenResponse(injectFailureIfNecessary(realAnswer));
      } else {
        LocatedBlocksWithMetaInfo realAnswer = realNN.openAndFetchMetaInfo((String) args[0],
            (Long) args[1], (Long) args[2]);
        return injectFailureIfNecessary(realAnswer);
      }
    }

    private LocatedBlocksWithMetaInfo injectFailureIfNecessary(
        LocatedBlocksWithMetaInfo realAnswer) {
      if (failuresLeft-- > 0) {
        NameNode.LOG.info("FailNTimesAnswer injecting failure.");
        return makeBadBlockList(realAnswer);
      }
      NameNode.LOG.info("FailNTimesAnswer no longer failing.");
      return realAnswer;
    }

    private LocatedBlocksWithMetaInfo makeBadBlockList(LocatedBlocksWithMetaInfo goodBlockList) {
      LocatedBlock goodLocatedBlock = goodBlockList.get(0);
      DatanodeInfo[] datanodes = new DatanodeInfo[numDataNode];
      for (int i = 0; i < numDataNode; i++) {
        datanodes[i] = new DatanodeInfo(new DatanodeID("255.255.255.255:" + (234 - i)));
      }
      
      LocatedBlock badLocatedBlock = new LocatedBlock(
        goodLocatedBlock.getBlock(),
        datanodes,
        goodLocatedBlock.getStartOffset(),
        false);


      List<LocatedBlock> badBlocks = new ArrayList<LocatedBlock>();
      badBlocks.add(badLocatedBlock);
      return new LocatedBlocksWithMetaInfo(
          goodBlockList.getFileLength(), badBlocks,
          false, goodBlockList.getDataProtocolVersion(),
          goodBlockList.getNamespaceID(), goodBlockList.getMethodFingerPrint());
    }
  }
}
