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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient.MultiDataInputStream;
import org.apache.hadoop.hdfs.DFSClient.MultiDataOutputStream;
import org.apache.hadoop.hdfs.protocol.AppendBlockHeader;
import org.apache.hadoop.hdfs.profiling.DFSWriteProfilingData;
import org.apache.hadoop.hdfs.profiling.DFSWriteProfilingData.WritePacketClientProfile;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PipelineAck;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithOldGS;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlock;
import org.apache.hadoop.hdfs.protocol.WriteBlockHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.protocol.BlockAlreadyCommittedException;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.NativeCrc32;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 *
 * The client application writes data that is cached internally by
 * this stream. Data is broken up into packets, each packet is
 * typically 64K in size. A packet comprises of chunks. Each chunk
 * is typically 512 bytes and has an associated checksum with it.
 *
 * When a client application fills up the currentPacket, it is
 * enqueued into dataQueue.  The DataStreamer thread picks up
 * packets from the dataQueue, sends it to the first datanode in
 * the pipeline and moves it from the dataQueue to the ackQueue.
 * The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the
 * ackQueue.
 *
 * In case of error, all outstanding packets and moved from
 * ackQueue. A new pipeline is setup by eliminating the bad
 * datanode from the original pipeline. The DataStreamer now
 * starts sending packets from the dataQueue.
****************************************************************/
class DFSOutputStream extends FSOutputSummer implements Syncable, Replicable {
  private final DFSClient dfsClient;
  private Socket[] s;
  boolean closed = false;

  private String src;
  private MultiDataOutputStream blockStream;
  private MultiDataInputStream blockReplyStream;
  private Block block;
  final private long blockSize;
  private boolean pktIncludeVersion = false;
  final private int packetVersion;
  private DataChecksum checksum;
  private LinkedList<DFSOutputStreamPacket> dataQueue = new LinkedList<DFSOutputStreamPacket>();
  private LinkedList<DFSOutputStreamPacket> ackQueue = new LinkedList<DFSOutputStreamPacket>();
  private int numPendingHeartbeats = 0;
  private long lastPacketSentTime = 0;
  private final long packetTimeout;
  private DFSOutputStreamPacket currentPacket = null;
  private int maxPackets = 80; // each packet 64K, total 5MB
  // private int maxPackets = 1000; // each packet 64K, total 64MB
  private DataStreamer streamer;
  private ResponseProcessor response = null;
  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes writen in current block
  private int packetSize = 0; // write packet size, including the header.
  private int chunksPerPacket = 0;
  DatanodeInfo[] nodes = null; // list of targets for current block
  private DatanodeInfo[] favoredNodes = null; // put replicas here if possible
  private volatile boolean hasError = false;
  private volatile int errorIndex = 0;
  volatile IOException lastException = null;
  private long artificialSlowdown = 0;
  private long lastFlushOffset = 0; // offset when flush was invoked
  private boolean persistBlocks = false; // persist blocks on namenode
  private int recoveryErrorCount = 0; // number of times block recovery failed
  private final int maxRecoveryErrorCount;
  private volatile boolean appendChunk = false;   // appending to existing partial block
  private long initialFileSize = 0; // at time of file open
  private Progressable progress;
  private short blockReplication; // replication factor of file
  private long lastBlkOffset = 0; // end pos of last block already sent

  private boolean forceSync;
  private boolean doParallelWrites = false;
    
  private final WriteOptions options;

  private void setLastException(IOException e) {
    if (lastException == null) {
      lastException = e;
    }
  }
  
  public void setOffsets(long offset) {
    DFSClient.LOG.info("set last block offsets in file: " + src + " pos: " + offset);
    lastBlkOffset = offset;
  }

  /** Decide if the write pipeline supports bidirectional heartbeat or not */
  private boolean supportClientHeartbeat() throws IOException {
    return dfsClient.getDataTransferProtocolVersion() >=
                 DataTransferProtocol.CLIENT_HEARTBEAT_VERSION;
  }

  /**
   * Check if the last outstanding packet has not received an ack before
   * it is timed out.
   * If true, for now just log it.
   * We will provide a decent solution to this later on.
   */
  private void checkIfLastPacketTimeout() {
     synchronized (ackQueue) {
             if( !ackQueue.isEmpty()  && (
                             System.currentTimeMillis() - lastPacketSentTime > packetTimeout) ) {
               DFSClient.LOG.warn("Packet " + ackQueue.getLast().seqno +
                             " of " + block + " is timed out");
             }
     }
  }


  //
  // The DataStreamer class is responsible for sending data packets to the
  // datanodes in the pipeline. It retrieves a new blockid and block locations
  // from the namenode, and starts streaming packets to the pipeline of
  // Datanodes. Every packet has a sequence number associated with
  // it. When all the packets for a block are sent out and acks for each
  // if them are received, the DataStreamer closes the current block.
  //
  private class DataStreamer extends Daemon {

    private volatile boolean closed = false;
    private long lastPacket;
    private boolean doSleep;

    DataStreamer() throws IOException {
      // explicitly invoke RPC so avoiding RPC in waitForWork
      // that might cause timeout
      dfsClient.getDataTransferProtocolVersion();
    }

    private void waitForWork() throws IOException {
      if ( supportClientHeartbeat() ) {  // send heart beat
        long now = System.currentTimeMillis();
        while ((!closed && !hasError && dfsClient.clientRunning
            && dataQueue.size() == 0  &&
            (blockStream == null || (
                blockStream != null && now - lastPacket < dfsClient.timeoutValue/2)))
                || doSleep) {
          long timeout = dfsClient.timeoutValue/2 - (now-lastPacket);
          timeout = timeout <= 0 ? 1000 : timeout;

          try {
            dataQueue.wait(timeout);
            checkIfLastPacketTimeout();
            now = System.currentTimeMillis();
          } catch (InterruptedException  e) {
          }
          doSleep = false;
        }
      } else { // no sending heart beat
        while ((!closed && !hasError && dfsClient.clientRunning
            && dataQueue.size() == 0) || doSleep) {
          try {
            dataQueue.wait(1000);
          } catch (InterruptedException  e) {
          }
          doSleep = false;
        }
      }
    }

    public void run() {
      while (!closed && dfsClient.clientRunning) {

        // if the Responder encountered an error, shutdown Responder
        if (hasError && response != null) {
          try {
            response.close();
            response.join();
            response = null;
          } catch (InterruptedException  e) {
          }
        }

        DFSOutputStreamPacket one = null;

        // process IO errors if any
        doSleep = processDatanodeError(hasError, false);

        try {
          synchronized (dataQueue) {
            // wait for a packet to be sent.
            waitForWork();

            if (closed || hasError || !dfsClient.clientRunning) {
              continue;
            }

            InjectionHandler
                .processEventIO(InjectionEvent.DFSCLIENT_DATASTREAM_AFTER_WAIT, blockStream);
            
            // get packet to be sent.
            if (dataQueue.isEmpty()) {
              one = DFSOutputStreamPacketFactory.getHeartbeatPacket(
                  DFSOutputStream.this, ifPacketIncludeVersion(),
                  getPacketVersion()); // heartbeat
                                                                     // packet
            } else {
              one = dataQueue.getFirst(); // regular data packet
              one.eventPopFromDataQueue();
            }
          }
                  
          long offsetInBlock = one.offsetInBlock;

          // get new block from namenode.
          if (blockStream == null) {
            DFSClient.LOG.debug("Allocating new block: " + src + "  pos: " + lastBlkOffset);

            nodes = nextBlockOutputStream(src);
            this.setName("DataStreamer for file " + src +
                " block " + block);
            response = new ResponseProcessor(nodes);
            response.start();
          }

          if (offsetInBlock > blockSize
              || (offsetInBlock == blockSize && (one.dataLength > 0 || !one.lastPacketInBlock))) {
            throw new IOException("BlockSize " + blockSize +
                                  " is smaller than data size. " +
                                  " Offset of packet in block " +
                                  offsetInBlock +
                                  " Aborting file " + src);
          }

          ByteBuffer buf = one.getBuffer();

          InjectionHandler.processEventIO(
              InjectionEvent.DFSCLIENT_DATASTREAM_BEFORE_WRITE, blockStream);
          
          // write out data to remote datanode
          blockStream.write(buf.array(), buf.position(), buf.remaining());

          if (one.lastPacketInBlock) {
            blockStream.writeInt(0); // indicate end-of-block
          }
          blockStream.flush();
          lastPacket = System.currentTimeMillis();
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("DataStreamer block " + block +
                      " wrote packet seqno:" + one.seqno +
                      " size:" + buf.remaining() +
                      " offsetInBlock:" + one.offsetInBlock +
                      " lastPacketInBlock:" + one.lastPacketInBlock);
          }

          // move packet from dataQueue to ackQueue
          synchronized (dataQueue) {
            if (!one.isHeartbeatPacket()) {
              dataQueue.removeFirst();
              dataQueue.notifyAll();
              synchronized (ackQueue) {
                ackQueue.addLast(one);
                one.eventAddToAckQueue();
                lastPacketSentTime = System.currentTimeMillis();
                ackQueue.notifyAll();
              }
            } else {
              synchronized (ackQueue) {
                numPendingHeartbeats++;
                ackQueue.notifyAll();                  
              }

              DFSClient.LOG.info("Sending a heartbeat packet for block " + block);
            }
          }
        } catch (Throwable e) {
          dfsClient.incWriteExpCntToStats();

          DFSClient.LOG.warn("DataStreamer Exception: ", e);
          if (e instanceof IOException) {
            setLastException((IOException)e);
          }
          hasError = true;
          if (blockStream != null) {
            // find the first datanode to which we could not write data.
            int possibleError =  blockStream.getErrorIndex();
            if (possibleError != -1) {
              errorIndex = possibleError;
              DFSClient.LOG.warn("DataStreamer bad datanode in pipeline:" +
                         possibleError);
            }
          }
        }

        if (closed || hasError || !dfsClient.clientRunning) {
          continue;
        }

        // Is this block full?
        if (one.lastPacketInBlock) {
          synchronized (ackQueue) {
            while (!hasError && ackQueue.size() != 0 && dfsClient.clientRunning) {
              try {
                ackQueue.wait();   // wait for acks to arrive from datanodes
              } catch (InterruptedException  e) {
              }
            }
          }
          DFSClient.LOG.debug("Closing old block " + block);
          this.setName("DataStreamer for file " + src);

          response.close();        // ignore all errors in Response
          try {
            response.join();
            response = null;
          } catch (InterruptedException  e) {
          }
          
          if (closed || hasError || !dfsClient.clientRunning) {
            continue;
          }

          synchronized (dataQueue) {
            try {
              blockStream.close();
              blockReplyStream.close();
            } catch (IOException e) {
            }
            nodes = null;
            response = null;
            blockStream = null;
            blockReplyStream = null;
          }
        }
        
        if (progress != null) { progress.progress(); }

        // This is used by unit test to trigger race conditions.
        if (artificialSlowdown != 0 && dfsClient.clientRunning) {
          DFSClient.sleepForUnitTest(artificialSlowdown);
        }
      }
    }

    // shutdown thread
    void close() {
      closed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
      synchronized (ackQueue) {
        ackQueue.notifyAll();
      }
      this.interrupt();
    }
  }

  //
  // Processes reponses from the datanodes.  A packet is removed
  // from the ackQueue when its response arrives.
  //
  private class ResponseProcessor extends Thread {

    private volatile boolean closed = false;
    private DatanodeInfo[] targets = null;
    private boolean lastPacketInBlock = false;

    ResponseProcessor (DatanodeInfo[] targets) {
      this.targets = targets;
    }

    public void run() {

      this.setName("ResponseProcessor for block " + block);

      while (!closed && dfsClient.clientRunning && !lastPacketInBlock) {
        // process responses from datanodes.
        int recordError = 0;
        try {
          long seqno = 0;
          synchronized (ackQueue) {
            while (!closed && dfsClient.clientRunning && ackQueue.isEmpty() &&
                   numPendingHeartbeats == 0) {
              try {
                ackQueue.wait();
              } catch (InterruptedException e) {
                // If the thread is being interrupted when waiting for
                // packet, we log the exception and treat it as a normal
                // exception.
                //
                DFSClient.LOG.info("ResponseProcessor thread interrupted when " +
                         "waiting for new packets");
                throw e;
              }
            }
          }
          if (closed || !dfsClient.clientRunning) {
            break;
          }

          eventStartReceiveAck();
          PipelineAck pipelineAck = null;
          if (!doParallelWrites) {
            // verify seqno from datanode
            if (supportClientHeartbeat()) {
              pipelineAck = new PipelineAck();
              pipelineAck.readFields(blockReplyStream.get(0), targets.length,
                  profileData != null);
              
              seqno = pipelineAck.getSeqno();
              
              if (!pipelineAck.isSuccess()) {
                for (int i = 0; i < targets.length && dfsClient.clientRunning; i++) {
                  short reply = pipelineAck.getReply(i);
                  if (reply != DataTransferProtocol.OP_STATUS_SUCCESS) {
                    recordError = i; // first bad datanode
                    throw new IOException("Bad response " + reply + " for block "
                        + block + " from datanode " + targets[i].getName());
                  }
                }                
              }
            } else {
              // Backward compatibility codes.
              seqno = blockReplyStream.get(0).readLong();
              DFSClient.LOG.debug("DFSClient received ack for seqno " + seqno);
              if (seqno == DFSOutputStreamPacket.HEART_BEAT_SEQNO) {
                continue;
              }
              // regular ack
              // processes response status from all datanodes.
              for (int i = 0; i < targets.length && dfsClient.clientRunning; i++) {
                short reply = blockReplyStream.get(0).readShort();
                if (reply != DataTransferProtocol.OP_STATUS_SUCCESS) {
                  recordError = i; // first bad datanode
                  throw new IOException("Bad response " + reply + " for block "
                      + block + " from datanode " + targets[i].getName());
                }
              }
            }
          } else {
            // The client is writing to all replicas in parallel. It also
            // expects an ack from all replicas.
            long lastsn = 0;
            assert blockReplyStream.size() > 0;
            for (int i = 0; i < blockReplyStream.size(); i++) {
              recordError = i; // remember the current slot
              seqno = blockReplyStream.get(i).readLong();
              if (DFSClient.LOG.isDebugEnabled()) {
                DFSClient.LOG.debug("DFSClient for block " + block + " " + seqno);
              }
              if (i != 0 && seqno != -2 && seqno != lastsn) {
                String msg = "Responses from datanodes do not match "
                    + " this replica acked " + seqno
                    + " but previous replica acked " + lastsn;
                DFSClient.LOG.warn(msg);
                throw new IOException(msg);
              }
              short reply = blockReplyStream.get(i).readShort();
              if (reply != DataTransferProtocol.OP_STATUS_SUCCESS) {
                recordError = i; // first bad datanode
                throw new IOException("Bad parallel response " + reply
                    + " for block " + block + " from datanode "
                    + targets[i].getName());
              }
              lastsn = seqno;
            }
          }

          assert seqno != -2 :
            "Ack for unkown seqno should be a failed ack!";
          if (seqno == DFSOutputStreamPacket.HEART_BEAT_SEQNO) {  // a heartbeat ack
            assert supportClientHeartbeat();
            synchronized(ackQueue) {
              assert numPendingHeartbeats > 0;
              numPendingHeartbeats--;
            }
            continue;
          }

          DFSOutputStreamPacket one = null;
          synchronized (ackQueue) {
            assert !ackQueue.isEmpty();
            one = ackQueue.getFirst();
          }
          if (one.seqno != seqno) {
            throw new IOException("Responseprocessor: Expecting seqno " +
                " for block " + block +
                one.seqno + " but received " + seqno);
          }
                    
          lastPacketInBlock = one.lastPacketInBlock;

          if (lastPacketInBlock) {
            if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG
                  .debug("Update pos in file: " + src + " curBlckOffset: "
                      + lastBlkOffset + " blockSize: "
                      + one.getEndPosInCurrBlk());
            }
            lastBlkOffset += one.getEndPosInCurrBlk();
          }

          synchronized (ackQueue) {
            assert seqno == lastAckedSeqno + 1;
            lastAckedSeqno = seqno;
            
            ackQueue.removeFirst();
            ackQueue.notifyAll();
          }

          one.eventAckReceived();

          if (getProfileData() != null) {
            getProfileData().finishPacket(one.profile, pipelineAck);
            long slowWriteProfileThreshold = options
                .getLogSlowWriteProfileDataThreshold();
            long totalTime = getProfileData().recentPacketProfile.getTotalTime();
            if (slowWriteProfileThreshold > 0
                && totalTime > slowWriteProfileThreshold) {
              DFSClient.LOG.warn("Slow Write Packet for block : " + block +
                  ", packet seqno : " + one.seqno +  ", total time : " +
                  totalTime + " \n" + getProfileData().recentPacketProfile);
            }
          }
        } catch (Exception e) {
          if (!closed) {
            hasError = true;
            errorIndex = recordError;
            if (e instanceof IOException) {
              setLastException((IOException)e);
            }
            DFSClient.LOG.warn("DFSOutputStream ResponseProcessor exception " +
                     " for block " + block +
                      StringUtils.stringifyException(e));
            closed = true;
          }
        }

        synchronized (dataQueue) {
          dataQueue.notifyAll();
        }
        synchronized (ackQueue) {
          ackQueue.notifyAll();
        }
      }
    }

    void close() {
      closed = true;
      this.interrupt();
    }
  }

  // If this stream has encountered any errors so far, shutdown
  // threads and mark stream as closed. Returns true if we should
  // sleep for a while after returning from this call.
  //
  private boolean processDatanodeError(boolean hasError, boolean isAppend) {
    if (!hasError) {
      return false;
    }
    if (response != null) {
      DFSClient.LOG.info("Error Recovery for block " + block +
               " waiting for responder to exit. ");
      return true;
    }
    dfsClient.incWriteExpCntToStats();

    if (errorIndex >= 0) {
      DFSClient.LOG.warn("Error Recovery for block " + block
          + " bad datanode[" + errorIndex + "] "
          + (nodes == null? "nodes == null": nodes[errorIndex].getName()));
    }

    if (blockStream != null) {
      try {
        blockStream.close();
        blockReplyStream.close();
      } catch (IOException e) {
      }
    }
    blockStream = null;
    blockReplyStream = null;

    // move packets from ack queue to front of the data queue
    synchronized (dataQueue) {
      synchronized (ackQueue) {
        if (!ackQueue.isEmpty()) {
          DFSClient.LOG.info("First unacked packet in " + block + " starts at "
              + ackQueue.getFirst().offsetInBlock);
          dataQueue.addAll(0, ackQueue);
          ackQueue.clear();
        }
        numPendingHeartbeats = 0;
      }
    }

    boolean success = false;
    while (!success && dfsClient.clientRunning) {
      DatanodeInfo[] newnodes = null;
      if (nodes == null) {
        String msg = "Could not get block locations. " +
                                        "Source file \"" + src
                                        + "\" - Aborting...";
        DFSClient.LOG.warn(msg);
        setLastException(new IOException(msg));
        closed = true;
        if (streamer != null) streamer.close();
        return false;
      }
      StringBuilder pipelineMsg = new StringBuilder();
      for (int j = 0; j < nodes.length; j++) {
        pipelineMsg.append(nodes[j].getName());
        if (j < nodes.length - 1) {
          pipelineMsg.append(", ");
        }
      }
      // remove bad datanode from list of datanodes.
      // If errorIndex was not set (i.e. appends), then do not remove
      // any datanodes
      //
      if (errorIndex < 0) {
        newnodes = nodes;
      } else {
        if (nodes.length <= 1) {
          lastException = new IOException("All datanodes " + pipelineMsg +
                                          " are bad. Aborting...");
          closed = true;
          if (streamer != null) streamer.close();
          return false;
        }
        DFSClient.LOG.warn("Error Recovery for block " + block +
                 " in pipeline " + pipelineMsg +
                 ": bad datanode " + nodes[errorIndex].getName());
        newnodes =  new DatanodeInfo[nodes.length-1];
        System.arraycopy(nodes, 0, newnodes, 0, errorIndex);
        System.arraycopy(nodes, errorIndex+1, newnodes, errorIndex,
            newnodes.length-errorIndex);
      }

      // Tell the primary datanode to do error recovery
      // by stamping appropriate generation stamps.
      //
      LocatedBlock newBlock = null;
      DatanodeInfo primaryNode = null;
      boolean clientAdRecoveryPrimaryProtocolSupported = false;
      try {
        clientAdRecoveryPrimaryProtocolSupported = dfsClient.namenodeProtocolProxy
            .isMethodSupported("nextGenerationStamp", Block.class,
                boolean.class);
      } catch (InterruptedIOException iie) {
        return false;
      } catch (IOException ioe) {
        DFSClient.LOG.warn(
            "Error when trying to determine whether namenode protocol "
                + "supports client as block recovoery coordinator.", ioe);
      }
      boolean clientAsRecoveryPrimary = dfsClient.conf.getBoolean(
          "dfs.client.as.block.recovery.primary", true)
          && clientAdRecoveryPrimaryProtocolSupported;
      try {
        if (clientAsRecoveryPrimary) {
          BlockRecoveryCoordinator brc = new BlockRecoveryCoordinator(
              DFSClient.LOG, dfsClient.conf, dfsClient.socketTimeout, null,
              new BlockSyncer(dfsClient.getNamespaceId(),
                  dfsClient.getNameNodeRPC(), DFSClient.LOG), null);
          newBlock = brc.recoverBlock(dfsClient.getNamespaceId(), block, false,
              newnodes, false, System.currentTimeMillis() + dfsClient.socketTimeout * 8000);
        } else {
          // Pick the "least" datanode as the primary datanode to avoid
          // deadlock.
          primaryNode = Collections.min(Arrays.asList(newnodes));
          newBlock = recoverBlockFromPrimaryDataNode(primaryNode, newnodes,
              isAppend);
        }
        
        if (newBlock == null) {
          throw new IOException("all datanodes do not have the block");
        }
        boolean isEmpty;
        long nextByteToSend;
        long newBlockSize = newBlock.getBlockSize();
        int numPktRemoved;
        synchronized (dataQueue) {
          numPktRemoved = adjustDataQueueAfterBlockRecovery(newBlockSize);
          isEmpty = dataQueue.isEmpty();
          if (isEmpty) {
            if (currentPacket != null) {
              nextByteToSend = currentPacket.offsetInBlock;
            } else {
              nextByteToSend = bytesCurBlock;
            }
          } else {
            nextByteToSend = dataQueue.getFirst().offsetInBlock;
          }
        }
        if (numPktRemoved > 0) {
          DFSClient.LOG.info("Remove " + numPktRemoved
              + " packets in the packet queue after block recovery");
          if (nextByteToSend > newBlockSize) {
            DFSClient.LOG
                .warn("Missing bytes after removing packets! It should never happen. nextByteToSend "
                    + nextByteToSend + " new block size " + newBlockSize);
          }
        } else if (nextByteToSend > newBlockSize) {
          DFSClient.LOG.warn("Missing bytes! Error Recovery for block " + block
              + " end up with " + newBlock.getBlockSize()
              + " bytes but client already sent " + nextByteToSend
              + " bytes and data queue is " + (isEmpty ? "" : "not ")
              + "empty.");
        } else if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Didn't remove any block. nextByteToSend "
                    + nextByteToSend + " new block size " + newBlockSize);
        }
      } catch (BlockAlreadyCommittedException e) {
        dfsClient.incWriteExpCntToStats();

        DFSClient.LOG
            .warn("Error Recovery for block "
                + block
                + " failed "
                + " because block is already committed according to primary datanode "
                + primaryNode + ". " + " Pipeline was " + pipelineMsg
                + ". Aborting...", e);

        lastException = e;
        closed = true;
        if (streamer != null) streamer.close();
        return false;       // abort with IOexception
      } catch (IOException e) {
        dfsClient.incWriteExpCntToStats();

        DFSClient.LOG.warn("Failed recovery attempt #" + recoveryErrorCount +
            " from primary datanode " + primaryNode, e);
        recoveryErrorCount++;
        // For client as primary, no need to retry as all failures thrown by
        // data nodes are already handled.
        if (clientAsRecoveryPrimary || recoveryErrorCount > maxRecoveryErrorCount) {
          if (!clientAsRecoveryPrimary && nodes.length > 1) {
            // if the primary datanode failed, remove it from the list.
            // The original bad datanode is left in the list because it is
            // conservative to remove only one datanode in one iteration.
            for (int j = 0; j < nodes.length; j++) {
              if (nodes[j].equals(primaryNode)) {
                errorIndex = j; // forget original bad node.
              }
            }
            // remove primary node from list
            newnodes =  new DatanodeInfo[nodes.length-1];
            System.arraycopy(nodes, 0, newnodes, 0, errorIndex);
            System.arraycopy(nodes, errorIndex+1, newnodes, errorIndex,
                             newnodes.length-errorIndex);
            nodes = newnodes;
            DFSClient.LOG.warn("Error Recovery for block " + block + " failed " +
                     " because recovery from primary datanode " +
                     primaryNode + " failed " + recoveryErrorCount +
                     " times. " + " Pipeline was " + pipelineMsg +
                     ". Marking primary datanode as bad.");
            recoveryErrorCount = 0;
            errorIndex = -1;
            return true;          // sleep when we return from here
          }
          String emsg = "Error Recovery for block " + block + " failed " +
                        " because recovery from primary datanode " +
                        primaryNode + " failed " + recoveryErrorCount +
                        " times. "  + " Pipeline was " + pipelineMsg +
                        ". Aborting...";
          DFSClient.LOG.warn(emsg);
          lastException = new IOException(emsg);
          closed = true;
          if (streamer != null) streamer.close();
          return false;       // abort with IOexception
        }
        DFSClient.LOG.warn("Error Recovery for block " + block + " failed " +
                 " because recovery from primary datanode " +
                 primaryNode + " failed " + recoveryErrorCount +
                 " times. "  + " Pipeline was " + pipelineMsg +
                 ". Will retry...");
        return true;          // sleep when we return from here
      } finally {
      }
      recoveryErrorCount = 0; // block recovery successful

      // If the block recovery generated a new generation stamp, use that
      // from now on.  Also, setup new pipeline
      //
      if (newBlock != null) {
        block = newBlock.getBlock();
        nodes = newBlock.getLocations();
      }

      this.hasError = false;
      lastException = null;
      errorIndex = 0;
      success = createBlockOutputStream(nodes, dfsClient.clientName, 
          true, false);
    }

    response = new ResponseProcessor(nodes);
    response.start();
    return false; // do not sleep, continue processing
  }
  
  LocatedBlock recoverBlockFromPrimaryDataNode(DatanodeInfo primaryNode,
      DatanodeInfo[] newnodes, boolean isAppend) throws IOException {
    ProtocolProxy<ClientDatanodeProtocol> primary = null;
    try {
    // Copied from org.apache.hadoop.ipc.Client
    int connectTimeout = dfsClient.conf.getInt(
        Client.CONNECT_TIMEOUT_KEY, Client.CONNECT_TIMEOUT_DEFAULT);
    int maxRetries = dfsClient.conf.getInt(
        Client.CONNECT_MAX_RETRIES_KEY, Client.CONNECT_MAX_RETRIES_DEFAULT);
    /*
     * considering pipeline recovery needs 3 RPCs to DataNodes and 2 RPCs to
     * NameNode; So rpcTimeout sets to be 5 times of client socketTimeout.
     * Also each datanode RPC might take upto (connectTimeout * maxRetries)
     * to establish connection.
     */
    int recoverTimeout = 5 * dfsClient.socketTimeout + 3
        * (connectTimeout * maxRetries);
    primary = DFSClient.createClientDNProtocolProxy(primaryNode,
        dfsClient.conf, recoverTimeout);
    try {
      if (primary.isMethodSupported("recoverBlock", int.class, Block.class,
          boolean.class, DatanodeInfo[].class, long.class)) {
        // The deadline is up to RPC time out minus one socket timeout
        // to be more conservative.
        return primary.getProxy().recoverBlock(dfsClient.namespaceId, block,
            isAppend, newnodes,
            System.currentTimeMillis() + recoverTimeout -
            dfsClient.socketTimeout - (maxRetries * connectTimeout));
      } else if (primary.isMethodSupported("recoverBlock", int.class, Block.class, boolean.class, DatanodeInfo[].class)) {
        return primary.getProxy().recoverBlock(
            dfsClient.namespaceId, block, isAppend, newnodes);
      } else {
        return primary.getProxy().recoverBlock(block, isAppend, newnodes);
      }
    } catch (RemoteException re) {
      if (re.unwrapRemoteException() instanceof BlockAlreadyCommittedException) {
        throw new BlockAlreadyCommittedException(re);
      } else {
        throw re;
      }
    }
    } finally {
      if (primary != null) {
        RPC.stopProxy(primary.getProxy());
      }
    }
  }
  
  
  private int adjustDataQueueAfterBlockRecovery(long newBlockSize) {
    // New block size should be one of the packet's ending position
    // If the block offset of the first packet is not the new block
    // size, we should be able to remove several packets in packet
    // queue and make sure the first packet is the new block size.
    // Otherwise, something went wrong.
    //
    // We are conservative here: if the first unacked packet starts
    // with a full chunk, it can always be a clean checkpoint. We
    // keep the packets starting from it.
    //
    int bytesPerChecksum = checksum.getBytesPerChecksum();
    int numPktRemoved = 0;
    long newAckedSeqno = -1;
    while (!dataQueue.isEmpty()) {
      DFSOutputStreamPacket first = dataQueue.getFirst();
      long endOffsetOfBlock = first.getEndPosInCurrBlk();
      if (first.isHeartbeatPacket()) {
        dataQueue.removeFirst();
        numPktRemoved++;
      } else if (first.offsetInBlock % bytesPerChecksum == 0) {
        // The first unacked packet starts with a full chunk.
        //
        break;
      } else if (endOffsetOfBlock <= newBlockSize) {
        if (first.lastPacketInBlock) {
          // Last block is already acked in all remaining replicas
          // Resend an empty one to force the stream to finish.
          //
          if (endOffsetOfBlock != newBlockSize) {
            DFSClient.LOG.warn("Packet is the last packet in block with "
                + endOffsetOfBlock
                + " but new block length after block recovery is "
                + newBlockSize + ". Something went wrong.");
          }
          
          first.cleanup();
          first.offsetInBlock = endOffsetOfBlock;
          DFSClient.LOG
              .info("Resend last packet in block and make it empty, new offsetInBlock "
                  + endOffsetOfBlock);
          break;
        } else {
          dataQueue.removeFirst();
          numPktRemoved++;
          if (first.seqno > lastAckedSeqno) {
            newAckedSeqno = first.seqno;
          }
        }
      } else {
        if (first.offsetInBlock != newBlockSize) {
          DFSClient.LOG.warn("Packet has start offset " + first.offsetInBlock
              + " and end offset " + endOffsetOfBlock
              + " but new block length after block recovery is " + newBlockSize
              + ". Something went wrong.");          
        }
        break;
      }
    }
    
    if (numPktRemoved > 0 && newAckedSeqno != -1) {
      synchronized (ackQueue) {
        lastAckedSeqno = newAckedSeqno;
        ackQueue.notifyAll();
      }
    }

    return numPktRemoved;
  }

  private void isClosed() throws IOException {
    if ((closed || !dfsClient.clientRunning) && lastException != null) {
        throw lastException;
    }
  }

  //
  // returns the list of targets, if any, that is being currently used.
  //
  DatanodeInfo[] getPipeline() {
    synchronized (dataQueue) {
      if (nodes == null) {
        return null;
      }
      DatanodeInfo[] value = new DatanodeInfo[nodes.length];
      for (int i = 0; i < nodes.length; i++) {
        value[i] = nodes[i];
      }
      return value;
    }
  }
  
  static private DFSWriteProfilingData getProfile(DFSClient dfsClient) {
    DFSWriteProfilingData profile = DFSClient.getAndResetProfileDataForNextOutputStream();
    if (dfsClient != null) {
      boolean ifAutoPrint = dfsClient.conf.getBoolean(
          FSConstants.FS_OUTPUT_STREAM_AUTO_PRINT_PROFILE, false);
      if (ifAutoPrint) {
        if (profile == null) {
          profile = new DFSWriteProfilingData();
        }
        profile.setAutoPrintWhileClose(true);
      }
    }
    return profile;
  }

  private DFSOutputStream(DFSClient dfsClient, String src, long blockSize,
      Progressable progress, int bytesPerChecksum, short replication, boolean forceSync,
 boolean doParallelWrites, DatanodeInfo[] favoredNodes,
      WriteOptions options)
  throws IOException {
    super(new NativeCrc32(), bytesPerChecksum, 4, getProfile(dfsClient));
    this.dfsClient = dfsClient;
    this.forceSync = forceSync;
    this.doParallelWrites = doParallelWrites;
    this.src = src;
    this.blockSize = blockSize;
    this.blockReplication = replication;
    this.progress = progress;
    this.options = options;
    this.pktIncludeVersion = dfsClient.ifPacketIncludeVersion();
    this.packetVersion = dfsClient.getOutPacketVersion();
    
    streamer = new DataStreamer();
    
    packetTimeout =
        dfsClient.conf.getLong("dfs.client.packet.timeout", 15000); // 15 seconds
    // try block recovery 5 times:
    maxRecoveryErrorCount =
        dfsClient.conf.getInt("dfs.client.block.recovery.retries", 5);
    
    if (progress != null) {
      DFSClient.LOG.debug("Set non-null progress callback on DFSOutputStream "+src);
    }

    this.favoredNodes = favoredNodes;

    if ( bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
      throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum +
                            ") and blockSize(" + blockSize +
                            ") do not match. " + "blockSize should be a " +
                            "multiple of io.bytes.per.checksum");

    }
    checksum = DataChecksum.newDataChecksum(FSConstants.CHECKSUM_TYPE,
                                            bytesPerChecksum,
                                            new NativeCrc32());
  }
  
  /**
   * Create a new output stream to the given DataNode.
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  DFSOutputStream(DFSClient dfsClient, String src, int buffersize,
      Progressable progress, LocatedBlock lastBlock, FileStatus stat,
      int bytesPerChecksum)
      throws IOException {
    this(dfsClient, src, buffersize, progress, lastBlock, stat,
        bytesPerChecksum, 0);
  }

  DFSOutputStream(DFSClient dfsClient, String src, FsPermission masked,
      boolean overwrite, boolean createParent, short replication,
      long blockSize, Progressable progress, int buffersize,
      int bytesPerChecksum, boolean forceSync, boolean doParallelWrites,
      DatanodeInfo[] favoredNodes) throws IOException {
    this(dfsClient, src, masked, overwrite, createParent, replication,
        blockSize, progress, buffersize, bytesPerChecksum, forceSync,
        doParallelWrites, favoredNodes, new WriteOptions());
  }
  /**
   * Create a new output stream to the given DataNode.
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  DFSOutputStream(DFSClient dfsClient, String src, FsPermission masked,
      boolean overwrite, boolean createParent, short replication, long blockSize,
      Progressable progress,int buffersize, int bytesPerChecksum,
      boolean forceSync, boolean doParallelWrites,
      DatanodeInfo[] favoredNodes, WriteOptions options)
      throws IOException {
    this(dfsClient, src, blockSize, progress, bytesPerChecksum, replication,
        forceSync, doParallelWrites, favoredNodes, options);

    computePacketChunkSize(dfsClient.writePacketSize, bytesPerChecksum);

    try {
      if (dfsClient.namenodeProtocolProxy != null && 
            dfsClient.namenodeProtocolProxy.isMethodSupported("create", String.class, 
               FsPermission.class, String.class, boolean.class, boolean.class,
               short.class, long.class)) {
        dfsClient.namenode.create(src, masked, dfsClient.clientName, overwrite,
                        createParent, replication, blockSize);
      } else {
        dfsClient.namenode.create(src, masked, dfsClient.clientName, overwrite,
                        replication, blockSize);
      }
    } catch(RemoteException re) {
      dfsClient.incWriteExpCntToStats();

      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileAlreadyExistsException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
    streamer.start();
  }

  /**
   * Create a new output stream to the given DataNode with namespace id.
   */
  DFSOutputStream(DFSClient dfsClient, String src, int buffersize,
      Progressable progress, LocatedBlock lastBlock, FileStatus stat,
      int bytesPerChecksum, int namespaceId) throws IOException {
    this(dfsClient, src, stat.getBlockSize(), progress, bytesPerChecksum,
        stat.getReplication(), false, false, null,
        new WriteOptions());
    initialFileSize = stat.getLen(); // length of file when opened
    dfsClient.updateNamespaceIdIfNeeded(namespaceId);
    //
    // The last partial block of the file has to be filled.
    //
    if (lastBlock != null) {
      block = lastBlock.getBlock();
      long usedInLastBlock = stat.getLen() % blockSize;
      int freeInLastBlock = (int)(blockSize - usedInLastBlock);

      // calculate the amount of free space in the pre-existing
      // last crc chunk
      int usedInCksum = (int)(stat.getLen() % bytesPerChecksum);
      super.bytesSentInChunk = usedInCksum;
      int freeInCksum = bytesPerChecksum - usedInCksum;

      // if there is space in the last block, then we have to
      // append to that block
      if (freeInLastBlock > blockSize) {
        throw new IOException("The last block for file " +
                              src + " is full.");
      }

      int dataProtocolVersion = dfsClient.getDataTransferProtocolVersion();
      // indicate that we are appending to an existing block
      if (dataProtocolVersion >= DataTransferProtocol.APPEND_BLOCK_VERSION) {
        bytesCurBlock = lastBlock.getBlock().getNumBytes();
      } else {
        bytesCurBlock = lastBlock.getBlockSize();
      }
      
      if (usedInCksum > 0 && freeInCksum > 0) {
        // if there is space in the last partial chunk, then
        // setup in such a way that the next packet will have only
        // one chunk that fills up the partial chunk.
        //
        computePacketChunkSize(0, freeInCksum);
        resetChecksumChunk();
        this.appendChunk = true;
      } else {
        // if the remaining space in the block is smaller than
        // that expected size of of a packet, then create
        // smaller size packet.
        //
        computePacketChunkSize(Math.min(dfsClient.writePacketSize, freeInLastBlock),
                               bytesPerChecksum);
      }

      // setup pipeline to append to the last block
      nodes = lastBlock.getLocations();
      errorIndex = -1;   // no errors yet.
      if (nodes.length < 1) {
        throw new IOException("Unable to retrieve blocks locations" +
                              " for append to last block " + block +
                              " of file " + src);

      }
      
      if (dataProtocolVersion < DataTransferProtocol.APPEND_BLOCK_VERSION) {
        // go through the block recovery process to setup the pipeline for append
        while(processDatanodeError(true, true)) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException  e) {
            lastException = new IOException(e);
            break;
          }
        }
      } else {
        setupPipelineForAppend(lastBlock);
      }
      if (lastException != null) {
        throw lastException;
      }
    }
    else {
      computePacketChunkSize(dfsClient.writePacketSize, bytesPerChecksum);
    }
    
    long blockOffset = stat.getLen();
    blockOffset -= blockOffset % blockSize;
    setOffsets(blockOffset);
    streamer.start();
  }
  
  /**
   * Setup the Append pipeline, the length of current pipeline will shrink
   * if any datanodes are dead during the process.
   */
  private boolean setupPipelineForAppend(LocatedBlock lastBlock) throws IOException {
    if (nodes == null || nodes.length == 0) {
      String msg = "Could not get block locations. " +
          "Source file \"" + src
          + "\" - Aborting...";
      DFSClient.LOG.warn(msg);
      setLastException(new IOException(msg));
      closed = true;
      if (streamer != null) streamer.close();
      return false;
    }
    
    boolean success = createBlockOutputStream(nodes, dfsClient.clientName, false, true);
    long oldGenerationStamp = 
        ((LocatedBlockWithOldGS)lastBlock).getOldGenerationStamp();
    
    if (success) {
      // bump up the generation stamp in NN.
      Block newBlock = lastBlock.getBlock();
      Block oldBlock = new Block(newBlock.getBlockId(), newBlock.getNumBytes(), 
          oldGenerationStamp);
      dfsClient.namenode.updatePipeline(dfsClient.clientName, 
          oldBlock, newBlock, nodes);
    } else {
      DFSClient.LOG.warn("Fall back to block recovery process when trying" +
          " to setup the append pipeline for file " + src);
      // set the old generation stamp 
      block.setGenerationStamp(oldGenerationStamp);
      // fall back the block recovery
      while(processDatanodeError(true, true)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException  e) {
          lastException = new IOException(e);
          break;
        }
      }
    }
    return success;
  }

  private void computePacketChunkSize(int psize, int csize) {
    int chunkSize = csize + checksum.getChecksumSize();
    int n = getPacketHeaderLen() + DFSClient.SIZE_OF_INTEGER;
    chunksPerPacket = Math.max((psize - n + chunkSize-1)/chunkSize, 1);
    packetSize = n + chunkSize*chunksPerPacket;
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("computePacketChunkSize: src=" + src +
                ", chunkSize=" + chunkSize +
                ", chunksPerPacket=" + chunksPerPacket +
                ", packetSize=" + packetSize);
    }
  }

  /**
   * Open a DataOutputStream to a DataNode so that it can be written to.
   * This happens when a file is created and each time a new block is allocated.
   * Must get block ID and the IDs of the destinations from the namenode.
   * Returns the list of target datanodes.
   */
  private DatanodeInfo[] nextBlockOutputStream(String client) throws IOException {
    LocatedBlock lb = null;
    boolean retry = false;
    DatanodeInfo[] nodes;
    ArrayList<DatanodeInfo> excludedNodes = new ArrayList<DatanodeInfo>();
    int count = dfsClient.conf.getInt("dfs.client.block.write.retries", 3);
    boolean success;
    do {
      hasError = false;
      lastException = null;
      errorIndex = 0;
      retry = false;
      nodes = null;
      success = false;

      long startTime = System.currentTimeMillis();

      DatanodeInfo[] excluded = excludedNodes.toArray(new DatanodeInfo[0]);
      lb = locateFollowingBlock(startTime, excluded.length > 0 ? excluded
          : null);
      block = lb.getBlock();
      nodes = lb.getLocations();

      //
      // Connect to first DataNode in the list.
      //
      success = createBlockOutputStream(nodes, dfsClient.clientName, 
          false, false);

      if (!success) {
        DFSClient.LOG.info("Abandoning block " + block + " for file " + src);
        dfsClient.namenode.abandonBlock(block, src, dfsClient.clientName);

        if (errorIndex < nodes.length) {
          DFSClient.LOG.debug("Excluding datanode " + nodes[errorIndex]);
          excludedNodes.add(nodes[errorIndex]);
        }
        // Connection failed.  Let's wait a little bit and retry
        retry = true;
      }
    } while (retry && --count >= 0);

    if (!success && nodes != null) {
      // in the last fail time, we will retry with the remaining nodes.
      while (nodes.length > 1 && !success) {
        if (errorIndex >= nodes.length) {
          break;
        }
        
        DatanodeInfo[] remainingNodes = new DatanodeInfo[nodes.length - 1];
        for (int i = 0; i < errorIndex; i++) {
          remainingNodes[i] = nodes[i];
        }
        
        for (int i = errorIndex + 1; i < nodes.length; i++) {
          remainingNodes[i - 1] = nodes[i];
        }
        
        nodes = remainingNodes;
        success = createBlockOutputStream(nodes, dfsClient.clientName, 
            false, false);
      }
    }
    
    if (!success) {
      throw new IOException("Unable to create new block.");
    }
    return nodes;
  }

  // For pipelined writes, connects to the first datanode in the pipeline.
  // For parallel writes, connect to all specified datanodes.
  // Returns true if success, otherwise return failure.
  //
  private boolean createBlockOutputStream(DatanodeInfo[] nodes, String client,
                  boolean recoveryFlag, boolean appendFlag) {
    String firstBadLink = "";
    if (DFSClient.LOG.isDebugEnabled()) {
      for (int i = 0; i < nodes.length; i++) {
        DFSClient.LOG.debug("pipeline = " + nodes[i].getName());
      }
    }

    // persist blocks on namenode on next flush
    persistBlocks = true;
    boolean result = false;
    int curNode = 0;
    int length = 0;
    int pipelineDepth;
    if (doParallelWrites) {
      length = nodes.length; // connect to all datanodes
      pipelineDepth = 1;
    } else {
      length = 1; // connect to only the first datanode
      pipelineDepth = nodes.length;
    }
    DataOutputStream[] tmpOut = new DataOutputStream[length];
    DataInputStream[] replyIn = new DataInputStream[length];
    Socket[] sockets = new Socket[length];

    try {
      for (curNode = 0; curNode < length;  curNode++) {

        DFSClient.LOG.debug("Connecting to " + nodes[curNode].getName());
        InetSocketAddress target = NetUtils.createSocketAddr(nodes[curNode].getName());
        Socket s = dfsClient.socketFactory.createSocket();
        sockets[curNode] = s;
        dfsClient.timeoutValue = dfsClient.socketReadExtentionTimeout *
            pipelineDepth + dfsClient.socketTimeout;
        NetUtils.connect(s, target, dfsClient.timeoutValue, dfsClient.ipTosValue);
        s.setSoTimeout(dfsClient.timeoutValue);
        s.setSendBufferSize(DFSClient.DEFAULT_DATA_SOCKET_SIZE);
        DFSClient.LOG.debug("Send buf size " + s.getSendBufferSize());
        long writeTimeout = dfsClient.datanodeWriteExtentionTimeout *
                            pipelineDepth + dfsClient.datanodeWriteTimeout;

        //
        // Xmit header info to datanode (see DataXceiver.java)
        //
        DataOutputStream out = new DataOutputStream(
          new BufferedOutputStream(NetUtils.getOutputStream(s, writeTimeout),
                                   DataNode.SMALL_BUFFER_SIZE));
        tmpOut[curNode] = out;
        DataInputStream brs = new DataInputStream(NetUtils.getInputStream(s));
        replyIn[curNode] = brs;
        
        if (getProfileData() != null) {
          getProfileData().nextBlock();
        }

        int version = dfsClient.getDataTransferProtocolVersion();
        // write the header
        if (!appendFlag) {
          WriteBlockHeader header = new WriteBlockHeader(version,
              dfsClient.namespaceId, block.getBlockId(), block.getGenerationStamp(),
              pipelineDepth, recoveryFlag, false, null, pipelineDepth - 1,
              nodes, client);
          header.getWritePipelineInfo().setWriteOptions(options);
          header.getWritePipelineInfo().getWriteOptions()
              .setIfProfileEnabled(profileData != null);
          header.writeVersionAndOpCode(out);
          header.write(out);
        } else {
          AppendBlockHeader header = new AppendBlockHeader(version,
              dfsClient.namespaceId, block.getBlockId(), block.getNumBytes(), 
              block.getGenerationStamp(),
              pipelineDepth, false, null, pipelineDepth - 1,
              nodes, client);
          header.writeVersionAndOpCode(out);
          header.write(out);
        }
        checksum.writeHeader(out);
        out.flush();

        // receive ack for connect
        firstBadLink = Text.readString(brs);
        if (firstBadLink.length() != 0) {
          throw new IOException("Bad connect ack with firstBadLink " +
                                firstBadLink);
        }
      }
      result = true;     // success
      blockStream = dfsClient.new MultiDataOutputStream(tmpOut);
      blockReplyStream = dfsClient.new MultiDataInputStream(replyIn);
      this.s = sockets;
      
      if (appendFlag) {
        // start the responseProcessor if the pipeline is successfully setup
        // for append only
        response = new ResponseProcessor(nodes);
        response.start(); 
      }
    } catch (IOException ie) {

      DFSClient.LOG.info("Exception in createBlockOutputStream " +
          nodes[curNode].getName() + " " + " for file " + src + ie);

      dfsClient.incWriteExpCntToStats();
      
      // find the datanode that matches
      if (firstBadLink.length() != 0) {
        for (int i = 0; i < nodes.length; i++) {
          if (nodes[i].getName().equals(firstBadLink)) {
            errorIndex = i;
            break;
          }
        }
      } else {
        // if we are doing parallel writes, then record the datanode that is bad
        errorIndex = curNode;
      }
      hasError = true;
      setLastException(ie);
      blockReplyStream = null;
      result = false;
    } finally {
      if (!result) {
        for (int i = 0; i < sockets.length; i++) {
          IOUtils.closeSocket(sockets[i]);
        }
        this.s = null;
      }
    }
    
    return result;
  }

  private LocatedBlock locateFollowingBlock(long start,
                                            DatanodeInfo[] excludedNodes
                                            ) throws IOException {
    int retries = dfsClient.conf.getInt(
        "dfs.client.block.write.locateFollowingBlock.retries", 5);
    
    long sleeptime = 400;
    while (true) {
      long localstart = System.currentTimeMillis();
      while (true) {
        try {
          VersionedLocatedBlock loc = null;
          if (dfsClient.namenodeProtocolProxy != null
              && dfsClient.namenodeProtocolProxy.isMethodSupported(
                  "addBlockAndFetchMetaInfo", String.class, String.class,
                  DatanodeInfo[].class, DatanodeInfo[].class, long.class,
                  Block.class)) {
           loc = dfsClient.namenode.addBlockAndFetchMetaInfo(src, 
               dfsClient.clientName, excludedNodes, favoredNodes,
               this.lastBlkOffset, getLastBlock());
          } else if (dfsClient.namenodeProtocolProxy != null
              && dfsClient.namenodeProtocolProxy.isMethodSupported(
                  "addBlockAndFetchMetaInfo", String.class, String.class,
                  DatanodeInfo[].class, DatanodeInfo[].class, long.class)) {
            loc = dfsClient.namenode.addBlockAndFetchMetaInfo(src,
                dfsClient.clientName, excludedNodes, favoredNodes, this.lastBlkOffset);
          } else if (dfsClient.namenodeProtocolProxy != null
              && dfsClient.namenodeProtocolProxy.isMethodSupported(
                  "addBlockAndFetchMetaInfo", String.class, String.class,
                  DatanodeInfo[].class, long.class)) {
            loc = dfsClient.namenode.addBlockAndFetchMetaInfo(src,
                dfsClient.clientName, excludedNodes, this.lastBlkOffset);
          } else if (dfsClient.namenodeProtocolProxy != null
              && dfsClient.namenodeProtocolProxy.isMethodSupported(
                  "addBlockAndFetchMetaInfo", String.class, String.class,
                  DatanodeInfo[].class)) {
            loc = dfsClient.namenode.addBlockAndFetchMetaInfo(src,
                dfsClient.clientName, excludedNodes);
          } else if (dfsClient.namenodeProtocolProxy != null
              && dfsClient.namenodeProtocolProxy.isMethodSupported(
                  "addBlockAndFetchVersion", String.class, String.class,
                  DatanodeInfo[].class)) {
            loc = dfsClient.namenode.addBlockAndFetchVersion(src,
                dfsClient.clientName, excludedNodes);
          } else if (dfsClient.namenodeProtocolProxy != null
              && dfsClient.namenodeProtocolProxy.isMethodSupported("addBlock",
                  String.class, String.class, DatanodeInfo[].class)) {
            return dfsClient.namenode.addBlock(src, dfsClient.clientName,
                excludedNodes);
          } else {
            return dfsClient.namenode.addBlock(src, dfsClient.clientName);
          }
          dfsClient.updateDataTransferProtocolVersionIfNeeded(
              loc.getDataProtocolVersion());
          if (loc instanceof LocatedBlockWithMetaInfo) {
            LocatedBlockWithMetaInfo metaLoc = (LocatedBlockWithMetaInfo)loc;
            dfsClient.updateNamespaceIdIfNeeded(metaLoc.getNamespaceID());
            dfsClient.getNewNameNodeIfNeeded(metaLoc.getMethodFingerPrint());
          }
          return loc;
        } catch (RemoteException e) {
          IOException ue =
            e.unwrapRemoteException(FileNotFoundException.class,
                                    AccessControlException.class,
                                    NSQuotaExceededException.class,
                                    DSQuotaExceededException.class);
          if (ue != e) {
            throw ue; // no need to retry these exceptions
          }

          if (NotReplicatedYetException.class.getName().
              equals(e.getClassName())) {

              if (retries == 0) {
                throw e;
              } else {
                --retries;
                DFSClient.LOG.info(StringUtils.stringifyException(e));
                if (System.currentTimeMillis() - localstart > 5000) {
                  DFSClient.LOG.info("Waiting for replication for "
                      + (System.currentTimeMillis() - localstart) / 1000
                      + " seconds");
                }
                try {
                  DFSClient.LOG.warn("NotReplicatedYetException sleeping " + src
                      + " retries left " + retries);
                  Thread.sleep(sleeptime);
                  sleeptime *= 2;
                } catch (InterruptedException ie) {
                }
              }
          } else {
            throw e;
          }
        }
      }
    }
  }

  @Override
  protected void incMetrics(int len){
    dfsClient.metrics.incWriteOps();
  	dfsClient.metrics.incWriteSize(len);
  }
  // @see FSOutputSummer#writeChunk()
  @Override
  protected synchronized void writeChunk(byte[] b, int offset, int len, byte[] checksum)
                                                        throws IOException {
    dfsClient.checkOpen();
    isClosed();


    int cklen = checksum.length;
    int bytesPerChecksum = this.checksum.getBytesPerChecksum();
    if (len > bytesPerChecksum) {
      throw new IOException("writeChunk() buffer size is " + len +
                            " is larger than supported  bytesPerChecksum " +
                            bytesPerChecksum);
    }
    if (checksum.length != this.checksum.getChecksumSize()) {
      throw new IOException("writeChunk() checksum size is supposed to be " +
                            this.checksum.getChecksumSize() +
                            " but found to be " + checksum.length);
    }

    eventStartEnqueuePacket();
    
    synchronized (dataQueue) {

      // If queue is full, then wait till we can create  enough space
      while (!closed && dataQueue.size() + ackQueue.size()  > maxPackets) {
        try {
          dataQueue.wait(packetTimeout);
          checkIfLastPacketTimeout();
        } catch (InterruptedException  e) {
        }
      }
      isClosed();
      
      if (currentPacket == null) {
        WritePacketClientProfile pktProfile = null;
        if (getProfileData() != null) {
          pktProfile = getProfileData().getWritePacketClientProfile();
        }

        currentPacket = DFSOutputStreamPacketFactory.getPacket(
            DFSOutputStream.this, ifPacketIncludeVersion(),
            getPacketVersion(), packetSize, chunksPerPacket, bytesCurBlock, pktProfile);

        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("DFSClient writeChunk allocating new packet seqno=" +
                    currentPacket.seqno +
                    ", src=" + src +
                    ", packetSize=" + packetSize +
                    ", chunksPerPacket=" + chunksPerPacket +
                    ", bytesCurBlock=" + bytesCurBlock +
                    ", forceSync=" + forceSync +
                    ", doParallelWrites=" + doParallelWrites +
                    ", len=" + len +
                    ", blocksize=" + blockSize);
        }
      }

      if (packetVersion == DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST) {
        currentPacket.writeChecksum(checksum, 0, cklen);
        currentPacket.writeData(b, offset, len);
      } else {
        // packetVersion == DataTransferProtocol.PACKET_VERSION_CHECKSUM_INLINE
        currentPacket.writeData(b, offset, len);
        currentPacket.writeChecksum(checksum, 0, cklen);
      }
      currentPacket.numChunks++;
      bytesCurBlock += len;

      // If packet is full, enqueue it for transmission
      if (currentPacket.numChunks == currentPacket.maxChunks ||
          bytesCurBlock == blockSize) {
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("DFSClient writeChunk packet full seqno=" +
                    currentPacket.seqno +
                    ", src=" + src +
                    ", bytesCurBlock=" + bytesCurBlock +
                    ", blockSize=" + blockSize +
                    ", appendChunk=" + appendChunk);
        }
        //
        // if we allocated a new packet because we encountered a block
        // boundary, reset bytesCurBlock.
        //
        if (bytesCurBlock == blockSize) {
          currentPacket.lastPacketInBlock = true;
          bytesCurBlock = 0;
          lastFlushOffset = 0;
        }
        enqueueCurrentPacket();
        
        eventEndEnquePacket();

        // If this was the first write after reopening a file, then the above
        // write filled up any partial chunk. Tell the summer to generate full
        // crc chunks from now on.
        if (appendChunk) {
          appendChunk = false;
          resetChecksumChunk();
        }
        int psize = Math.min((int)(blockSize-bytesCurBlock), 
            dfsClient.writePacketSize);
        computePacketChunkSize(psize, bytesPerChecksum);
      }
    }
    

    //LOG.debug("DFSClient writeChunk done length " + len +
    //          " checksum length " + cklen);
  }

  private synchronized void enqueueCurrentPacket() {
    synchronized (dataQueue) {
      if (currentPacket == null) return;
      dataQueue.addLast(currentPacket);
      currentPacket.eventAddToDataQueue();
      dataQueue.notifyAll();
      lastQueuedSeqno = currentPacket.seqno;
      currentPacket = null;
    }
  }

  /**
   * All data is written out to datanodes. It is not guaranteed
   * that data has been flushed to persistent store on the
   * datanode. Block allocations are persisted on namenode.
   */
  public void sync() throws IOException {
  	long start = System.currentTimeMillis();
    try {
      long toWaitFor;
      synchronized (this) {
        eventStartSync();
        /* Record current blockOffset. This might be changed inside
         * flushBuffer() where a partial checksum chunk might be flushed.
         * After the flush, reset the bytesCurBlock back to its previous value,
         * any partial checksum chunk will be sent now and in next packet.
         */
        long saveOffset = bytesCurBlock;
        DFSOutputStreamPacket oldCurrentPacket = currentPacket;

        // flush checksum buffer as an incomplete chunk
        flushBuffer(false, shouldKeepPartialChunkData());
        // bytesCurBlock potentially incremented if there was buffered data
        
        eventSyncStartWaitAck();
        
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("DFSClient flush() : bytesCurBlock " + bytesCurBlock +
                    " lastFlushOffset " + lastFlushOffset);
        }

        // Flush only if we haven't already flushed till this offset.
        if (lastFlushOffset != bytesCurBlock) {
          assert bytesCurBlock > lastFlushOffset;
          // record the valid offset of this flush
          lastFlushOffset = bytesCurBlock;
          enqueueCurrentPacket();
        } else {
          // just discard the current packet since it is already been sent.
          if (oldCurrentPacket == null && currentPacket != null) {
            // If we didn't previously have a packet queued, and now we do,
            // but we don't plan on sending it, then we should not
            // skip a sequence number for it!
            currentSeqno--;
          }
          currentPacket = null;
        }

        if (shouldKeepPartialChunkData()) {
          // Restore state of stream. Record the last flush offset
          // of the last full chunk that was flushed.
          //
          bytesCurBlock = saveOffset;
        }
        toWaitFor = lastQueuedSeqno;
      }

      waitForAckedSeqno(toWaitFor);
      
      eventSyncPktAcked();

      // If any new blocks were allocated since the last flush,
      // then persist block locations on namenode.
      //
      boolean willPersist;
      synchronized (this) {
        willPersist = persistBlocks;
        persistBlocks = false;
      }
      if (willPersist) {
        dfsClient.namenode.fsync(src, dfsClient.clientName);
      }
      long timeval = System.currentTimeMillis() - start;
      dfsClient.metrics.incSyncTime(timeval);

      eventEndSync();

    } catch (IOException e) {
        lastException = new IOException("IOException flush:", e);
        closed = true;
        closeThreads();
        throw e;
    }
  }
  
  private Block getLastBlock() {
    return this.block;
  }

  /**
   * Returns the number of replicas of current block. This can be different
   * from the designated replication factor of the file because the NameNode
   * does not replicate the block to which a client is currently writing to.
   * The client continues to write to a block even if a few datanodes in the
   * write pipeline have failed. If the current block is full and the next
   * block is not yet allocated, then this API will return 0 because there are
   * no replicas in the pipeline.
   */
  public int getNumCurrentReplicas() throws IOException {
    synchronized(dataQueue) {
      if (nodes == null) {
        return blockReplication;
      }
      return nodes.length;
    }
  }
  
  public DFSWriteProfilingData getProfileData() {
    return (DFSWriteProfilingData) profileData;
  }

  /**
   * Waits till all existing data is flushed and confirmations
   * received from datanodes.
   */
  private void flushInternal() throws IOException {
    isClosed();
    dfsClient.checkOpen();

    long toWaitFor;
    synchronized (this) {
      enqueueCurrentPacket();
      toWaitFor = lastQueuedSeqno;
    }

    waitForAckedSeqno(toWaitFor);
  }

  private void waitForAckedSeqno(long seqnumToWaitFor) throws IOException {
    boolean interrupted = false;

    synchronized (ackQueue) {
      while (!closed) {
        isClosed();
        if (lastAckedSeqno >= seqnumToWaitFor) {
          break;
        }
        try {
          ackQueue.wait();
        } catch (InterruptedException ie) {
          interrupted = true;
        }
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    isClosed();
  }

  /**
   * Closes this output stream and releases any system
   * resources associated with this stream.
   */
  @Override
  public void close() throws IOException {
    try {
      if (closed) {
        IOException e = lastException;
        if (e == null)
          return;
        else
          throw e;
      }

      try {
        closeInternal();

        if (s != null) {
          for (int i = 0; i < s.length; i++) {
            s[i].close();
          }
          s = null;
        }
      } catch (IOException e) {
        lastException = e;
        throw e;
      }
      if (profileData != null && profileData.isAutoPrintWhileClose()) {
        DFSClient.LOG.info("Write Profile for " + this.src + ":"
            + profileData.toString());
      }
    } finally {
      // We always try to remove the connection from the lease to
      // avoid memory leak. In case of failed close(), it is possible
      // that later users' retry of close() could succeed but fail on
      // lease expiration. Since clients don't possibly write more data
      // after calling close(), this case doesn't change any guarantee
      // of data itself.
      dfsClient.leasechecker.remove(src);
    }
  }

  /**
   * Harsh abort method that should only be used from tests - this
   * is in order to prevent pipeline recovery when eg a DN shuts down.
   */
  void abortForTests() throws IOException {
    if (streamer != null) {
      streamer.close();
    }
    if (response != null) {
      response.close();
    }
    closed = true;
  }

  /**
   * Aborts this output stream and releases any system
   * resources associated with this stream.
   */
  synchronized void abort() throws IOException {
    if (closed) {
      return;
    }
    setLastException(new IOException("Lease timeout of " +
                                     (dfsClient.hdfsTimeout/1000) + " seconds expired."));
    closeThreads();
  }


  // shutdown datastreamer and responseprocessor threads.
  private void closeThreads() throws IOException {
    try {
      if (streamer != null) {
        streamer.close();
        streamer.join();
      }

      // shutdown response after streamer has exited.
      if (response != null) {
        response.close();
        response.join();
        response = null;
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Failed to shutdown response thread");
    }
  }

  /**
   * Closes this output stream and releases any system
   * resources associated with this stream.
   */
  private synchronized void closeInternal() throws IOException {
    dfsClient.checkOpen();
    isClosed();

    try {
        eventStartWrite();

        flushBuffer(true, false);       // flush from all upper layers

        eventCloseAfterFlushBuffer();
        
        // Mark that this packet is the last packet in block.
        // If there are no outstanding packets and the last packet
        // was not the last one in the current block, then create a
        // packet with empty payload.
        synchronized (dataQueue) {
          if (currentPacket == null && bytesCurBlock != 0) {
            WritePacketClientProfile pktProfile = null;
            if (getProfileData() != null) {
              pktProfile = getProfileData().getWritePacketClientProfile();
            }
            currentPacket = DFSOutputStreamPacketFactory.getPacket(
              DFSOutputStream.this, ifPacketIncludeVersion(),
              getPacketVersion(), packetSize, chunksPerPacket, bytesCurBlock, pktProfile);
          }
          if (currentPacket != null) {
            currentPacket.lastPacketInBlock = true;
          }
        }
                
      flushInternal();             // flush all data to Datanodes
      isClosed(); // check to see if flushInternal had any exceptions
      closed = true; // allow closeThreads() to showdown threads

      closeThreads();

      synchronized (dataQueue) {
        if (blockStream != null) {
          blockStream.writeInt(0); // indicate end-of-block to datanode
          blockStream.close();
          blockReplyStream.close();
        }
        if (s != null) {
          for (int i = 0; i < s.length; i++) {
            s[i].close();
          }
          s = null;
        }
      }

      streamer = null;
      blockStream = null;
      blockReplyStream = null;

      eventCloseReceivedAck();
      
      dfsClient.closeFile(src, lastBlkOffset, getLastBlock());
      
      eventEndClose();
    } finally {
      closed = true;
    }
  }

  void setArtificialSlowdown(long period) {
    artificialSlowdown = period;
  }

  synchronized void setChunksPerPacket(int value) {
    chunksPerPacket = Math.min(chunksPerPacket, value);
    packetSize = getPacketHeaderLen() + DFSClient.SIZE_OF_INTEGER +
						 (checksum.getBytesPerChecksum() +
							checksum.getChecksumSize()) * chunksPerPacket;
  }

  synchronized void setTestFilename(String newname) {
    src = newname;
  }

  /**
   * Returns the size of a file as it was when this stream was opened
   */
  long getInitialLen() {
    return initialFileSize;
  }
  
  private void eventStartEnqueuePacket() {
    if (getProfileData() != null) {
      getProfileData().startEnqueuePacket();
    }
  }

  private void eventEndEnquePacket() {
    if (getProfileData() != null) {
      getProfileData().endEnquePacket();
    }
  }

  private void eventStartSync() {
    if (getProfileData() != null) {
      getProfileData().startSync();
    }
  }

  private void eventSyncStartWaitAck() {
    if (getProfileData() != null) {
      getProfileData().syncStartWaitAck();
    }
  }

  private void eventSyncPktAcked() {
    if (getProfileData() != null) {
      getProfileData().syncPktAcked();
    }
  }

  private void eventEndSync() {
    if (getProfileData() != null) {
      getProfileData().endSync();
    }
  }

  private void eventCloseAfterFlushBuffer() {
    if (getProfileData() != null) {
      getProfileData().closeAfterFlushBuffer();
    }
  }

  private void eventCloseReceivedAck() {
    if (getProfileData() != null) {
      getProfileData().closeReceivedAck();
    }
  }

  private void eventEndClose() {
    if (getProfileData() != null) {
      getProfileData().endClose();
    }
  }

  public void eventStartReceiveAck() {
    if (getProfileData() != null) {
      getProfileData().startReceiveAck();
    }
  }

  int getPacketHeaderLen() {
    return DataNode.getPacketHeaderLen(ifPacketIncludeVersion());
  }
  
  long incAndGetCurrentSeqno() {
    return currentSeqno++;
  }
  
  int getPacketVersion() {
    return packetVersion;
  }

  boolean ifPacketIncludeVersion() {
    return pktIncludeVersion;
  }
  
  boolean ifForceSync() {
    return forceSync;
  }
  
  int getBytesPerChecksum() {
    return checksum.getBytesPerChecksum();
  }
  
  int getChecksumSize() {
    return checksum.getChecksumSize();
  }

  @Override
  protected boolean shouldKeepPartialChunkData() throws IOException {
    return this.dfsClient.getDataTransferProtocolVersion() <
        DataTransferProtocol.NOT_RESEND_PARTIAL_CHUNK_VERSION;
  }
}
