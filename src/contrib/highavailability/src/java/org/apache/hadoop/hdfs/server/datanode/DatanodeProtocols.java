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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.IncrementalBlockReport;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtocolSignature;

/**********************************************************************
 * Protocol that a DFS datanode uses to communicate with the NameNode.
 * This class encapsules multiple objects that expose DatanodeProtocol.
 *
 **********************************************************************/
public class DatanodeProtocols implements DatanodeProtocol {
  public final static int DNA_BACKOFF = -1;
  // Instruct the datanode to finish processing all primary avatar commands.
  public final static int DNA_CLEARPRIMARY = -2;
  // A no-op command.
  public final static int DNA_RETRY = -3;
  // Instruct datanodes to prepare for failover
  public final static int DNA_PREPAREFAILOVER = -4;
  
  public static final Log LOG = LogFactory.getLog(DatanodeProtocols.class.getName());

  DatanodeProtocol node[];
  int numProtocol;

  private String errMessage = " should occur individually " +
                              " for each namenode.";

  /**
   * Maximum number of protocol object encapsulated here
   */
  DatanodeProtocols(int max) {
    numProtocol = max;
    node = new DatanodeProtocol[max];
    for (int i = 0; i < max; i++) {
      node[i] = null;
    }
  }

  void setDatanodeProtocol(DatanodeProtocol prot, int index) {
    this.node[index] = prot;
  }

  /** {@inheritDoc} */
  public long getProtocolVersion(String protocol,
                                 long clientVersion) throws IOException {
    IOException last = new IOException("No DatanodeProtocol found.");
    long lastProt = -1;
    for (int i = 0; i < numProtocol; i++) {
      try {
        if (node[i] != null) {
          long prot =  node[i].getProtocolVersion(protocol, clientVersion);
          if (lastProt != -1) {
            if (prot != lastProt) {
              throw new IOException("Versions of DatanodeProtocol " +
                                    " objects have to be same." +
                                    " Found version " + prot +
                                    " does not match with " + lastProt);
            }
            lastProt = prot;
          }
        }
      } catch (IOException e) {
        last = e;
        LOG.info("Server " + i + " failed at getProtocolVersion.", e);
      }
    }
    if (lastProt == -1) {
      throw last; // fail if all DatanodeProtocol object failed.
    }
    return lastProt; // all objects have the same version
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }
  
  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  @Override
  public DatanodeRegistration register(DatanodeRegistration registration
                                       ) throws IOException {
    throw new IOException("Registration" + errMessage);
  }

  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  @Override
  public DatanodeRegistration register(DatanodeRegistration registration,
                              int dataTransferVersion) throws IOException {
    throw new IOException("Registration" + errMessage);
  }

  public void keepAlive(DatanodeRegistration registration) throws IOException {
    throw new IOException("keepAlive " + errMessage);
  }

  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration,
                                       long capacity,
                                       long dfsUsed, long remaining,
                                       long namespaceUsed,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException {
    throw new IOException("sendHeartbeat" + errMessage);
  }

  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  public DatanodeCommand blockReport(DatanodeRegistration registration,
                                     long[] blocks) throws IOException {
    throw new IOException("blockReport" + errMessage);
  }

  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  public void blocksBeingWrittenReport(DatanodeRegistration registration,
                                     BlockReport blocks) throws IOException {
    throw new IOException("blockReport" + errMessage);
  }
    
  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  public DatanodeCommand blockReport(DatanodeRegistration registration,
                                     BlockReport blocks) throws IOException {
    throw new IOException("blockReport" + errMessage);
  }
    
  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
                                      Block blocksReceivedAndDeleted[])
                                      throws IOException {
    throw new IOException("blockReceivedAndDeleted" + errMessage);
  }

  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
                                      IncrementalBlockReport receivedAndDeletedBlocks)
                                      throws IOException {
    throw new IOException("blockReceivedAndDeleted" + errMessage);
  }

  /** {@inheritDoc} */
  public void errorReport(DatanodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException {
    for (int i = 0; i < numProtocol; i++) {
      try {
        if (node[i] != null) {
          node[i].errorReport(registration, errorCode, msg);
        }
      } catch (IOException e) {
        LOG.info("Server " + i + " failed at errorReport.", e);
      }
    }
  }
    
  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  public NamespaceInfo versionRequest() throws IOException {
    throw new IOException("versionRequest" + errMessage);
  }

  /**
   * This method should not be invoked on the composite 
   * DatanodeProtocols object. You can call these on the individual
   * DatanodeProcol objects.
   */
  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    throw new IOException("processUpgradeCommand" + errMessage);
  }
  
  /** {@inheritDoc} */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    for (int i = 0; i < numProtocol; i++) {
      try {
        if (node[i] != null) {
          node[i].reportBadBlocks(blocks);
        }
      } catch (IOException e) {
        LOG.info("Server " + i + " failed at reportBadBlocks.", e);
      }
    }
  }
  
  /** {@inheritDoc} */
  public long nextGenerationStamp(Block block, boolean fromNN) throws IOException {
    IOException last = new IOException("No DatanodeProtocol found.");
    for (int i = 0; i < numProtocol; i++) {
      try {
        if (node[i] != null) {
          return node[i].nextGenerationStamp(block, fromNN);
        }
      } catch (IOException e) {
        last = e;
        LOG.info("Server " + i + " failed at nextGenerationStamp.", e);
      }
    }
    throw last; // fail if all DatanodeProtocol object failed.
  }

  /** {@inheritDoc} */
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
      ) throws IOException {
    IOException last = new IOException("No DatanodeProtocol found.");
    for (int i = 0; i < numProtocol; i++) {
      try {
        if (node[i] != null) {
          node[i].commitBlockSynchronization(block, newgenerationstamp,
                                             newlength, closeFile,
                                             deleteblock, newtargets);
          return;
        }
      } catch (IOException e) {
        last = e;
        LOG.info("Server " + i + " failed at commitBlockSynchronization.", e);
      }
    }
    throw last; // fail if all DatanodeProtocol object failed.
  }
}
