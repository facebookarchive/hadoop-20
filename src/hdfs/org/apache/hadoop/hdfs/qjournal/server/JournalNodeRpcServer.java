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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalRequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetImageManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetStorageStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.ShortVoid;
import org.apache.hadoop.ipc.FastProtocolRegister;
import org.apache.hadoop.ipc.FastProtocolRegister.FastProtocolId;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.hdfs.util.InjectionEvent;

public class JournalNodeRpcServer implements QJournalProtocol {
  public static final Log LOG = LogFactory.getLog(Journal.class);
  private static final int HANDLER_COUNT = 5;
  private final JournalNode jn;
  private final Server server;
  private final Configuration conf;

  JournalNodeRpcServer(Configuration conf, JournalNode jn) throws IOException {
    this.jn = jn;
    
    Configuration confCopy = new Configuration(conf);
    
    // Ensure that nagling doesn't kick in, which could cause latency issues.
    confCopy.setBoolean("ipc.server.tcpnodelay", true);
    // reader threads will handle the RPC calls
    confCopy.setBoolean("ipc.direct.handling", true);
    // set the number of reader threads, should be at least the number of
    // served namenodes
    confCopy.setInt(Server.IPC_SERVER_RPC_READ_THREADS_KEY, confCopy.getInt(
        JournalConfigKeys.DFS_QJOURNAL_IPC_READER_KEY,
        JournalConfigKeys.DFS_QJOURNAL_IPC_READER_DEFAULT));

    InetSocketAddress addr = getAddress(confCopy);

    this.server = RPC.getServer(this, addr.getAddress().getHostAddress(), addr.getPort(),
        HANDLER_COUNT, false, confCopy, false);
    this.conf = confCopy;
  }

  void start() throws IOException {
    this.server.start();
  }

  public InetSocketAddress getAddress() {
    return server.getListenerAddress();
  }
  
  void join() throws InterruptedException {
    this.server.join();
  }
  
  void stop() {
    this.server.stop();
  }
  
  static InetSocketAddress getAddress(Configuration conf) {
    String addr = conf.get(
        JournalConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY,
        JournalConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr, 0);
  }

  @Override
  public boolean isJournalFormatted(byte[] journalId) throws IOException {
    return jn.getOrCreateJournal(journalId).isJournalFormatted();
  }
  
  @Override
  public boolean isImageFormatted(byte[] journalId) throws IOException {
    return jn.getOrCreateJournal(journalId).isImageFormatted();
  }

  @Override
  public GetJournalStateResponseProto getJournalState(byte[] journalId)
        throws IOException {
    long epoch = jn.getOrCreateJournal(journalId).getLastPromisedEpoch(); 
    
    GetJournalStateResponseProto ret = new GetJournalStateResponseProto();
    ret.setLastPromisedEpoch(epoch);
    ret.setHttpPort(jn.getBoundHttpAddress().getPort());
    return ret;
  }

  @Override
  public NewEpochResponseProto newEpoch(byte[] journalId,
      NamespaceInfo nsInfo,
      long epoch) throws IOException {
    NewEpochResponseProto p = jn.getOrCreateJournal(journalId).newEpoch(nsInfo, epoch);
    return p;
  }

  @Override
  public void transitionJournal(byte[] journalId, NamespaceInfo nsInfo,
      Transition transition, StartupOption startOpt) throws IOException {
    jn.getOrCreateJournal(journalId).transitionJournal(nsInfo, transition,
        startOpt);
  }

  @Override
  public void transitionImage(byte[] journalId, NamespaceInfo nsInfo,
      Transition transition, StartupOption startOpt) throws IOException {
    jn.getOrCreateJournal(journalId).transitionImage(nsInfo, transition,
        startOpt);
  }

  @Override
  public ShortVoid journal(JournalRequestInfo reqInfo) throws IOException {
    InjectionHandler.processEventIO(InjectionEvent.QJM_JOURNALNODE_JOURNAL,
        conf, this.server.getListenerAddress());
    long startTime = System.currentTimeMillis();
    ShortVoid ret = jn.getOrCreateJournal(reqInfo.getJournalId()).journal(reqInfo,
        reqInfo.getSegmentTxId(), reqInfo.getFirstTxId(), reqInfo.getNumTxns(),
        reqInfo.getRecords());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time spent in journal: " + (System.currentTimeMillis() - startTime) + 
          ". RequestInfo: " + reqInfo.toString());
    }
    return ret;
  }
  
  @Override
  public void heartbeat(RequestInfo reqInfo) throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId())
      .heartbeat(reqInfo);
  }

  @Override
  public void startLogSegment(RequestInfo reqInfo, long txid)
      throws IOException {
    InjectionHandler.processEventIO(
        InjectionEvent.QJM_JOURNALNODE_STARTSEGMENT, conf);
    jn.getOrCreateJournal(reqInfo.getJournalId())
        .startLogSegment(reqInfo, txid);
  }

  @Override
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId())
      .finalizeLogSegment(reqInfo, startTxId, endTxId);
  }

  @Override
  public void purgeLogsOlderThan(RequestInfo reqInfo, long minTxIdToKeep)
      throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId())
      .purgeLogsOlderThan(reqInfo, minTxIdToKeep);
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(byte[] jid,
      long sinceTxId) throws IOException {
    
    RemoteEditLogManifest manifest = jn.getOrCreateJournal(jid)
        .getEditLogManifest(sinceTxId);
    
    GetEditLogManifestResponseProto ret = new GetEditLogManifestResponseProto();
    ret.setLogs(manifest.getLogs());
    ret.setHttpPort(jn.getBoundHttpAddress().getPort());
    
    if (JournalNode.LOG.isDebugEnabled()) {
      JournalNode.LOG.info("Returning manifest " + manifest.toString());
    }

    return ret;
  }

  @Override
  public PrepareRecoveryResponseProto prepareRecovery(RequestInfo reqInfo,
      long segmentTxId) throws IOException {
    return jn.getOrCreateJournal(reqInfo.getJournalId())
        .prepareRecovery(reqInfo, segmentTxId);
  }

  @Override
  public void acceptRecovery(RequestInfo reqInfo, SegmentStateProto log,
      String fromUrl) throws IOException {
    
    jn.getOrCreateJournal(reqInfo.getJournalId())
        .acceptRecovery(reqInfo, log, new URL(fromUrl));
  }
  
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws VersionIncompatible, IOException {
    if (protocol.equals(QJournalProtocol.class.getName())){
      return QJournalProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol: " + protocol);
    }
  }
  
  @Override
  public GetStorageStateProto analyzeJournalStorage(byte[] jid)
      throws IOException {
    return jn.getOrCreateJournal(jid).analyzeJournalStorage();
  }
  
  @Override
  public void saveDigestAndRenameCheckpointImage(RequestInfo reqInfo,
      long txid, MD5Hash digest) throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId())
        .saveDigestAndRenameCheckpointImage(txid, digest);

  }

  @Override
  public GetImageManifestResponseProto getImageManifest(byte[] jid,
      long sinceTxId) throws IOException {
    GetImageManifestResponseProto ret = new GetImageManifestResponseProto();
    ret.setImages(jn.getOrCreateJournal(jid).getImageManifest(sinceTxId)
        .getImages());
    return ret;
  }
  
  @Override
  public GetStorageStateProto analyzeImageStorage(byte[] jid)
      throws IOException {
    return jn.getOrCreateJournal(jid).analyzeImageStorage();
  }
  
  // register fast protocol
  public static void init() {
    try {
      FastProtocolRegister.register(FastProtocolId.SERIAL_VERSION_ID_1,
          QJournalProtocol.class.getMethod("journal",
              JournalRequestInfo.class));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
