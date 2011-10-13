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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.util.Date;
import java.util.Collection;
import java.text.SimpleDateFormat;
import java.lang.Thread;
import java.net.URI;
import java.net.InetSocketAddress;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.http.HttpServer;

/**
 * This class drives the ingest of transaciton logs from primary.
 * It also implements periodic checkpointing of the  primary namenode.
 */

public class Standby implements Runnable {

  public static final Log LOG = AvatarNode.LOG;
  private static final long CHECKPOINT_DELAY = 10000; // 10 seconds
  private AvatarNode avatarNode;
  private Configuration confg; // configuration of local standby namenode
  private Configuration startupConf; // original configuration of AvatarNode
  private FSImage fsImage; // fsImage of the current namenode.
  private FSNamesystem fsnamesys; // fsnamesystem of the local standby namenode
  volatile private Ingest ingest;   // object that processes transaction logs from primary
  volatile private Thread ingestThread;  // thread that is procesing the transaction log
  volatile private boolean running;
  volatile boolean checkpointInProgress; // Standby is checkpointing
  private final String machineName; // host name of name node

  //
  // These are for the Secondary NameNode.
  //
  private String fsName;                    // local namenode http name
  private InetSocketAddress nameNodeAddr;   // remote primary namenode address
  private NamenodeProtocol primaryNamenode; // remote primary namenode
  private HttpServer infoServer;
  private int infoPort;
  private String infoBindAddress;
  private long checkpointPeriod;        // in seconds
  private long checkpointSize;    // size (in MB) of current Edit Log
  private long lastCheckpointTime;
  private long earlyScheduledCheckpointTime = Long.MAX_VALUE;
  private long sleepBetweenErrors;
  private boolean checkpointEnabled;
  volatile private Thread backgroundThread;  // thread for secondary namenode 
  private CheckpointSignature sig;

  // The Standby can either be processing transaction logs
  // from the primary namenode or it could be doing a checkpoint to upload a merged
  // fsimage to the primary.
  // The startupConf is the original configuration that was used to start the
  // AvatarNode. It is used by the secondary namenode to talk to the primary.
  // The "conf" is the configuration of the local standby namenode.
  //
  Standby(AvatarNode avatarNode, Configuration startupConf, Configuration conf) 
    throws IOException {
    this.running = true;
    this.avatarNode = avatarNode;
    this.confg = conf;
    this.startupConf = startupConf;
    this.fsImage = avatarNode.getFSImage();
    this.fsnamesys = avatarNode.getNamesystem();
    this.sleepBetweenErrors = startupConf.getInt("hdfs.avatarnode.sleep", 5000);
    initSecondary(startupConf); // start webserver for secondary namenode

    this.machineName =
      DNS.getDefaultHost(conf.get("dfs.namenode.dns.interface","default"),
                         conf.get("dfs.namenode.dns.nameserver","default"));
    LOG.info("machineName=" + machineName);
    
  }

  public void run() {
    backgroundThread = Thread.currentThread();
    while (running) {
      try {
        // if the checkpoint periodicity or the checkpoint size has
        // exceeded the configured parameters, then also we have to checkpoint
        //
        long now = AvatarNode.now();
        // Check to see if the primary is somehow checkpointing itself. If so, then 
        // exit the StandbyNode, we cannot have two daemons checkpointing the same
        // namespace at the same time
        if (hasStaleCheckpoint()) {
          backgroundThread = null;
          quiesce();
          break;
        }

        if (lastCheckpointTime == 0 ||
            (lastCheckpointTime + 1000 * checkpointPeriod < now) ||
            (earlyScheduledCheckpointTime < now) ||
            avatarNode.editSize(confg) > checkpointSize) {

          // schedule an early checkpoint if this current one fails.
          earlyScheduledCheckpointTime = now + CHECKPOINT_DELAY;
          doCheckpoint();
          earlyScheduledCheckpointTime = Long.MAX_VALUE;
          lastCheckpointTime = now;

          // set the last expected checkpoint time on the primary.
          avatarNode.setStartCheckpointTime(startupConf);
        }

        // if edit and edits.new both exists, then we schedule a checkpoint
        // to occur very soon.
        // Only reschedule checkpoint if it is not scheduled to occur even sooner
        if ((avatarNode.twoEditsFile(startupConf)) &&
                (earlyScheduledCheckpointTime > now + CHECKPOINT_DELAY)) {
          LOG.warn("Standby: edits and edits.new found, scheduling early checkpoint.");
          earlyScheduledCheckpointTime = now + CHECKPOINT_DELAY;
        }

        // if the checkpoint creation has switched off ingesting, then we restart the
        // ingestion here.
        if (ingest == null) {
          ingest = new Ingest(this, fsnamesys, confg, 
                              avatarNode.getRemoteEditsFile(startupConf));
          ingestThread = new Thread(ingest);
          ingestThread.start(); // start thread to process transaction logs
        }
        try {
          Thread.sleep(sleepBetweenErrors);
        } catch (InterruptedException e) {
          // give a change to exit this thread, if necessary
        }
      } catch (IOException e) {
        LOG.warn("Standby: encounter exception " + StringUtils.stringifyException(e));
        try {
          Thread.sleep(sleepBetweenErrors);
        } catch (InterruptedException e1) {
          // give a change to exit this thread, if necessary
        }

        // since we had an error, we have to cleanup the ingest thread
        if (ingest != null) {
          ingest.stop();
          try {
            ingestThread.join();
            LOG.info("Standby: error cleanup Ingest thread exited.");
          } catch (InterruptedException em) {
            String msg = "Standby: error cleanup Ingest thread did not exit. " + em;
            LOG.info(msg);
            throw new RuntimeException(msg);
          }
          ingest = null;
          ingestThread = null;
        }
      } catch (Throwable e) {
        LOG.warn("Standby: encounter exception ", e);
      }
    }
  }

  synchronized void shutdown() {
    if (!running) {
      return;
    }
    if (infoServer != null) {
      try {
      LOG.info("Shutting down secondary info server");
      infoServer.stop();
      infoServer = null;
      } catch (Exception ex) {
        LOG.error("Error shutting down infoServer", ex);
      }
    }
  }

  //
  // stop checkpointing, read edit and edits.new(if it exists) 
  // into local namenode
  //
  synchronized void quiesce() throws IOException {
    // have to wait for the main thread to exit here
    // first stop the main thread before stopping the ingest thread
    LOG.info("Standby: Quiescing.");
    running = false;
    try {
      if (backgroundThread != null) {
        backgroundThread.join();
        backgroundThread = null;
      }
    } catch (InterruptedException e) {
      LOG.info("Standby: quiesce interrupted.");
      throw new IOException(e.getMessage());
    }
    try {
      if (infoServer != null) {
        infoServer.stop();
        infoServer= null;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }

    // Ingest till end of edits log
    if (ingest == null) {
      ingest = new Ingest(this, fsnamesys, confg, 
                          avatarNode.getRemoteEditsFile(startupConf));
      ingestThread = new Thread(ingest);
      ingestThread.start(); // start thread to process edits.new
    }
    
    ingest.quiesce(); // process everything till end of transaction log
    try {
      ingestThread.join();
      LOG.info("Standby: quiesce Ingest thread exited.");
    } catch (InterruptedException e) {
      LOG.info("Standby: quiesce interrupted.");
      throw new IOException(e.getMessage());
    }
    
    // verify that the entire transaction log was truly consumed
    if (!ingest.getIngestStatus()) {
      String emsg = "Standby: quiesce could not successfully ingest transaction log.";
      LOG.warn(emsg);
      throw new IOException(emsg);
    }
    ingest = null;
    ingestThread = null;
    
    // if edits.new exists, then read it in too
    // We are performing a failover, so checkpointing is no longer
    // relevant. Set it to false so Ingest will run on the edits.new
    checkpointInProgress = false;
    File editnew = avatarNode.getRemoteEditsFileNew(startupConf);
    if (editnew.exists()) {
      ingest = new Ingest(this, fsnamesys, confg, editnew);
      ingestThread = new Thread(ingest);
      ingestThread.start(); // start thread to process edits.new
      ingest.quiesce();     // read till end of edits.new
      try {
        ingestThread.join(); // wait till ingestion is complete
        LOG.info("Standby: quiesce Ingest thread for edits.new exited");
      } catch (InterruptedException e) {
        LOG.info("Standby: quiesce interrupted.");
        throw new IOException(e.getMessage());
      }
      if (!ingest.getIngestStatus()) {
        String emsg = "Standby: quiesce could not ingest transaction log for edits.new";
        LOG.warn(emsg);
         throw new IOException(emsg);
      }
      ingest = null;
      ingestThread = null;
    }
  }

  /**
   * Check to see if the remote namenode is doing its own checkpointing. This can happen 
   * when the remote namenode is restarted. This method returns true if the remote 
   * namenode has done an unexpected checkpoint. This method retrieves the fstime of the
   * remote namenode and matches it against the fstime of the checkpoint when this
   * AvatarNode did a full-sync of the edits log.
   */
  boolean hasStaleCheckpoint() throws IOException {
    long remotefsTime = avatarNode.readRemoteFstime(startupConf);
    long localfsTime = avatarNode.getStartCheckpointTime();
    if (remotefsTime != localfsTime) {
      LOG.warn("Standby: The remote active namenode might have been restarted.");
      LOG.warn("Standby: The fstime of checkpoint from which the Standby was created is " +
               AvatarNode.dateForm.format(new Date(localfsTime)) +
               " but remote fstime is " + 
               AvatarNode.dateForm.format(new Date(remotefsTime)));
      avatarNode.doRestart();
      return true;
    }
    return false;
  }

  /**
   * writes the in memory image of the local namenode to the fsimage
   * and tnen uploads this image to the primary namenode. The transaction 
   * log on the primary is purged too.
   */
  private void doCheckpoint() throws IOException {
    // Tell the remote namenode to start logging transactions in a new edit file
    // Retuns a token that would be used to upload the merged image.
    if (!checkpointEnabled) {
      // This means the Standby is not meant to checkpoint the primary
      LOG.info("Checkpointing is disabled - return");
      return;
    }
    CheckpointSignature sig = null;
    try {
      LOG.info("Standby: startCheckpoint Roll edits logs of primary namenode "
          + nameNodeAddr);
      sig = (CheckpointSignature) primaryNamenode.rollEditLog();
    } catch (IOException ex) {
      // In this case we can return since we did not kill the Ingest thread yet
      // Nothing prevents us from doing the next checkpoint attempt
      LOG.warn("Standby: Roll Edits on the primary node failed. Returning");
      return;
    }

    setLastRollSignature(sig);
    // Ingest till end of edits log
    if (ingest == null) {
      LOG.info("Standby: creating ingest thread to process all transactions.");
      ingest = new Ingest(this, fsnamesys, confg, 
                          avatarNode.getRemoteEditsFile(startupConf));
      ingestThread = new Thread(ingest);
      ingestThread.start(); // start thread to process edits.new
    }
    // make a last pass over the edits and then quit ingestion
    ingest.quiesce(sig);
    try {
      ingestThread.join();
      LOG.info("Standby: finished quitting ingest thread just before ckpt.");
    } catch (InterruptedException e) {
      LOG.info("Standby: quiesce interrupted.");
      throw new RuntimeException("Interrupted Exception waiting for Ingest " +
      		"to finish reading edits.", e);
    }
    if (!ingest.getIngestStatus()) {
      ingest = null;
      ingestThread = null;
      String emsg = "Standby: doCheckpoint could not ingest transaction log.";
      emsg += " This is real bad because we do not know how much edits we have consumed.";
      emsg += " It is better to exit the AvatarNode here.";
      LOG.error(emsg);
      throw new RuntimeException(emsg);
    }
    ingest = null;
    ingestThread = null;

    /**
     * From now on Ingest thread needs to know if the checkpoint was started and never finished.
     * This would mean that it doesn't have to read the edits, since they were already processed
     * to the end as a part of a checkpoint.
     */
    checkpointInProgress = true;

    fsnamesys.writeLock();
    try {
      // roll transaction logs on local namenode
      LOG.info("Standby: Roll edits logs to local namenode.");
      fsImage.rollEditLog();

      // save a checkpoint of the current namespace of the local Namenode
      // We should ideally use fsnamesystem.saveNamespace but that works
      // only if namenode is not in safemode.
      LOG.info("Standby: Save fsimage on local namenode.");
      fsnamesys.saveNamespace(false, false);
    } catch (IOException ex) {
      // Standby failed to save fsimage locally. Need to reinitialize
      String msg = "Standby: doCheckpoint failed to checkpoint itself, so " +
      		"no image can be uploaded to the primary. The only course of action " +
      		"is to start from the very beginning by reinitializing AvatarNode";
      LOG.error(msg, ex);
      throw new RuntimeException(msg, ex);
    } finally {
      fsnamesys.writeUnlock();
    }
    try {
      // copy image to primary namenode
      LOG.info("Standby: Upload fsimage to remote namenode.");
      putFSImage(sig);

      // make transaction to primary namenode to switch edit logs
      LOG.info("Standby: Roll fsimage on primary namenode.");
      primaryNamenode.rollFsImage(new CheckpointSignature(fsImage));
      checkpointInProgress = false;
      setLastRollSignature(null);
      LOG.info("Standby: Checkpoint done. New Image Size: "
          + fsImage.getFsImageName().length());
    } catch (IOException ex) {
      // If the rollFsImage has actually succeeded on the Primary, but
      // returned with the exception on recreation our Ingest will throw
      // a runtime exception and the Avatar will be restarted.
      LOG.error("Rolling the fsimage on the Primary node failed.", ex);
    }
  }

  /**
   * Initialize the webserver so that the primary namenode can fetch
   * transaction logs from standby via http.
   */
  void initSecondary(Configuration conf) throws IOException {

    nameNodeAddr = avatarNode.getRemoteNamenodeAddress(conf);
    this.primaryNamenode =
        (NamenodeProtocol) RPC.waitForProxy(NamenodeProtocol.class,
            NamenodeProtocol.versionID, nameNodeAddr, conf);

    fsName = avatarNode.getRemoteNamenodeHttpName(conf);

    // Initialize other scheduling parameters from the configuration
    checkpointEnabled = conf.getBoolean("fs.checkpoint.enabled", false);
    checkpointPeriod = conf.getLong("fs.checkpoint.period", 3600);
    checkpointSize = conf.getLong("fs.checkpoint.size", 4194304);

    // initialize the webserver for uploading files.
    String infoAddr = 
      NetUtils.getServerAddress(conf,
                                "dfs.secondary.info.bindAddress",
                                "dfs.secondary.info.port",
                                "dfs.secondary.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    infoServer = new HttpServer("secondary", infoBindAddress, tmpInfoPort,
        tmpInfoPort == 0, conf);
    infoServer.setAttribute("name.system.image", fsImage);
    this.infoServer.setAttribute("name.conf", conf);
    infoServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    infoServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = infoServer.getPort();
    conf.set("dfs.secondary.http.address", infoBindAddress + ":" +infoPort);
    LOG.info("Secondary Web-server up at: " + infoBindAddress + ":" +infoPort);
    LOG.warn("Checkpoint Period   :" + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.warn("Log Size Trigger    :" + checkpointSize + " bytes " +
             "(" + checkpointSize/1024 + " KB)");
  }

  /**
   * Copy the new fsimage into the NameNode
   */
  private void putFSImage(CheckpointSignature sig) throws IOException {
    String fileid = "putimage=1&port=" + infoPort +
      "&machine=" +
      machineName +
      "&token=" + sig.toString();
    LOG.info("Standby: Posted URL " + fsName + fileid);
    TransferFsImage.getFileClient(fsName, fileid, (File[])null, false);
  }
  
  public synchronized void setLastRollSignature(CheckpointSignature sig) {
    this.sig = sig;
  }
  
  public synchronized CheckpointSignature getLastRollSignature() {
    return this.sig;
  }
  
  public boolean fellBehind() {
    // Ingest is catching up or there is no ingest
    return ingest == null || ingest.catchingUp();
  }

  public long getLagBytes() {
    return this.ingest == null ? -1L : this.ingest.getLagBytes();
  }
}
