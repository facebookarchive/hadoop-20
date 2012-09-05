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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.metrics.jvm.JvmMetrics;

/**********************************************************
 * The Secondary NameNode is a helper to the primary NameNode.
 * The Secondary is responsible for supporting periodic checkpoints 
 * of the HDFS metadata. The current design allows only one Secondary
 * NameNode per HDFs cluster.
 *
 * The Secondary NameNode is a daemon that periodically wakes
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * The Secondary NameNode uses the ClientProtocol to talk to the
 * primary NameNode.
 *
 **********************************************************/
public class SecondaryNameNode implements Runnable {
    
  public static final Log LOG = 
    LogFactory.getLog(SecondaryNameNode.class.getName());

  private String fsName;
  private CheckpointStorage checkpointImage;
  private FSNamesystem namesystem;

  private NamenodeProtocol namenode;
  private Configuration conf;
  private InetSocketAddress nameNodeAddr;
  private volatile boolean shouldRun;
  private HttpServer infoServer;
  private int infoPort;
  private String infoBindAddress;

  private Collection<URI> checkpointDirs;
  private Collection<URI> checkpointEditsDirs;
  private long checkpointPeriod;	// in seconds
  private long checkpointSize;    // size (in MB) of current Edit Log

  FSImage getFSImage() {
    return checkpointImage;
  }

  /**
   * Create a connection to the primary namenode.
   */
  public SecondaryNameNode(Configuration conf)  throws IOException {
    try {
      initialize(conf);
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }

  private Collection<URI> getFileStorageDirs(Collection<URI> uris) {
    ArrayList<URI> directories = new ArrayList<URI>();
    for (URI uri : uris) {
      if (uri.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) == 0) {
        directories.add(uri);
      }
    }
    return directories;
  }
  
  /**
   * Initialize SecondaryNameNode.
   */
  private void initialize(Configuration conf) throws IOException {
    // initiate Java VM metrics
    JvmMetrics.init("SecondaryNameNode", conf.get("session.id"));
    
    // Create connection to the namenode.
    shouldRun = true;
    nameNodeAddr = NameNode.getAddress(conf);

    this.conf = conf;
    this.namenode =
        (NamenodeProtocol) RPC.waitForProxy(NamenodeProtocol.class,
            NamenodeProtocol.versionID, nameNodeAddr, conf);

    // initialize checkpoint directories
    fsName = getInfoServer();
    checkpointDirs = getFileStorageDirs(NNStorageConfiguration
        .getCheckpointDirs(conf, "/tmp/hadoop/dfs/namesecondary"));
    checkpointEditsDirs = getFileStorageDirs(NNStorageConfiguration
        .getCheckpointEditsDirs(conf, "/tmp/hadoop/dfs/namesecondary")); 
    checkpointImage = new CheckpointStorage(conf);
    checkpointImage.recoverCreate(checkpointDirs, checkpointEditsDirs);

    // Initialize other scheduling parameters from the configuration
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
    infoServer.setAttribute("name.system.image", checkpointImage);
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
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   */
  public void shutdown() {
    shouldRun = false;
    try {
      if (infoServer != null) infoServer.stop();
    } catch (Exception e) {
      LOG.warn("Exception shutting down SecondaryNameNode", e);
    }
    try {
      if (checkpointImage != null) checkpointImage.close();
    } catch(IOException e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }

  //
  // The main work loop
  //
  public void run() {

    //
    // Poll the Namenode (once every 5 minutes) to find the size of the
    // pending edit log.
    //
    long period = 5 * 60;              // 5 minutes
    long lastCheckpointTime = 0;
    if (checkpointPeriod < period) {
      period = checkpointPeriod;
    }

    while (shouldRun) {
      try {
        Thread.sleep(1000 * period);
      } catch (InterruptedException ie) {
        // do nothing
      }
      if (!shouldRun) {
        break;
      }
      try {
        long now = System.currentTimeMillis();

        long size = namenode.getEditLogSize();
        if (size >= checkpointSize || 
            now >= lastCheckpointTime + 1000 * checkpointPeriod) {
          doCheckpoint();
          lastCheckpointTime = now;
        }
      } catch (IOException e) {
        LOG.error("Exception in doCheckpoint: ");
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
        checkpointImage.storage.imageDigest = null;
      } catch (Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint: ");
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
    }
  }

  /**
   * Download <code>fsimage</code> and <code>edits</code>
   * files from the name-node.
   * @return true if a new image has been downloaded and needs to be loaded
   * @throws IOException
   */
  private boolean downloadCheckpointFiles(CheckpointSignature sig
                                      ) throws IOException {
    
    checkpointImage.storage.layoutVersion = sig.layoutVersion;
    checkpointImage.storage.cTime = sig.cTime;
    checkpointImage.storage.checkpointTime = sig.checkpointTime;
    
    boolean downloadImage = true;
    String fileid;
    File[] srcNames;
    if (sig.imageDigest.equals(checkpointImage.storage.imageDigest)) {
      downloadImage = false;
      LOG.info("Image has not changed. Will not download image." + sig.imageDigest);
    } else {
      // get fsimage
      srcNames = checkpointImage.getImageFiles();
      assert srcNames.length > 0 : "No checkpoint targets.";
      TransferFsImage.downloadImageToStorage(fsName, HdfsConstants.INVALID_TXID, checkpointImage, true, srcNames);
      checkpointImage.storage.imageDigest = sig.imageDigest;
      LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
          srcNames[0].length() + " bytes.");
    }
    // get edits file
    fileid = "getedit=1";
    srcNames = checkpointImage.getEditsFiles();
    assert srcNames.length > 0 : "No checkpoint targets.";
    TransferFsImage.downloadEditsToStorage(fsName, new RemoteEditLog(), checkpointImage, false);
    LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
        srcNames[0].length() + " bytes.");

    checkpointImage.checkpointUploadDone(null);
    
    return downloadImage;
  }

  /**
   * Returns the Jetty server that the Namenode is listening on.
   */
  private String getInfoServer() throws IOException {
    URI fsName = FileSystem.getDefaultUri(conf);
    if (!"hdfs".equals(fsName.getScheme())) {
      throw new IOException("This is not a DFS");
    }
    return NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                                     "dfs.info.port", "dfs.http.address");
  }

  /**
   * Create a new checkpoint
   */
  boolean doCheckpoint() throws IOException {

    LOG.info("Checkpoint starting");
    
    // Do the required initialization of the merge work area.
    startCheckpoint();

    // Tell the namenode to start logging transactions in a new edit file
    // Returns a token that would be used to upload the merged image.
    CheckpointSignature sig = (CheckpointSignature)namenode.rollEditLog();
    NNStorage dstStorage = checkpointImage.storage;
    
    // Make sure we're talking to the same NN!
    if (checkpointImage.getNamespaceID() != 0) {
      // If the image actually has some data, make sure we're talking
      // to the same NN as we did before.
      sig.validateStorageInfo(checkpointImage.storage);
    } else {
      // if we're a fresh 2NN, just take the storage info from the server
      // we first talk to.
      dstStorage.setStorageInfo(sig);
    }
    
    // error simulation code for junit test
    InjectionHandler.processEventIO(InjectionEvent.SECONDARYNAMENODE_CHECKPOINT0);

    boolean loadImage = downloadCheckpointFiles(sig);   // Fetch fsimage and edits
    
    doMerge(sig, loadImage);                   // Do the merge
  
    //
    // Upload the new image into the NameNode. Then tell the Namenode
    // to make this new uploaded image as the most current image.
    //
    
    //TODO temporary
    //long txid = checkpointImage.getLastAppliedTxId();
    long txid = HdfsConstants.INVALID_TXID;
    TransferFsImage.uploadImageFromStorage(fsName, 
        InetAddress.getLocalHost().getHostAddress(), infoPort,
        checkpointImage.storage, txid);

    // error simulation code for junit test
    InjectionHandler.processEventIO(InjectionEvent.SECONDARYNAMENODE_CHECKPOINT1);

    namenode.rollFsImage(new CheckpointSignature(checkpointImage));
    checkpointImage.storage.setMostRecentCheckpointTxId(sig.mostRecentCheckpointTxId);
    checkpointImage.endCheckpoint();

    LOG.info("Checkpoint done. New Image Size: " 
              + checkpointImage.getFsImageName().length());
    return loadImage;
  }

  private void startCheckpoint() throws IOException {
    checkpointImage.storage.unlockAll();
    checkpointImage.getEditLog().close();
    checkpointImage.recoverCreate(checkpointDirs, checkpointEditsDirs);
    checkpointImage.startCheckpoint();
  }

  /**
   * Merge downloaded image and edits and write the new image into
   * current storage directory.
   */
  private void doMerge(CheckpointSignature sig, boolean loadImage) throws IOException {
    if (loadImage) {  // create an empty namespace if new image
      namesystem = new FSNamesystem(checkpointImage, conf);
    }
    assert namesystem.dir.fsImage == checkpointImage;
    checkpointImage.doMerge(sig, loadImage);
  }

  /**
   * @param argv The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  private int processArgs(String[] argv) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-geteditsize".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-checkpoint".equals(cmd)) {
      if (argv.length != 1 && argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
      if (argv.length == 2 && !"force".equals(argv[i])) {
        printUsage(cmd);
        return exitCode;
      }
    }

    exitCode = 0;
    try {
      if ("-checkpoint".equals(cmd)) {
        long size = namenode.getEditLogSize();
        if (size >= checkpointSize || 
            argv.length == 2 && "force".equals(argv[i])) {
          doCheckpoint();
        } else {
          System.err.println("EditLog size " + size + " bytes is " +
                             "smaller than configured checkpoint " +
                             "size " + checkpointSize + " bytes.");
          System.err.println("Skipping checkpoint.");
        }
      } else if ("-geteditsize".equals(cmd)) {
        long size = namenode.getEditLogSize();
        System.out.println("EditLog size is " + size + " bytes");
      } else {
        exitCode = -1;
        LOG.error(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        LOG.error(cmd.substring(1) + ": "
                  + content[0]);
      } catch (Exception ex) {
        LOG.error(cmd.substring(1) + ": "
                  + ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      //
      exitCode = -1;
      LOG.error(cmd.substring(1) + ": "
                + e.getLocalizedMessage());
    } finally {
      // Does the RPC connection need to be closed?
    }
    return exitCode;
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-geteditsize".equals(cmd)) {
      System.err.println("Usage: java SecondaryNameNode"
                         + " [-geteditsize] [-service serviceName]");
    } else if ("-checkpoint".equals(cmd)) {
      System.err.println("Usage: java SecondaryNameNode"
                         + " [-checkpoint [force]] [-service serviceName]");
    } else {
      System.err.println("Usage: java SecondaryNameNode " +
                         "[-checkpoint [force]] [-service serviceName]\n" +
                         "[-geteditsize] [-service serviceName]\n");
    }
  }

  /**
   * main() has some simple utility methods.
   * @param argv Command line parameters.
   * @exception Exception if the filesystem does not exist.
   */
  public static void main(String[] argv) throws Exception {
    StringUtils.startupShutdownMessage(SecondaryNameNode.class, argv, LOG);
    Configuration tconf = new Configuration();
    try {
      argv = DFSUtil.setGenericConf(argv, tconf);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      printUsage("");
      return;
    }
    if (argv.length >= 1) {
      SecondaryNameNode secondary = new SecondaryNameNode(tconf);
      int ret = secondary.processArgs(argv);
      System.exit(ret);
    }

    // Create a never ending deamon
    Daemon checkpointThread = new Daemon(new SecondaryNameNode(tconf)); 
    checkpointThread.start();
  }

  static class CheckpointStorage extends FSImage {
    /**
     */
    CheckpointStorage(Configuration conf) throws IOException {
      super(conf);
    }

    /**
     * Analyze checkpoint directories.
     * Create directories if they do not exist.
     * Recover from an unsuccessful checkpoint is necessary. 
     * 
     * @param dataDirs
     * @param editsDirs
     * @throws IOException
     */
    void recoverCreate(Collection<URI> dataDirs,
                       Collection<URI> editsDirs) throws IOException {
      Collection<URI> tempDataDirs = new ArrayList<URI>(dataDirs);
      Collection<URI> tempEditsDirs = new ArrayList<URI>(editsDirs);
      storage.setStorageDirectories(tempDataDirs, tempEditsDirs);
      for (Iterator<StorageDirectory> it = 
                   storage.dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        boolean isAccessible = true;
        try { // create directories if don't exist yet
          if(!sd.getRoot().mkdirs()) {
            // do nothing, directory is already created
          }
        } catch(SecurityException se) {
          isAccessible = false;
        }
        if(!isAccessible)
          throw new InconsistentFSStateException(sd.getRoot(),
              "cannot access checkpoint directory.");
        StorageState curState;
        try {
          curState = sd.analyzeStorage(HdfsConstants.StartupOption.REGULAR);
          // sd is locked but not opened
          switch(curState) {
          case NON_EXISTENT:
            // fail if any of the configured checkpoint dirs are inaccessible 
            throw new InconsistentFSStateException(sd.getRoot(),
                  "checkpoint directory does not exist or is not accessible.");
          case NOT_FORMATTED:
            break;  // it's ok since initially there is no current and VERSION
          case NORMAL:
            break;
          default:  // recovery is possible
            sd.doRecover(curState);
          }
        } catch (IOException ioe) {
          sd.unlock();
          throw ioe;
        }
      }
    }

    /**
     * Prepare directories for a new checkpoint.
     * <p>
     * Rename <code>current</code> to <code>lastcheckpoint.tmp</code>
     * and recreate <code>current</code>.
     * @throws IOException
     */
    void startCheckpoint() throws IOException {
      for(StorageDirectory sd : storage.getStorageDirs()) {
        moveCurrent(sd);
      }
    }

    void endCheckpoint() throws IOException {
      for(StorageDirectory sd : storage.getStorageDirs()) {
        moveLastCheckpoint(sd);
      }
    }

    /**
     * Merge image and edits, and verify consistency with the signature.
     */
    private void doMerge(CheckpointSignature sig, boolean loadImage) throws IOException {
      getEditLog().open();
      StorageDirectory sdName = null;
      StorageDirectory sdEdits = null;
      Iterator<StorageDirectory> it = null;
      if (loadImage) {
        it = storage.dirIterator(NameNodeDirType.IMAGE);
        if (it.hasNext())
          sdName = it.next();
        if (sdName == null)
          throw new IOException("Could not locate checkpoint fsimage");
      }
      it = storage.dirIterator(NameNodeDirType.EDITS);
      if (it.hasNext())
        sdEdits = it.next();
      if (sdEdits == null)
        throw new IOException("Could not locate checkpoint edits");
      if (loadImage) {
        loadFSImage(NNStorage.getStorageFile(sdName, NameNodeFile.IMAGE));
      }
      Collection<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>();
      EditLogInputStream is = new EditLogFileInputStream(NNStorage.getStorageFile(sdEdits, NameNodeFile.EDITS));
      editStreams.add(is);
      loadEdits(editStreams);
      sig.validateStorageInfo(this.storage);
      saveNamespace(false);
      sig.mostRecentCheckpointTxId = editLog.getCurrentTxId() - 1;
    }
  }
}
