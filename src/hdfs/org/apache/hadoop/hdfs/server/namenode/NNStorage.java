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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.UpgradeManager;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.ValidateNamespaceDirPolicy.NNStorageLocation;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.util.AtomicFileOutputStream;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;

/**
 * NNStorage is responsible for management of the StorageDirectories used by
 * the NameNode.
 */
public class NNStorage extends Storage implements Closeable, StorageErrorReporter {
  public static final Log LOG = LogFactory.getLog(NNStorage.class.getName());

  public static final String MESSAGE_DIGEST_PROPERTY = "imageMD5Digest";
  public static final String LOCAL_URI_SCHEME = "file";
  
  /**
   * Namenode storage directory, which stores additional information
   * about mount point, if the directory is remote, shared, etc.
   */
  public static enum StorageLocationType {
    LOCAL,
    REMOTE,
    SHARED
  }
  
  public class NNStorageDirectory extends StorageDirectory {

    final StorageLocationType type;

    public NNStorageDirectory(File dir, StorageDirType dirType,
        NNStorageLocation location) {
      super(dir, dirType, true);
      if (location == null) {
        type = null;
        return;
      }
      type = location.type;
    }
  }

  //
  // The filenames used for storing the images
  //
  public enum NameNodeFile {
    IMAGE     ("fsimage"),
    TIME      ("fstime"), // from "old" pre-HDFS-1073 format
    SEEN_TXID ("seen_txid"),
    EDITS     ("edits"),
    IMAGE_NEW ("fsimage.ckpt"),
    EDITS_NEW ("edits.new"), // from "old" pre-HDFS-1073 format
    EDITS_INPROGRESS ("edits_inprogress");

    private String fileName = null;
    private NameNodeFile(String name) { this.fileName = name; }
    public String getName() { return fileName; }
  }
  

  /**
   * Implementation of StorageDirType specific to namenode storage
   * A Storage directory could be of type IMAGE which stores only fsimage,
   * or of type EDITS which stores edits or of type IMAGE_AND_EDITS which
   * stores both fsimage and edits.
   */
  public static enum NameNodeDirType implements StorageDirType {
    UNDEFINED,
    IMAGE,
    EDITS,
    IMAGE_AND_EDITS;
    
    public StorageDirType getStorageDirType() {
      return this;
    }
    
    public boolean isOfType(StorageDirType type) {
      if ((this == IMAGE_AND_EDITS) && (type == IMAGE || type == EDITS))
        return true;
      return this == type;
    }
  }
  
  private UpgradeManager upgradeManager = null;
  
  private Object restorationLock = new Object();
  private boolean disablePreUpgradableLayoutCheck = false;


  /**
   * TxId of the last transaction that was included in the most
   * recent fsimage file. This does not include any transactions
   * that have since been written to the edit log.
   */
  private long mostRecentCheckpointTxId = HdfsConstants.INVALID_TXID;  
  // used for webui
  private long mostRecentCheckpointTime = 0;
  
  private final Map<Long, MD5Hash> checkpointImageDigests = new HashMap<Long,MD5Hash>();

  /**
   * list of failed (and thus removed) storages
   */
  final protected List<StorageDirectory> removedStorageDirs
  = Collections.synchronizedList(new ArrayList<StorageDirectory>());

  /**
   * Properties from old layout versions that may be needed
   * during upgrade only.
   */
  private HashMap<String, String> deprecatedProperties;
  
  private final Configuration conf;
  
  final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();

  /**
   * Construct the NNStorage.
   * @param conf Namenode configuration.
   * @param imageDirs Directories the image can be stored in.
   * @param editsDirs Directories the editlog can be stored in.
   * @throws IOException if any directories are inaccessible.
   */
  public NNStorage(Configuration conf, Collection<URI> imageDirs, 
      Collection<URI> editsDirs, Map<URI, NNStorageLocation> locationMap) 
      throws IOException {
    super(NodeType.NAME_NODE);

    storageDirs = Collections.synchronizedList(new ArrayList<StorageDirectory>());
    
    // this may modify the editsDirs, so copy before passing in
    setStorageDirectories(imageDirs, 
                          new ArrayList<URI>(editsDirs), locationMap);
    this.conf = conf;
  }
  
  public Collection<StorageDirectory> getStorageDirs() {
    return storageDirs;
  }
  
  void checkpointUploadDone(long txid, MD5Hash checkpointImageMd5)
      throws IOException {
    setCheckpointImageDigest(txid, checkpointImageMd5);
  }
  
  /**
   * For testing
   * @param storageInfo
   * @throws IOException
   */
  public NNStorage(StorageInfo storageInfo) throws IOException {
    super(NodeType.NAME_NODE, storageInfo);
    this.conf = new Configuration();
  }

  @Override // Storage
  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    if (disablePreUpgradableLayoutCheck) {
      return false;
    }

    File oldImageDir = new File(sd.getRoot(), "image");
    if (!oldImageDir.exists()) {
      return false;
    }
    // check the layout version inside the image file
    File oldF = new File(oldImageDir, "fsimage");
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    try {
      oldFile.seek(0);
      int oldVersion = oldFile.readInt();
      oldFile.close();
      oldFile = null;
      if (oldVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      IOUtils.cleanup(LOG, oldFile);
    }
    return true;
  }

  @Override // Closeable
  public void close() throws IOException {
    unlockAll();
    storageDirs.clear();
  }

  /**
   * See if any of removed storages is "writable" again, and can be returned
   * into service.
   */
  void attemptRestoreRemovedStorage() {
    // if directory is "alive" - copy the images there...
    if(removedStorageDirs.size() == 0)
      return; //nothing to restore

    /* We don't want more than one thread trying to restore at a time */
    synchronized (this.restorationLock) {
      LOG.info("attemptRestoreRemovedStorage: check removed(failed) "+
               "storage. removedStorages size = " + removedStorageDirs.size());
      for(Iterator<StorageDirectory> it
            = this.removedStorageDirs.iterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        File root = sd.getRoot();
        LOG.info("attemptRestoreRemovedStorage: currently disabled dir "
            + root.getAbsolutePath()
            + "; type="
            + sd.getStorageDirType()
            + ";canwrite=" + root.canWrite());
        try {
          
          if(root.exists() && root.canWrite()) { 
            LOG.info("attemptRestoreRemovedStorage: restoring dir "
                + sd.getRoot().getAbsolutePath());
            this.addStorageDir(sd); // restore
            it.remove();
            sd.lock();
          }
        } catch(IOException e) {
          LOG.warn("attemptRestoreRemovedStorage: failed to restore "
              + sd.getRoot().getAbsolutePath(), e);
        }
      }
    }
  }

  /**
   * @return A list of storage directories which are in the errored state.
   */
  public List<StorageDirectory> getRemovedStorageDirs() {
    return this.removedStorageDirs;
  }
  
  public synchronized void setStorageDirectories(Collection<URI> fsNameDirs,
      Collection<URI> fsEditsDirs) throws IOException {
    setStorageDirectories(fsNameDirs, fsEditsDirs, null);
  }

  /**
   * Set the storage directories which will be used. This should only ever be
   * called from inside NNStorage. However, it needs to remain package private
   * for testing, as StorageDirectories need to be reinitialised after using
   * Mockito.spy() on this class, as Mockito doesn't work well with inner
   * classes, such as StorageDirectory in this case.
   *
   * Synchronized due to initialization of storageDirs and removedStorageDirs.
   *
   * @param fsNameDirs Locations to store images.
   * @param fsEditsDirs Locations to store edit logs.
   * @param locationMap location descriptors
   * @throws IOException
   */
  public synchronized void setStorageDirectories(Collection<URI> fsNameDirs,
                                          Collection<URI> fsEditsDirs,
                                          Map<URI, NNStorageLocation> locationMap)
      throws IOException {
    
    this.storageDirs.clear();
    this.removedStorageDirs.clear();

    for (URI dirName : fsNameDirs) {
      boolean isAlsoEdits = false;
      for (URI editsDirName : fsEditsDirs) {
        if (editsDirName.compareTo(dirName) == 0) {
          isAlsoEdits = true;
          fsEditsDirs.remove(editsDirName);
          break;
        }
      }
      NameNodeDirType dirType = (isAlsoEdits) ?
                          NameNodeDirType.IMAGE_AND_EDITS :
                          NameNodeDirType.IMAGE;
      // Add to the list of storage directories, only if the
      // URI is of type file://
      if (dirName.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) == 0) {
        this.addStorageDir(new NNStorageDirectory(new File(dirName.getPath()),
            dirType, locationMap == null ? null : locationMap.get(dirName)));
      }
    }
    
    // Add edits dirs if they are different from name dirs
    for (URI dirName : fsEditsDirs) {
      checkSchemeConsistency(dirName);
      // Add to the list of storage directories, only if the
      // URI is of type file://
      if (dirName.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) == 0)
        this.addStorageDir(new NNStorageDirectory(new File(dirName.getPath()),
            NameNodeDirType.EDITS, locationMap == null ? null : locationMap.get(dirName)));
    }
  }
 

  /**
   * Return the storage directory corresponding to the passed URI
   * @param uri URI of a storage directory
   * @return The matching storage directory or null if none found
   */
  StorageDirectory getStorageDirectory(URI uri) {
    try {
      uri = Util.fileAsURI(new File(uri));
      Iterator<StorageDirectory> it = dirIterator();
      for (; it.hasNext(); ) {
        StorageDirectory sd = it.next();
        if (Util.fileAsURI(sd.getRoot()).equals(uri)) {
          return sd;
        }
      }
    } catch (IOException ioe) {
      LOG.warn("Error converting file to URI", ioe);
    }
    return null;
  }

  /**
   * Checks the consistency of a URI, in particular if the scheme
   * is specified 
   * @param u URI whose consistency is being checked.
   */
  private static void checkSchemeConsistency(URI u) throws IOException {
    String scheme = u.getScheme();
    // the URI should have a proper scheme
    if(scheme == null) {
      throw new IOException("Undefined scheme for " + u);
    }
  }

  /**
   * Retrieve current directories of type IMAGE
   * @return Collection of URI representing image directories
   * @throws IOException in case of URI processing error
   */
  Collection<File> getImageDirectories() throws IOException {
    return getDirectories(NameNodeDirType.IMAGE);
  }

  /**
   * Retrieve current directories of type EDITS
   * @return Collection of URI representing edits directories
   * @throws IOException in case of URI processing error
   */
  Collection<File> getEditsDirectories() throws IOException {
    return getDirectories(NameNodeDirType.EDITS);
  }

  /**
   * Return number of storage directories of the given type.
   * @param dirType directory type
   * @return number of storage directories of type dirType
   */
  int getNumStorageDirs(NameNodeDirType dirType) {
    if(dirType == null)
      return getNumStorageDirs();
    Iterator<StorageDirectory> it = dirIterator(dirType);
    int numDirs = 0;
    for(; it.hasNext(); it.next())
      numDirs++;
    return numDirs;
  }

  /**
   * Return the list of locations being used for a specific purpose.
   * i.e. Image or edit log storage.
   *
   * @param dirType Purpose of locations requested.
   * @throws IOException
   */
  Collection<File> getDirectories(NameNodeDirType dirType)
      throws IOException {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it = (dirType == null) ? dirIterator() :
                                    dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      StorageDirectory sd = it.next();
      list.add(sd.getRoot());
    }
    return list;
  }
  
  /**
   * Determine the last transaction ID noted in this storage directory.
   * This txid is stored in a special seen_txid file since it might not
   * correspond to the latest image or edit log. For example, an image-only
   * directory will have this txid incremented when edits logs roll, even
   * though the edits logs are in a different directory.
   *
   * @param sd StorageDirectory to check
   * @return If file exists and can be read, last recorded txid. If not, 0L.
   * @throws IOException On errors processing file pointed to by sd
   */
  static long readTransactionIdFile(StorageDirectory sd) throws IOException {
    File txidFile = getStorageFile(sd, NameNodeFile.SEEN_TXID);
    long txid = 0L;
    if (txidFile.exists() && txidFile.canRead()) {
      BufferedReader br = new BufferedReader(new FileReader(txidFile));
      try {
        txid = Long.valueOf(br.readLine());
        br.close();
        br = null;
      } finally {
        IOUtils.cleanup(LOG, br);
      }
    }
    return txid;
  }
  
  /**
   * Write last checkpoint time into a separate file.
   *
   * @param sd
   * @throws IOException
   */
  void writeTransactionIdFile(StorageDirectory sd, long txid) throws IOException {
    if (txid < -1) {
      // -1 is valid when formatting
      throw new IOException("Bad txid: " + txid);
    }
    File txIdFile = getStorageFile(sd, NameNodeFile.SEEN_TXID);
    OutputStream fos = new AtomicFileOutputStream(txIdFile);
    try {
      fos.write(String.valueOf(txid).getBytes());
      fos.write('\n');
      fos.close();
      fos = null;
    } finally {
      IOUtils.cleanup(LOG, fos);
    }
  }

  /**
   * Set the transaction ID of the last checkpoint
   */
  synchronized void setMostRecentCheckpointTxId(long txid) {
    if(txid > mostRecentCheckpointTxId) {
      this.mostRecentCheckpointTxId = txid;
      this.mostRecentCheckpointTime = FSNamesystem.now();
    }
  }

  /**
   * Return the transaction ID of the last checkpoint.
   */
  public long getMostRecentCheckpointTxId() {
    return mostRecentCheckpointTxId;
  }
  
  /**
   * Return the time of last successful checkpoint
   */
  public String getMostRecentCheckpointTime() {
    return new Date(mostRecentCheckpointTime).toString();
  }

  /**
   * Write a small file in all available storage directories that
   * indicates that the namespace has reached some given transaction ID.
   * 
   * This is used when the image is loaded to avoid accidental rollbacks
   * in the case where an edit log is fully deleted but there is no
   * checkpoint. See TestNameEditsConfigs.testNameEditsConfigsFailure()
   * @param txid the txid that has been reached
   */
  public void writeTransactionIdFileToStorage(long txid, FSImage image)
      throws IOException {
    // Write txid marker in all storage directories
    List<StorageDirectory> badSDs = new ArrayList<StorageDirectory>();
    for (StorageDirectory sd : storageDirs) {
      try {
        writeTransactionIdFile(sd, txid);
      } catch(IOException e) {
        // Close any edits stream associated with this dir and remove directory
        LOG.warn("writeTransactionIdToStorage failed on " + sd,
            e);
        badSDs.add(sd);
      }
    }
    reportErrorsOnDirectories(badSDs, image);
    if (image != null) {
      
    }
  }

  /**
   * Return the name of the image file that is uploaded by periodic
   * checkpointing
   *
   * @return List of filenames to save checkpoints to.
   */
  public File[] getFsImageNameCheckpoint(long txid) {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it =
                 dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(getStorageFile(it.next(), NameNodeFile.IMAGE_NEW, txid));
    }
    return list.toArray(new File[list.size()]);
  }

  /**
   * Return the name of the image file, preferring
   * "type" images. Otherwise, return any image.
   * 
   * @return The name of the image file.
   */
  public File getFsImageName(StorageLocationType type, long txid) {
    File lastCandidate = null;
    for (Iterator<StorageDirectory> it =
      dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File fsImage = getStorageFile(sd, NameNodeFile.IMAGE, txid);
      if(sd.getRoot().canRead() && fsImage.exists()) {
        if (isPreferred(type, sd)) {
          return fsImage;
        }
        lastCandidate = fsImage;
      }    
    }
    return lastCandidate;
  }
  
  /**
   * Return all images for given txid, together with their types
   * (local, shared, remote).
   */
  public Map<File, StorageLocationType> getImages(long txid) {
    Map<File, StorageLocationType> map = new HashMap<File, StorageLocationType>();
    for (Iterator<StorageDirectory> it =
      dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File fsImage = getStorageFile(sd, NameNodeFile.IMAGE, txid);
      if(sd.getRoot().canRead() && fsImage.exists()) {
        map.put(fsImage, getType(sd));
      }      
    }
    return map;
  }

  /**
   * Format all available storage directories.
   */
  public void format() throws IOException {
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = newNamespaceID();
    this.cTime = 0L;
    for (Iterator<StorageDirectory> it =
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      format(sd);
    }
  }
  
  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  private void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create current dir
    sd.write();
    writeTransactionIdFile(sd, -1);

    LOG.info("Storage directory " + sd.getRoot()
             + " has been successfully formatted.");
  }

  /**
   * Generate new namespaceID.
   * 
   * namespaceID is a persistent attribute of the namespace.
   * It is generated when the namenode is formatted and remains the same
   * during the life cycle of the namenode.
   * When a datanodes register they receive it as the registrationID,
   * which is checked every time the datanode is communicating with the 
   * namenode. Datanodes that do not 'know' the namespaceID are rejected.
   * 
   * @return new namespaceID
   */
  static int newNamespaceID() {
    Random r = new Random();
    r.setSeed(FSNamesystem.now());
    int newID = 0;
    while(newID == 0)
      newID = r.nextInt(0x7FFFFFFF);  // use 31 bits only
    return newID;
  }


  @Override // Storage
  protected void getFields(Properties props, StorageDirectory sd)
      throws IOException {
    super.getFields(props, sd);
    if (layoutVersion == 0)
      throw new IOException("NameNode directory " + sd.getRoot()
          + " is not formatted.");
    String sDUS, sDUV;
    sDUS = props.getProperty("distributedUpgradeState");
    sDUV = props.getProperty("distributedUpgradeVersion");
    setDistributedUpgradeState(
        sDUS == null ? false : Boolean.parseBoolean(sDUS),
        sDUV == null ? getLayoutVersion() : Integer.parseInt(sDUV));
    setDeprecatedPropertiesForUpgrade(props);
  }
  
  /**
   * Return a property that was stored in an earlier version of HDFS.
   * 
   * This should only be used during upgrades.
   */
  String getDeprecatedProperty(String prop) {
    assert getLayoutVersion() > FSConstants.LAYOUT_VERSION :
      "getDeprecatedProperty should only be done when loading " +
      "storage from past versions during upgrade.";
    return deprecatedProperties.get(prop);
  }

  /**
   * Write version file into the storage directory.
   *
   * The version file should always be written last.
   * Missing or corrupted version file indicates that
   * the checkpoint is not valid.
   *
   * @param sd storage directory
   * @throws IOException
   */
  @Override // Storage  
  protected void setFields(Properties props, StorageDirectory sd)
      throws IOException {
    super.setFields(props, sd);
    boolean uState = getDistributedUpgradeState();
    int uVersion = getDistributedUpgradeVersion();
    if (uState && uVersion != getLayoutVersion()) {
      props.setProperty("distributedUpgradeState", Boolean.toString(uState));
      props
          .setProperty("distributedUpgradeVersion", Integer.toString(uVersion));
    }
  }
  
  /**
   * Pull any properties out of the VERSION file that are from older
   * versions of HDFS and only necessary during upgrade.
   */
  private void setDeprecatedPropertiesForUpgrade(Properties props) {
    deprecatedProperties = new HashMap<String, String>();
    String md5 = props.getProperty(MESSAGE_DIGEST_PROPERTY);
    if (md5 != null) {
      deprecatedProperties.put(MESSAGE_DIGEST_PROPERTY, md5);
    }
  }
  
  ////////////////////////////////////////////////////////////////////////
  // names and files for images checkpoint images, edits, etc
  ////////////////////////////////////////////////////////////////////////
  
  static File getStorageFile(StorageDirectory sd, NameNodeFile type, long imageTxId) {
    return new File(sd.getCurrentDir(),
                    String.format("%s_%019d", type.getName(), imageTxId));
  }
  
  /**
   * Get a storage file for one of the files that doesn't need a txid associated
   * (e.g version, seen_txid)
   */
  static File getStorageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
  }
  
  public static String getCheckpointImageFileName(long txid) {
    return String.format("%s_%019d",
                         NameNodeFile.IMAGE_NEW.getName(), txid);
  }
  
  public static File getCheckpointImageFile(StorageDirectory sd, long txid) {
    return new File(sd.getCurrentDir(),
        getCheckpointImageFileName(txid));
  }

  public static String getImageFileName(long txid) {
    return String.format("%s_%019d",
                         NameNodeFile.IMAGE.getName(), txid);
  }
  
  public static File getImageFile(StorageDirectory sd, long txid) {
    return new File(sd.getCurrentDir(),
        getImageFileName(txid));
  }
  
  public static String getInProgressEditsFileName(long startTxId) {
    return String.format("%s_%019d", NameNodeFile.EDITS_INPROGRESS.getName(),
                         startTxId);
  }
  
  static File getInProgressEditsFile(StorageDirectory sd, long startTxId) {
    return new File(sd.getCurrentDir(), getInProgressEditsFileName(startTxId));
  }
  
  static File getFinalizedEditsFile(StorageDirectory sd,
      long startTxId, long endTxId) {
    return new File(sd.getCurrentDir(),
        getFinalizedEditsFileName(startTxId, endTxId));
  }

  public static String getFinalizedEditsFileName(long startTxId, long endTxId) {
    return String.format("%s_%019d-%019d", NameNodeFile.EDITS.getName(),
                         startTxId, endTxId);
  }
  
  ////////////////////////////////////////////////////////////////////////
  
  /**
   * Return the first readable finalized edits file for the given txid.
   */
  File findFinalizedEditsFile(long startTxId, long endTxId)
  throws IOException {
    File ret = findFile(NameNodeDirType.EDITS,
        getFinalizedEditsFileName(startTxId, endTxId));
    if (ret == null) {
      throw new IOException(
          "No edits file for txid " + startTxId + "-" + endTxId + " exists!");
    }
    return ret;
  }
  
  /**
   * Return the first readable inprogress edits file for the given txid.
   */
  File findInProgressEditsFile(long startTxId)
  throws IOException {
    File ret = findFile(NameNodeDirType.EDITS,
        getInProgressEditsFileName(startTxId));
    if (ret == null) {
      throw new IOException(
          "No edits file for txid " + startTxId + "-in progress");
    }
    return ret;
  }
    
  /**
   * Return the first readable image file for the given txid, or null
   * if no such image can be found
   */
  File findImageFile(long txid) throws IOException {
    return findFile(NameNodeDirType.IMAGE,
        getImageFileName(txid));
  }

  /**
   * Return the first readable storage file of the given name
   * across any of the 'current' directories in SDs of the
   * given type, or null if no such file exists.
   */
  private File findFile(NameNodeDirType dirType, String name) {
    for (StorageDirectory sd : dirIterable(dirType)) {
      File candidate = new File(sd.getCurrentDir(), name);
      if (sd.getCurrentDir().canRead() &&
          candidate.exists()) {
        return candidate;
      }
    }
    return null;
  }
  
  /**
   * Checks if we have information about this directory
   * that it is preferred.
   * @param type preferred type
   * @param sd storage directory
   */
  static boolean isPreferred(StorageLocationType type, StorageDirectory sd) {
    if ((sd instanceof NNStorageDirectory)) {
      return ((NNStorageDirectory) sd).type == type;
    } 
    // by default all are preferred
    return true;
  }
  
  /**
   * Get the type of given directory.
   */
  static StorageLocationType getType(StorageDirectory sd) {
    if ((sd instanceof NNStorageDirectory)) {
      return ((NNStorageDirectory) sd).type;
    } 
    // by default all are local
    return StorageLocationType.LOCAL;
  }

  /**
   * @return A list of the given File in every available storage directory,
   * regardless of whether it might exist.
   */
  File[] getFiles(NameNodeDirType dirType, String fileName) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it =
      (dirType == null) ? dirIterator() : dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      list.add(new File(it.next().getCurrentDir(), fileName));
    }
    return list.toArray(new File[list.size()]);
  }

  /**
   * Set the upgrade manager for use in a distributed upgrade.
   * @param um The upgrade manager
   */
  void setUpgradeManager(UpgradeManager um) {
    upgradeManager = um;
  }

  /**
   * @return The current distribued upgrade state.
   */
  boolean getDistributedUpgradeState() {
    return upgradeManager == null ? false : upgradeManager.getUpgradeState();
  }

  /**
   * @return The current upgrade version.
   */
  int getDistributedUpgradeVersion() {
    return upgradeManager == null ? 0 : upgradeManager.getUpgradeVersion();
  }

  /**
   * Set the upgrade state and version.
   * @param uState the new state.
   * @param uVersion the new version.
   */
  private void setDistributedUpgradeState(boolean uState, int uVersion) {
    if (upgradeManager != null) {
      upgradeManager.setUpgradeState(uState, uVersion);
    }
  }

  /**
   * Verify that the distributed upgrade state is valid.
   * @param startOpt the option the namenode was started with.
   */
  void verifyDistributedUpgradeProgress(StartupOption startOpt
                                        ) throws IOException {
    if(startOpt == StartupOption.ROLLBACK || startOpt == StartupOption.IMPORT)
      return;

    assert upgradeManager != null : "FSNameSystem.upgradeManager is null.";
    if(startOpt != StartupOption.UPGRADE) {
      if(upgradeManager.getUpgradeState())
        throw new IOException(
                    "\n   Previous distributed upgrade was not completed. "
                  + "\n   Please restart NameNode with -upgrade option.");
      if(upgradeManager.getDistributedUpgrades() != null)
        throw new IOException("\n   Distributed upgrade for NameNode version "
                              + upgradeManager.getUpgradeVersion()
                              + " to current LV " + layoutVersion
                              + " is required.\n   Please restart NameNode"
                              + " with -upgrade option.");
    }
  }

  /**
   * Initialize a distributed upgrade.
   */
  void initializeDistributedUpgrade() throws IOException {
    if(! upgradeManager.initializeUpgrade())
      return;
    // write new upgrade state into disk
    writeAll();
    LOG.info("\n   Distributed upgrade for NameNode version "
             + upgradeManager.getUpgradeVersion() + " to current LV "
             + layoutVersion + " is initialized.");
  }

  /**
   * Disable the check for pre-upgradable layouts. Needed for BackupImage.
   * @param val Whether to disable the preupgradeable layout check.
   */
  void setDisablePreUpgradableLayoutCheck(boolean val) {
    disablePreUpgradableLayoutCheck = val;
  }

  /**
   * Marks a list of directories as having experienced an error.
   *
   * @param sds A list of storage directories to mark as errored.
   * @throws IOException
   */
  synchronized void reportErrorsOnDirectories(List<StorageDirectory> sds,
      FSImage image) throws IOException {
    for (StorageDirectory sd : sds) {
      reportErrorsOnDirectory(sd, image);
    }
    
    // check image managers (this will update image metrics)
    if (image != null) {
      image.checkImageManagers();
    }
    
    // only check if something was wrong
    if(!sds.isEmpty()) {
      if (this.getNumStorageDirs() == 0)  
        throw new IOException("No more storage directories left");
      
      // check image directories, edits are checked withing FSEditLog.checkJournals
      if (getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
        throw new IOException("No more image storage directories left");
    }
  }

  /**
   * Reports that a directory has experienced an error.
   * Notifies listeners that the directory is no longer
   * available.
   *
   * @param sd A storage directory to mark as errored.
   * @throws IOException
   */
  synchronized void reportErrorsOnDirectory(StorageDirectory sd, 
      FSImage image) {
    String lsd = listStorageDirectories();
    LOG.info("reportErrorsOnDirectory: Current list of storage dirs:" + lsd);

    LOG.error("reportErrorsOnDirectory: Error reported on storage directory "
        + sd.getRoot());

    if (this.storageDirs.remove(sd)) {
      try {
        sd.unlock();
      } catch (Exception e) {
        LOG.warn(
            "reportErrorsOnDirectory: Unable to unlock bad storage directory: "
                + sd.getRoot().getPath(), e);
      }
      this.removedStorageDirs.add(sd);
    }
    if (image != null) {
      image.reportErrorsOnImageManager(sd);
    }
    
    lsd = listStorageDirectories();
    LOG.info("reportErrorsOnDirectory: Current list of storage dirs:" + lsd);
  }
  
  /**
   * Report that an IOE has occurred on some file which may
   * or may not be within one of the NN image storage directories.
   */
  public void reportErrorOnFile(File f) {
    // We use getAbsolutePath here instead of getCanonicalPath since we know
    // that there is some IO problem on that drive.
    // getCanonicalPath may need to call stat() or readlink() and it's likely
    // those calls would fail due to the same underlying IO problem.
    String absPath = f.getAbsolutePath();
    for (StorageDirectory sd : storageDirs) {
      String dirPath = sd.getRoot().getAbsolutePath();
      if (!dirPath.endsWith("/")) {
        dirPath += "/";
      }
      if (absPath.startsWith(dirPath)) {
        reportErrorsOnDirectory(sd, null);
        return;
      }
    }
    
  }

  /**
   * Iterate over all current storage directories, inspecting them
   * with the given inspector.
   */
  void inspectStorageDirs(FSImageStorageInspector inspector)
      throws IOException {

    // Process each of the storage directories to find the pair of
    // newest image file and edit file
    for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      inspector.inspectDirectory(sd);
    }
  }

  /**
   * Iterate over all of the storage dirs, reading their contents to determine
   * their layout versions. Returns an FSImageStorageInspector which has
   * inspected each directory.
   * 
   * <b>Note:</b> this can mutate the storage info fields (ctime, version, etc).
   * @throws IOException if no valid storage dirs are found
   */
  FSImageStorageInspector readAndInspectDirs()
      throws IOException {
    int minLayoutVersion = Integer.MAX_VALUE; // the newest
    int maxLayoutVersion = Integer.MIN_VALUE; // the oldest
    
    // First determine what range of layout versions we're going to inspect
    for (Iterator<StorageDirectory> it = dirIterator();
         it.hasNext();) {
      StorageDirectory sd = it.next();
      if (!sd.getVersionFile().exists()) {
        FSImage.LOG.warn("Storage directory " + sd + " contains no VERSION file. Skipping...");
        continue;
      }
      sd.read(); // sets layoutVersion
      minLayoutVersion = Math.min(minLayoutVersion, getLayoutVersion());
      maxLayoutVersion = Math.max(maxLayoutVersion, getLayoutVersion());
    }
    
    if (minLayoutVersion > maxLayoutVersion) {
      throw new IOException("No storage directories contained VERSION information");
    }
    assert minLayoutVersion <= maxLayoutVersion;
    
    // If we have any storage directories with the new layout version
    // (ie edits_<txnid>) then use the new inspector, which will ignore
    // the old format dirs.
    FSImageStorageInspector inspector;
    if (LayoutVersion.supports(Feature.TXID_BASED_LAYOUT, minLayoutVersion)) {
      inspector = new FSImageTransactionalStorageInspector();
      if (!LayoutVersion.supports(Feature.TXID_BASED_LAYOUT, maxLayoutVersion)) {
        FSImage.LOG.warn("Ignoring one or more storage directories with old layouts");
      }
    } else {
      inspector = new FSImagePreTransactionalStorageInspector(conf);
    }
    
    inspectStorageDirs(inspector);
    return inspector;
  }

  @Override
  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    File oldImageDir = new File(rootDir, "image");
    if (!oldImageDir.exists())
      if (!oldImageDir.mkdir())
        throw new IOException("Cannot create directory " + oldImageDir);
    File oldImage = new File(oldImageDir, "fsimage");
    if (!oldImage.exists())
      // recreate old image file to let pre-upgrade versions fail
      if (!oldImage.createNewFile())
        throw new IOException("Cannot create file " + oldImage);
    RandomAccessFile oldFile = new RandomAccessFile(oldImage, "rws");
    // write new version into old image file
    try {
      writeCorruptedData(oldFile);
    } finally {
      oldFile.close();
    }
  }
  
  synchronized void setCheckpointImageDigest(long txid, MD5Hash imageDigest) 
      throws IOException{
    if(checkpointImageDigests.containsKey(txid)) {
      MD5Hash existing = checkpointImageDigests.get(txid);
      if (!existing.equals(imageDigest)) {
        throw new IOException(
            "Trying to set checkpoint image digest for txid: " + txid + "="
                + imageDigest + " existing " + existing);
      }
    } else {
      checkpointImageDigests.put(txid, imageDigest);
    }
  }
  
  synchronized void clearCheckpointImageDigest(long txid) 
      throws IOException{
    checkpointImageDigests.remove(txid);
  }
  
  synchronized MD5Hash getCheckpointImageDigest(long txid) throws IOException {
    if (checkpointImageDigests.containsKey(txid)) {
      return checkpointImageDigests.get(txid);
    }
    throw new IOException("Trying to get checkpoint image digest for txid: "
        + txid + " but it's not stored");
  }

  synchronized void purgeOldStorage(long minImageTxId) {
    // clear image digests
    for (Iterator<Map.Entry<Long, MD5Hash>> it = checkpointImageDigests
        .entrySet().iterator(); it.hasNext();) {
      Map.Entry<Long, MD5Hash> entry = it.next();
      if (entry.getKey() < minImageTxId) {
        it.remove();
      }
    }
  }
  
  public static boolean recoverDirectory(StorageDirectory sd,
      StartupOption startOpt, StorageState curState, boolean checkImport)
      throws IOException {
    boolean isFormatted = false;
    // sd is locked but not opened
    switch (curState) {
    case NON_EXISTENT:
      // name-node fails if any of the configured storage dirs are missing
      throw new InconsistentFSStateException(sd.getRoot(),
          "storage directory does not exist or is not accessible.");
    case NOT_FORMATTED:
      break;
    case NORMAL:
      break;
    default: // recovery is possible
      sd.doRecover(curState);
    }
    if (curState != StorageState.NOT_FORMATTED
        && startOpt != StartupOption.ROLLBACK) {
      // read and verify consistency with other directories
      sd.read();
      isFormatted = true;
    }
    if (checkImport && startOpt == StartupOption.IMPORT && isFormatted)
      // import of a checkpoint is allowed only into empty image directories
      throw new IOException("Cannot import image from a checkpoint. "
          + " NameNode already contains an image in " + sd.getRoot());
    return isFormatted;
  }

  public static void finalize(StorageDirectory sd, int layoutVersion,
      long cTime) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) { // already discarded
      LOG.info("Directory " + prevDir + " does not exist.");
      LOG.info("Finalize upgrade for " + sd.getRoot() + " is not required.");
      return;
    }
    LOG.info("Finalizing upgrade for storage directory "
        + sd.getRoot()
        + "."
        + (layoutVersion == 0 ? "" : "\n   cur LV = " + layoutVersion
            + "; cur CTime = " + cTime));
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    NNStorage.rename(prevDir, tmpDir);
    NNStorage.deleteDir(tmpDir);
    LOG.info("Finalize upgrade for " + sd.getRoot() + " is complete.");
  }

  public static boolean canRollBack(StorageDirectory sd, Storage storage)
      throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) { // use current directory then
      LOG.info("Storage directory " + sd.getRoot()
          + " does not contain previous fs state.");
      // read and verify consistency with other directories
      sd.read();
      return false;
    }

    // read and verify consistency of the prev dir
    sd.read(sd.getPreviousVersionFile());

    if (storage.getLayoutVersion() != FSConstants.LAYOUT_VERSION) {
      throw new IOException("Cannot rollback to storage version "
          + storage.getLayoutVersion()
          + " using this version of the NameNode, which uses storage version "
          + FSConstants.LAYOUT_VERSION + ". "
          + "Please use the previous version of HDFS to perform the rollback.");
    }
    return true;
  }

  public static void doRollBack(StorageDirectory sd, Storage storage)
      throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists())
      return;

    LOG.info("Rolling back storage directory " + sd.getRoot()
        + ".\n   new LV = " + storage.getLayoutVersion() + "; new CTime = "
        + storage.getCTime());
    File tmpDir = sd.getRemovedTmp();
    assert !tmpDir.exists() : "removed.tmp directory must not exist.";
    // rename current to tmp
    File curDir = sd.getCurrentDir();
    assert curDir.exists() : "Current directory must exist.";
    NNStorage.rename(curDir, tmpDir);
    // rename previous to current
    NNStorage.rename(prevDir, curDir);

    // delete tmp dir
    NNStorage.deleteDir(tmpDir);
    LOG.info("Rollback of " + sd.getRoot() + " is complete.");
  }

  Configuration getConf() {
    return conf;
  }
}
