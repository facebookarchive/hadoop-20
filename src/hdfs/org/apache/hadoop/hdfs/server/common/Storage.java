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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;

import com.google.common.base.Preconditions;



/**
 * Storage information file.
 * <p>
 * Local storage information is stored in a separate file VERSION.
 * It contains type of the node, 
 * the storage layout version, the namespace id, and 
 * the fs state creation time.
 * <p>
 * Local storage can reside in multiple directories. 
 * Each directory should contain the same VERSION file as the others.
 * During startup Hadoop servers (name-node and data-nodes) read their local 
 * storage information from them.
 * <p>
 * The servers hold a lock for each storage directory while they run so that 
 * other nodes were not able to startup sharing the same storage.
 * The locks are released when the servers stop (normally or abnormally).
 * 
 */
public abstract class Storage extends StorageInfo {
  public static final Log LOG = LogFactory.getLog(Storage.class.getName());

  // Constants
  
  // last layout version that did not suppot upgrades
  public static final int LAST_PRE_UPGRADE_LAYOUT_VERSION = -3;
  
  // this corresponds to Hadoop-0.14.
  public static final int LAST_UPGRADABLE_LAYOUT_VERSION = -7;
  protected static final String LAST_UPGRADABLE_HADOOP_VERSION = "Hadoop-0.14";

  /* this should be removed when LAST_UPGRADABLE_LV goes beyond -13.
   * any upgrade code that uses this constant should also be removed. */
  public static final int PRE_GENERATIONSTAMP_LAYOUT_VERSION = -13;

  private   static final String STORAGE_FILE_LOCK     = "in_use.lock";
  protected static final String STORAGE_FILE_VERSION  = "VERSION";
  public static final String STORAGE_DIR_CURRENT   = "current";
  public final static String STORAGE_DIR_RBW = "rbw";
  public final static String OLD_STORAGE_DIR_RBW = "blocksBeingWritten";
  public final static String STORAGE_DIR_FINALIZED = "finalized";
  protected static final String STORAGE_DIR_PREVIOUS  = "previous";
  private   static final String STORAGE_TMP_REMOVED   = "removed.tmp";
  private   static final String STORAGE_TMP_PREVIOUS  = "previous.tmp";
  private   static final String STORAGE_TMP_FINALIZED = "finalized.tmp";
  private   static final String STORAGE_TMP_LAST_CKPT = "lastcheckpoint.tmp";
  private   static final String STORAGE_PREVIOUS_CKPT = "previous.checkpoint";
  public   static final String STORAGE_BLOCK_CRC = "blockcrc";
  public   static final String STORAGE_TMP_BLOCK_CRC = "blockcrc.tmp";
  
  // meta info property names
  protected static final String STORAGE_TYPE = "storageType";
  public static final String NAMESPACE_ID = "namespaceID";
  public static final String LAYOUT_VERSION = "layoutVersion";
  public static final String CHECK_TIME = "cTime";
  
  public enum StorageState {
    NON_EXISTENT,
    NOT_FORMATTED,
    COMPLETE_UPGRADE,
    UPGRADE_DONE,
    RECOVER_UPGRADE,
    COMPLETE_FINALIZE,
    COMPLETE_ROLLBACK,
    RECOVER_ROLLBACK,
    COMPLETE_CHECKPOINT,
    RECOVER_CHECKPOINT,
    NORMAL,
    INCONSISTENT;
  }

  /**
   * An interface to denote storage directory type
   * Implementations can define a type for storage directory by implementing
   * this interface.
   */
  public interface StorageDirType {
    public StorageDirType getStorageDirType();
    public boolean isOfType(StorageDirType type);
  }
  
  protected NodeType storageType;    // Type of the node using this storage 
  protected List<StorageDirectory> storageDirs = new ArrayList<StorageDirectory>();
  
  private class DirIterator implements Iterator<StorageDirectory> {
    StorageDirType dirType;
    int prevIndex; // for remove()
    int nextIndex; // for next()
    
    DirIterator(StorageDirType dirType) {
      this.dirType = dirType;
      this.nextIndex = 0;
      this.prevIndex = 0;
    }
    
    public boolean hasNext() {
      if (storageDirs.isEmpty() || nextIndex >= storageDirs.size())
        return false;
      if (dirType != null) {
        while (nextIndex < storageDirs.size()) {
          if (getStorageDir(nextIndex).getStorageDirType().isOfType(dirType))
            break;
          nextIndex++;
        }
        if (nextIndex >= storageDirs.size())
         return false;
      }
      return true;
    }

    public StorageDirectory next() {
      StorageDirectory sd = getStorageDir(nextIndex);
      prevIndex = nextIndex;
      nextIndex++;
      if (dirType != null) {
        while (nextIndex < storageDirs.size()) {
          if (getStorageDir(nextIndex).getStorageDirType().isOfType(dirType))
            break;
          nextIndex++;
        }
      }
      return sd;
    }

    public void remove() {
      nextIndex = prevIndex; // restore previous state
      storageDirs.remove(prevIndex); // remove last returned element
      hasNext(); // reset nextIndex to correct place
    }
  }

  /**
   * Return default iterator
   * This iterator returns all entires of storageDirs
   */
  public Iterator<StorageDirectory> dirIterator() {
    return dirIterator(null);
  }

  /**
   * Return iterator based on Storage Directory Type
   * This iterator selects entires of storageDirs of type dirType and returns
   * them via the Iterator
   */
  public Iterator<StorageDirectory> dirIterator(StorageDirType dirType) {
    return new DirIterator(dirType);
  }

  /**
   * generate storage list (debug line)
   */
  public String listStorageDirectories() {
    StringBuffer buf = new StringBuffer();
    for (StorageDirectory sd : storageDirs) {
      buf.append(sd.getRoot() + "(" + sd.getStorageDirType() + ");");
    }
    return buf.toString();
  }
  
  public static Properties getProps(File from) throws IOException {
    RandomAccessFile file = new RandomAccessFile(from, "rws");
    FileInputStream in = null;
    try {
      in = new FileInputStream(file.getFD());
      file.seek(0);
      Properties props = new Properties();
      props.load(in);
      return props;
    } finally {
      if (in != null) {
        in.close();
      }
      file.close();
    }
  }

  public static void writeProps(File to, Properties props) throws IOException {
    RandomAccessFile file = new RandomAccessFile(to, "rws");
    FileOutputStream out = null;
    try {
      file.seek(0);
      out = new FileOutputStream(file.getFD());
      /*
       * If server is interrupted before this line, the version file will remain
       * unchanged.
       */
      props.store(out, null);
      /*
       * Now the new fields are flushed to the head of the file, but file length
       * can still be larger then required and therefore the file can contain
       * whole or corrupted fields from its old contents in the end. If server
       * is interrupted here and restarted later these extra fields either
       * should not effect server behavior or should be handled by the server
       * correctly.
       */
      file.setLength(out.getChannel().position());
    } finally {
      if (out != null) {
        out.close();
      }
      file.close();
    }
  }


  /**
   * One of the storage directories.
   */
  public class StorageDirectory implements FormatConfirmable {
    File              root; // root directory
    final boolean useLock;  // flag to enable storage lock
    FileLock          lock; // storage lock
    StorageDirType dirType; // storage dir type
    
    public StorageDirectory(File dir) {
      // default dirType is null
      this(dir, null);
    }
    
    public StorageDirectory(File dir, StorageDirType dirType) {
      this(dir, dirType, true);
    }
    
    public StorageDirectory(File dir, StorageDirType dirType, boolean useLock) {
      this.root = dir;
      this.lock = null;
      this.dirType = dirType;
      this.useLock = useLock;
    }
    
    @Override
    public String toString() {
      return "Storage Directory " + this.root;
    }
    
    /**
     * Get root directory of this storage
     */
    public File getRoot() {
      return root;
    }

    /**
     * Get storage directory type
     */
    public StorageDirType getStorageDirType() {
      return dirType;
    }

    /**
     * Read version file.
     *
     * @throws IOException if file cannot be read or contains inconsistent data
     */
    public void read() throws IOException {
      read(getVersionFile());
    }

    public void read(File from) throws IOException {
      Properties props = getProps(from);
      getFields(props, this);
    }

    /**
     * Write version file.
     *
     * @throws IOException
     */
    public void write() throws IOException {
      corruptPreUpgradeStorage(root);
      write(getVersionFile());
    }

    public void write(File to) throws IOException {
      Properties props = new Properties();
      setFields(props, this);
      writeProps(to, props);
    }

    /**
     * Clear and re-create storage directory.
     * <p>
     * Removes contents of the current directory and creates an empty directory.
     * Removes other contents under the root directory 
     *
     * This does not fully format storage directory.
     * It cannot write the version file since it should be written last after
     * all other storage type dependent files are written.
     * Derived storage is responsible for setting specific storage values and
     * writing the version file to disk.
     *
     * @throws IOException
     */
    public void clearDirectory() throws IOException {
      File rootDir = this.getRootDir();
      if (rootDir.exists()) {
        File contents[] = rootDir.listFiles();
        for (int i = 0; i < contents.length; i++) {
          if (contents[i].getName().equals(STORAGE_FILE_LOCK)) {
            continue;
          }
          if (!(FileUtil.fullyDelete(contents[i])))
            throw new IOException("Cannot remove content: " + contents[i]);
        }
      }
      File curDir = this.getCurrentDir();
      if (!curDir.mkdirs())
        throw new IOException("Cannot create current directory " + curDir);
    }

    public boolean isEmpty() throws IOException {
      File rootDir = this.getRootDir();
      if (!rootDir.exists()) {
        throw new IOException("Directory " + rootDir + " does not exist!");
      }
      String contents[] = rootDir.list();
      if (contents == null) {
        throw new IOException("Unable to list files in " + rootDir);
      }
      if (contents.length == 1 && this.useLock) { // Ignore lock file
        return contents[0].equals(STORAGE_FILE_LOCK);
      }
      return contents.length == 0;
    }
    
    /**
     * @return true if the storage directory should prompt the user prior
     * to formatting (i.e if the directory appears to contain some data)
     * @throws IOException if the SD cannot be accessed due to an IO error
     */
    private boolean hasSomeData() throws IOException {
      // Its alright for a dir not to exist, or to exist (properly accessible)
      // and be completely empty.
      if (!root.exists()) return false;
      
      if (!root.isDirectory()) {
        LOG.info("Root is not a directory: " + this);
        // a file where you expect a directory should not cause silent
        // formatting
        return true;
      }
      
      if (FileUtil.listFiles(root).length == 0) {
        // Empty dir can format without prompt.
        return false;
      }      
      return true;
    }

    public File getRootDir() {
      return root;
    }

    public File getCurrentDir() {
      return new File(root, STORAGE_DIR_CURRENT);
    }

    public File getVersionFile() {
      return new File(new File(root, STORAGE_DIR_CURRENT), STORAGE_FILE_VERSION);
    }
    public File getPreviousVersionFile() {
      return new File(new File(root, STORAGE_DIR_PREVIOUS), STORAGE_FILE_VERSION);
    }
    public File getPreviousDir() {
      return new File(root, STORAGE_DIR_PREVIOUS);
    }
    public File getPreviousTmp() {
      return new File(root, STORAGE_TMP_PREVIOUS);
    }
    public File getRemovedTmp() {
      return new File(root, STORAGE_TMP_REMOVED);
    }
    public File getFinalizedTmp() {
      return new File(root, STORAGE_TMP_FINALIZED);
    }
    public File getLastCheckpointTmp() {
      return new File(root, STORAGE_TMP_LAST_CKPT);
    }
    public File getPreviousCheckpoint() {
      return new File(root, STORAGE_PREVIOUS_CKPT);
    }

    /**
     * Check consistency of the storage directory
     *
     * @param startOpt a startup option.
     *
     * @return state {@link StorageState} of the storage directory
     * @throws {@link InconsistentFSStateException} if directory state is not
     * consistent and cannot be recovered
     */
    public StorageState analyzeStorage(StartupOption startOpt) throws IOException {
      assert root != null : "root is null";
      String rootPath = root.getCanonicalPath();
      try { // check that storage exists
        if (!root.exists()) {
          // storage directory does not exist
          if (startOpt != StartupOption.FORMAT) {
            LOG.info("Storage directory " + rootPath + " does not exist.");
            return StorageState.NON_EXISTENT;
          }
          LOG.info(rootPath + " does not exist. Creating ...");
          if (!root.mkdirs())
            throw new IOException("Cannot create directory " + rootPath);
        }
        // or is inaccessible
        if (!root.isDirectory()) {
          LOG.info(rootPath + "is not a directory.");
          return StorageState.NON_EXISTENT;
        }
        if (!root.canWrite()) {
          LOG.info("Cannot access storage directory " + rootPath);
          return StorageState.NON_EXISTENT;
        }
      } catch(SecurityException ex) {
        LOG.info("Cannot access storage directory " + rootPath, ex);
        return StorageState.NON_EXISTENT;
      }

      this.lock(); // lock storage if it exists

      if (startOpt == HdfsConstants.StartupOption.FORMAT)
        return StorageState.NOT_FORMATTED;
      if (startOpt != HdfsConstants.StartupOption.IMPORT) {
        //make sure no conversion is required
        checkConversionNeeded(this);
      }

      // check whether current directory is valid
      File versionFile = getVersionFile();
      boolean hasCurrent = versionFile.exists();

      // check which directories exist
      boolean hasPrevious = getPreviousDir().exists();
      boolean hasPreviousTmp = getPreviousTmp().exists();
      boolean hasRemovedTmp = getRemovedTmp().exists();
      boolean hasFinalizedTmp = getFinalizedTmp().exists();
      boolean hasCheckpointTmp = getLastCheckpointTmp().exists();

      if (!(hasPreviousTmp || hasRemovedTmp
          || hasFinalizedTmp || hasCheckpointTmp)) {
        // no temp dirs - no recovery
        if (hasCurrent)
          return StorageState.NORMAL;
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                              "version file in current directory is missing.");
        return StorageState.NOT_FORMATTED;
      }

      if ((hasPreviousTmp?1:0) + (hasRemovedTmp?1:0)
          + (hasFinalizedTmp?1:0) + (hasCheckpointTmp?1:0) > 1)
        // more than one temp dirs
        throw new InconsistentFSStateException(root,
                                               "too many temporary directories.");

      // # of temp dirs == 1 should either recover or complete a transition
      if (hasCheckpointTmp) {
        return hasCurrent ? StorageState.COMPLETE_CHECKPOINT
                          : StorageState.RECOVER_CHECKPOINT;
      }

      if (hasFinalizedTmp) {
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_FINALIZED
                                                 + "cannot exist together.");
        return StorageState.COMPLETE_FINALIZE;
      }

      if (hasPreviousTmp) {
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_PREVIOUS
                                                 + " cannot exist together.");
        if (hasCurrent)
          return StorageState.COMPLETE_UPGRADE;
        return StorageState.RECOVER_UPGRADE;
      }

      if (hasPrevious && hasCurrent) {
        return StorageState.UPGRADE_DONE;
      }

      assert hasRemovedTmp : "hasRemovedTmp must be true";
      if (!(hasCurrent ^ hasPrevious))
        throw new InconsistentFSStateException(root,
                                               "one and only one directory " + STORAGE_DIR_CURRENT
                                               + " or " + STORAGE_DIR_PREVIOUS
                                               + " must be present when " + STORAGE_TMP_REMOVED
                                               + " exists.");
      if (hasCurrent)
        return StorageState.COMPLETE_ROLLBACK;
      return StorageState.RECOVER_ROLLBACK;
    }

    /**
     * Complete or recover storage state from previously failed transition.
     *
     * @param curState specifies what/how the state should be recovered
     * @throws IOException
     */
    public void doRecover(StorageState curState) throws IOException {
      File curDir = getCurrentDir();
      String rootPath = root.getCanonicalPath();
      switch(curState) {
      case COMPLETE_UPGRADE:  // mv previous.tmp -> previous
        LOG.info("Completing previous upgrade for storage directory "
                 + rootPath + ".");
        rename(getPreviousTmp(), getPreviousDir());
        return;
      case RECOVER_UPGRADE:   // mv previous.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from previous upgrade.");
        if (curDir.exists())
          deleteDir(curDir);
        rename(getPreviousTmp(), curDir);
        return;
      case COMPLETE_ROLLBACK: // rm removed.tmp
        LOG.info("Completing previous rollback for storage directory "
                 + rootPath + ".");
        deleteDir(getRemovedTmp());
        return;
      case RECOVER_ROLLBACK:  // mv removed.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from previous rollback.");
        rename(getRemovedTmp(), curDir);
        return;
      case COMPLETE_FINALIZE: // rm finalized.tmp
        LOG.info("Completing previous finalize for storage directory "
                 + rootPath + ".");
        deleteDir(getFinalizedTmp());
        return;
      case COMPLETE_CHECKPOINT: // mv lastcheckpoint.tmp -> previous.checkpoint
        LOG.info("Completing previous checkpoint for storage directory "
                 + rootPath + ".");
        File prevCkptDir = getPreviousCheckpoint();
        if (prevCkptDir.exists())
          deleteDir(prevCkptDir);
        rename(getLastCheckpointTmp(), prevCkptDir);
        return;
      case RECOVER_CHECKPOINT:  // mv lastcheckpoint.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from failed checkpoint.");
        if (curDir.exists())
          deleteDir(curDir);
        rename(getLastCheckpointTmp(), curDir);
        return;
      default:
        throw new IOException("Unexpected FS state: " + curState);
      }
    }

    /**
     * Lock storage to provide exclusive access.
     * 
     * <p> Locking is not supported by all file systems.
     * E.g., NFS does not consistently support exclusive locks.
     * 
     * <p> If locking is supported we guarantee exculsive access to the
     * storage directory. Otherwise, no guarantee is given.
     * 
     * @throws IOException if locking fails
     */
    public void lock() throws IOException {
      if (!useLock) {                                                                          
        LOG.info("Locking is disabled");
        return;
      }   

      this.lock = tryLock();
      if (lock == null) {
        String msg = "Cannot lock storage " + this.root 
          + ". The directory is already locked.";
        LOG.info(msg);
        throw new IOException(msg);
      }
    }

    /**
     * Attempts to acquire an exclusive lock on the storage.
     * 
     * @return A lock object representing the newly-acquired lock or
     * <code>null</code> if storage is already locked.
     * @throws IOException if locking fails.
     */
    FileLock tryLock() throws IOException {
      File lockF = new File(root, STORAGE_FILE_LOCK);
      lockF.deleteOnExit();
      RandomAccessFile file = new RandomAccessFile(lockF, "rws");
      FileLock res = null;
      try {
        res = file.getChannel().tryLock();
      } catch(OverlappingFileLockException oe) {
        file.close();
        return null;
      } catch(IOException e) {
        LOG.error("Cannot create lock on " + lockF, e);
        file.close();
        throw e;
      }
      return res;
    }

    /**
     * Unlock storage.
     *
     * @throws IOException
     */
    public void unlock() throws IOException {
      if (this.lock == null)
        return;
      this.lock.release();
      lock.channel().close();
      lock = null;
    }

    @Override
    public boolean hasSomeJournalData() throws IOException {
      return hasSomeData();
    }

    @Override
    public boolean hasSomeImageData() throws IOException {
      return hasSomeData();
    }
  }

  /**
   * Create empty storage info of the specified type
   */
  protected Storage(NodeType type) {
    super();
    this.storageType = type;
  }

  protected Storage(NodeType type, int nsID, long cT) {
    super(FSConstants.LAYOUT_VERSION, nsID, cT);
    this.storageType = type;
  }

  protected Storage(NodeType type, StorageInfo storageInfo) {
    super(storageInfo);
    this.storageType = type;
  }

  public int getNumStorageDirs() {
    return storageDirs.size();
  }

  public StorageDirectory getStorageDir(int idx) {
    return storageDirs.get(idx);
  }

  protected void addStorageDir(StorageDirectory sd) {
    storageDirs.add(sd);
  }

  public abstract boolean isConversionNeeded(StorageDirectory sd) throws IOException;

  /*
   * Coversion is no longer supported. So this should throw exception if
   * conversion is needed.
   */
  private void checkConversionNeeded(StorageDirectory sd) throws IOException {
    if (isConversionNeeded(sd)) {
      //throw an exception
      checkVersionUpgradable(0);
    }
  }

  /**
   * Checks if the upgrade from the given old version is supported. If
   * no upgrade is supported, it throws IncorrectVersionException.
   * 
   * @param oldVersion
   */
  public static void checkVersionUpgradable(int oldVersion) 
                                     throws IOException {
    if (oldVersion > LAST_UPGRADABLE_LAYOUT_VERSION) {
      String msg = "*********** Upgrade is not supported from this older" +
                   " version of storage to the current version." + 
                   " Please upgrade to " + LAST_UPGRADABLE_HADOOP_VERSION +
                   " or a later version and then upgrade to current" +
                   " version. Old layout version is " + 
                   (oldVersion == 0 ? "'too old'" : (""+oldVersion)) +
                   " and latest layout version this software version can" +
                   " upgrade from is " + LAST_UPGRADABLE_LAYOUT_VERSION +
                   ". ************";
      LOG.error(msg);
      throw new IOException(msg); 
    }
    
  }
  
  /**
   * Get common storage fields.
   * Should be overloaded if additional fields need to be get.
   * 
   * @param props
   * @throws IOException
   */
  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    String sv, st, sid, sct;
    sv = props.getProperty(LAYOUT_VERSION);
    st = props.getProperty(STORAGE_TYPE);
    sid = props.getProperty(NAMESPACE_ID);
    sct = props.getProperty(CHECK_TIME);
    if (sv == null || st == null || sid == null || sct == null)
      throw new InconsistentFSStateException(sd.root,
                                             "file " + STORAGE_FILE_VERSION + " is invalid.");
    int rv = Integer.parseInt(sv);
    NodeType rt = NodeType.valueOf(st);
    int rid = Integer.parseInt(sid);
    long rct = Long.parseLong(sct);
    if (!storageType.equals(rt) ||
        !((namespaceID == 0) || (rid == 0) || namespaceID == rid))
      throw new InconsistentFSStateException(sd.root,
                                             "is incompatible with others. " +
                                             " namespaceID is " + namespaceID +
                                             " and rid is " + rid + "," +
                                             " storage type is " + storageType + 
                                             " but rt is " + rt);
    if (rv < FSConstants.LAYOUT_VERSION) // future version
      throw new IncorrectVersionException(rv, "storage directory " 
                                          + sd.root.getCanonicalPath());
    layoutVersion = rv;
    storageType = rt;
    namespaceID = rid;
    cTime = rct;
  }
  
  /**
   * Set common storage fields.
   * Should be overloaded if additional fields need to be set.
   * 
   * @param props
   * @throws IOException
   */
  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    props.setProperty(LAYOUT_VERSION, String.valueOf(layoutVersion));
    props.setProperty(STORAGE_TYPE, storageType.toString());
    props.setProperty(NAMESPACE_ID, String.valueOf(namespaceID));
    props.setProperty(CHECK_TIME, String.valueOf(cTime));
  }

  public static void rename(File from, File to) throws IOException {
    if (!from.renameTo(to))
      throw new IOException("Failed to rename "
                            + from.getCanonicalPath() + " to " + to.getCanonicalPath());
  }

  public static void upgradeDirectory(StorageDirectory sd) throws IOException {
    File curDir = sd.getCurrentDir();
    File prevDir = sd.getPreviousDir();
    File tmpDir = sd.getPreviousTmp();
    assert curDir.exists() : "Current directory must exist.";
    assert !prevDir.exists() : "prvious directory must not exist.";
    assert !tmpDir.exists() : "prvious.tmp directory must not exist.";

    // rename current to tmp
    rename(curDir, tmpDir);

    if (!curDir.mkdir()) {
      throw new IOException("Cannot create directory " + curDir);
    }
  }

  public static void completeUpgrade(StorageDirectory sd) throws IOException {
    // Write the version file, since saveFsImage above only makes the
    // fsimage, and the directory is otherwise empty.
    sd.write();
    InjectionHandler
        .processEventIO(InjectionEvent.FSIMAGE_UPGRADE_AFTER_SAVE_IMAGE);

    File prevDir = sd.getPreviousDir();
    File tmpDir = sd.getPreviousTmp();
    // rename tmp to previous
    rename(tmpDir, prevDir);
  }

  public static void deleteDir(File dir) throws IOException {
    if (!FileUtil.fullyDelete(dir))
      throw new IOException("Failed to delete " + dir.getCanonicalPath());
  }

  /**
   * Write all data storage files.
   * @throws IOException
   */
  public void writeAll() throws IOException {
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      it.next().write();
    }
  }

  /**
   * Unlock all storage directories.
   * @throws IOException
   */
  public void unlockAll() throws IOException {
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      it.next().unlock();
    }
  }

  /**
   * Check whether underlying file system supports file locking.
   *
   * @return <code>true</code> if exclusive locks are supported or
   *         <code>false</code> otherwise.
   * @throws IOException
   * @see StorageDirectory#lock()
   */
  public boolean isLockSupported(int idx) throws IOException {
    StorageDirectory sd = storageDirs.get(idx);
    FileLock firstLock = null;
    FileLock secondLock = null;
    try {
      firstLock = sd.lock;
      if(firstLock == null) {
        firstLock = sd.tryLock();
        if(firstLock == null)
          return true;
      }
      secondLock = sd.tryLock();
      if(secondLock == null)
        return true;
    } finally {
      if(firstLock != null && firstLock != sd.lock) {
        firstLock.release();
        firstLock.channel().close();
      }
      if(secondLock != null) {
        secondLock.release();
        secondLock.channel().close();
      }
    }
    return false;
  }

  public static String getBuildVersion() {
    return VersionInfo.getRevision();
  }

  public static String getRegistrationID(StorageInfo storage) {
    return "NS-" + Integer.toString(storage.getNamespaceID())
      + "-" + Integer.toString(storage.getLayoutVersion())
      + "-" + Long.toString(storage.getCTime());
  }

  protected static String getProperty(Properties props, StorageDirectory sd,
      String name) throws InconsistentFSStateException {
    String property = props.getProperty(name);
    if (property == null) {
      throw new InconsistentFSStateException(sd.root, "file "
          + STORAGE_FILE_VERSION + " has " + name + " missing.");
    }
    return property;
  }
  
  /** Validate and set storage type from {@link Properties}*/
  protected void setStorageType(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    NodeType type = NodeType.valueOf(getProperty(props, sd, STORAGE_TYPE));
    if (!storageType.equals(type)) {
      throw new InconsistentFSStateException(sd.root,
          "node type is incompatible with others.");
    }
    storageType = type;
  }
  
  /** Validate and set ctime from {@link Properties}*/
  protected void setcTime(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    cTime = Long.parseLong(getProperty(props, sd, CHECK_TIME));
  }

  /** Validate and set layout version from {@link Properties}*/
  protected void setLayoutVersion(Properties props, StorageDirectory sd)
      throws IncorrectVersionException, InconsistentFSStateException {
    int lv = Integer.parseInt(getProperty(props, sd, LAYOUT_VERSION));
    if (lv < FSConstants.LAYOUT_VERSION) { // future version
      throw new IncorrectVersionException(lv, "storage directory "
          + sd.root.getAbsolutePath());
    }
    layoutVersion = lv;
  }
  
  /** Validate and set namespaceID version from {@link Properties}*/
  protected void setNamespaceID(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    int nsId = Integer.parseInt(getProperty(props, sd, NAMESPACE_ID));
    if (namespaceID != 0 && nsId != 0 && namespaceID != nsId) {
      throw new InconsistentFSStateException(sd.root,
          "namespaceID is incompatible with others.");
    }
    namespaceID = nsId;
  }
  
  // Pre-upgrade version compatibility
  protected abstract void corruptPreUpgradeStorage(File rootDir) throws IOException;

  protected void writeCorruptedData(RandomAccessFile file) throws IOException {
    final String messageForPreUpgradeVersion =
      "\nThis file is INTENTIONALLY CORRUPTED so that versions\n"
      + "of Hadoop prior to 0.13 (which are incompatible\n"
      + "with this directory layout) will fail to start.\n";

    file.seek(0);
    file.writeInt(FSConstants.LAYOUT_VERSION);
    org.apache.hadoop.io.UTF8.writeString(file, "");
    file.writeBytes(messageForPreUpgradeVersion);
    file.getFD().sync();
  }  
  
  public Iterable<StorageDirectory> dirIterable(final StorageDirType dirType) {
    return new Iterable<StorageDirectory>() {
      @Override
      public Iterator<StorageDirectory> iterator() {
        return dirIterator(dirType);
      }
    };
  }
  
  /**
   * Interface for classes which need to have the user confirm their
   * formatting during NameNode -format and other similar operations.
   * 
   * This is currently a storage directory or journal manager.
   */
  public interface FormatConfirmable {
    /**
     * @return true if the storage seems to have some valid journal data in it,
     * and the user should be required to confirm the format. Otherwise,
     * false.
     * @throws IOException if the storage cannot be accessed at all.
     */
    public boolean hasSomeJournalData() throws IOException;
    
    /**
     * @return true if the storage seems to have some valid image data in it,
     * and the user should be required to confirm the format. Otherwise,
     * false.
     * @throws IOException if the storage cannot be accessed at all.
     */
    public boolean hasSomeImageData() throws IOException;
    
    /**
     * @return a string representation of the formattable item, suitable
     * for display to the user inside a prompt
     */
    @Override
    public String toString();
  }
  
  /**
   * Iterate over each of the {@link FormatConfirmable} objects,
   * potentially checking with the user whether it should be formatted.
   * 
   * If running in interactive mode, will prompt the user for each
   * directory to allow them to format anyway. Otherwise, returns
   * false, unless 'force' is specified.
   * 
   * @param interactive prompt the user when a dir exists
   * @return true if formatting should proceed
   * @throws IOException if some storage cannot be accessed
   */
  public static boolean confirmFormat(
      Iterable<? extends FormatConfirmable> items, boolean force,
      boolean interactive)
      throws IOException {
    for (FormatConfirmable item : items) {
      if (!(item.hasSomeJournalData() || item.hasSomeImageData()))
        continue;
      if (force) { // Don't confirm, always format.
        System.err.println(
            "Data exists in " + item + ". Formatting anyway.");
        continue;
      }
      if (!interactive) { // Don't ask - always don't format
        System.err.println(
            "Running in non-interactive mode, and data appears to exist in " +
            item + ". Not formatting.");
        return false;
      }
      if (!ToolRunner.confirmPrompt("Re-format filesystem in " + item + " ?")) {
        System.err.println("Format aborted in " + item);
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * @return the storage directory, with the precondition that this storage
   * has exactly one storage directory
   */
  public StorageDirectory getSingularStorageDir() {
    Preconditions.checkState(storageDirs.size() == 1);
    return storageDirs.get(0);
  }
}
