/*
 * UpgradeUtilities.java
 *
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.HashMap;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;

import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.NameSpaceSliceStorage;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * This class defines a number of static helper methods used by the
 * DFS Upgrade unit tests.  By default, a singleton master populated storage
 * directory is created for a Namenode (contains edits, fsimage,
 * version, and time files) and a Datanode (contains version and
 * block files).  The master directories are lazily created.  They are then
 * copied by the createStorageDirs() method to create new storage
 * directories of the appropriate type (Namenode or Datanode).
 */
public class UpgradeUtilities {

  // Root scratch directory on local filesystem 
  private static File TEST_ROOT_DIR = new File(
      System.getProperty("test.build.data","/tmp").replace(' ', '+'));
  // The singleton master storage directory for Namenode
  private static File namenodeStorage = new File(TEST_ROOT_DIR, "namenodeMaster");
  // checksums of the contents in namenodeStorage directory
  private static long[] namenodeStorageChecksums;
  // The namespaceId of the namenodeStorage directory
  private static int namenodeStorageNamespaceID;
  // The fsscTime of the namenodeStorage directory
  private static long namenodeStorageFsscTime;
  // The singleton master storage directory for Datanode
  private static File datanodeStorage = new File(TEST_ROOT_DIR, "datanodeMaster");
  // The singleton storage directory for single namespace
  private static File datanodeNSStorage;
  // a checksum of the contents in datanodeStorage directory
  private static long datanodeStorageChecksum;
  // Temp a checksum of the contents in datanodeStorage directory for all namespaces
  private static long[] datanodeNSStorageChecksums;
  
  public static void initialize() throws Exception {
    initialize(1, new Configuration(), false);
  }
  
  /**
   * Initialize the data structures used by federation test;
   * IMPORTANT NOTE: This method must be called once before calling 
   *                 any other public method on this class.  
   * <p>
   * Creates a singleton master populated storage
   * directory for multiple namenodes (contain edits, fsimage,
   * version, and time files) and a Datanode (contains version and
   * block files for all namespaces).  This can be a lengthy operation.
   */
  public static void initialize(int numNameNodes, Configuration config, 
      boolean federation) throws Exception {
    if (!federation && numNameNodes > 1) {
      throw new Exception("Shouldn't have more than 1 namenode in " +
        "non-federation cluster.");
    }
    createEmptyDirs(new String[] {TEST_ROOT_DIR.toString()});
    config.set("dfs.name.dir", namenodeStorage.toString());
    config.set("dfs.name.edits.dir", namenodeStorage.toString());
    config.set("dfs.data.dir", datanodeStorage.toString());
    MiniDFSCluster cluster = null;
    try {
      // format data-node
      createEmptyDirs(new String[] {datanodeStorage.toString()});
      
      if (federation) {
        // format and start NameNode and start DataNode
        cluster = new MiniDFSCluster(0, config, 1, true, false, null,
            numNameNodes);
      } else {
        NameNode.format(config); 
        cluster = new MiniDFSCluster(config, 1, StartupOption.REGULAR);
      }
      datanodeNSStorageChecksums = new long[numNameNodes];
      for (int nnIndex = 0; nnIndex < numNameNodes; nnIndex++) {
        NameNode namenode = cluster.getNameNode(nnIndex);
        if (!federation) {
          namenodeStorageNamespaceID = namenode.versionRequest().getNamespaceID();
          namenodeStorageFsscTime = namenode.versionRequest().getCTime();
        }
        FileSystem fs = cluster.getFileSystem(nnIndex);
        Path baseDir = new Path("/TestUpgrade");
        fs.mkdirs(baseDir);
        // write some files
        int bufferSize = 4096;
        byte[] buffer = new byte[bufferSize];
        for(int i=0; i < bufferSize; i++)
          buffer[i] = (byte)('0' + i % 50);
        writeFile(fs, new Path(baseDir, "file1"), buffer, bufferSize);
        writeFile(fs, new Path(baseDir, "file2"), buffer, bufferSize);
      
        // save image
        namenode.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
        namenode.saveNamespace();
        namenode.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      
        // write more files
        writeFile(fs, new Path(baseDir, "file3"), buffer, bufferSize);
        writeFile(fs, new Path(baseDir, "file4"), buffer, bufferSize);
        datanodeNSStorage = NameSpaceSliceStorage.getNsRoot(namenode.getNamespaceID(),
            new File(datanodeStorage, "current"));
        datanodeNSStorageChecksums[nnIndex] = checksumContents(DATA_NODE,
            new File(datanodeNSStorage,
            MiniDFSCluster.FINALIZED_DIR_NAME));
      }
    } finally {
      // shutdown
      if (cluster != null) cluster.shutdown();
      namenodeStorageChecksums = new long[numNameNodes];
      if (federation) {
        String[] serviceIds = 
            config.getStrings(FSConstants.DFS_FEDERATION_NAMESERVICES);
        int nnIndex = 0;
        for (String serviceId : serviceIds) {
          File nsStorage = new File(namenodeStorage, serviceId);
          FileUtil.fullyDelete(new File(nsStorage, "in_use.lock"));
          namenodeStorageChecksums[nnIndex] = checksumContents(
              NAME_NODE, new File(nsStorage,"current"));
          nnIndex++;
        }
      } else {
        namenodeStorageChecksums[0] = checksumContents(
            NAME_NODE, new File(namenodeStorage,"current"));
        FileUtil.fullyDelete(new File(namenodeStorage,"in_use.lock"));
      }
      FileUtil.fullyDelete(new File(datanodeStorage,"in_use.lock"));
    }

    datanodeStorageChecksum = checksumContents(
        DATA_NODE, new File(datanodeStorage, "current"));
  }
  
  // Private helper method that writes a file to the given file system.
  private static void writeFile(FileSystem fs, Path path, byte[] buffer,
                                int bufferSize) throws IOException 
  {
    OutputStream out;
    out = fs.create(path, true, bufferSize, (short) 1, 1024);
    out.write(buffer, 0, bufferSize);
    out.close();
  }
  
  /**
   * Initialize dfs.name.dir and dfs.data.dir with the specified number of
   * directory entries. Also initialize dfs.blockreport.intervalMsec.
   */
  public static Configuration initializeStorageStateConf(int numDirs,
                                                         Configuration conf) {
    String defaultNameDir = namenodeStorage.toString();
    String defaultDataDir = datanodeStorage.toString();
    StringBuffer nameNodeDirs =
      new StringBuffer(new File(TEST_ROOT_DIR, defaultNameDir).toString());
    StringBuffer dataNodeDirs =
      new StringBuffer(new File(TEST_ROOT_DIR, defaultDataDir).toString());
    for (int i = 2; i <= numDirs; i++) {
      nameNodeDirs.append("," + new File(TEST_ROOT_DIR, defaultNameDir+i));
      dataNodeDirs.append("," + new File(TEST_ROOT_DIR, defaultDataDir+i));
    }
    if (conf == null) {
      conf = new Configuration();
    }
    conf.set("dfs.name.dir", nameNodeDirs.toString());
    conf.set("dfs.name.edits.dir", nameNodeDirs.toString());
    conf.set("dfs.data.dir", dataNodeDirs.toString());
    conf.setInt("dfs.blockreport.intervalMsec", 10000);
    return conf;
  }
  
  /**
   * Create empty directories.  If a specified directory already exists
   * then it is first removed.
   */
  public static void createEmptyDirs(String[] dirs) throws IOException {
    for (String d : dirs) {
      File dir = new File(d);
      if (dir.exists()) {
        FileUtil.fullyDelete(dir);
      }
      dir.mkdirs();
    }
  }
  
  /**
   * Return the checksum for the singleton master storage directory
   * of the given node type.
   */
  public static long checksumMasterContents(NodeType nodeType) throws IOException {
    return checksumMasterContents(nodeType, 0);
  }
  
  public static long checksumMasterContents(NodeType nodeType, int nnIndex) throws IOException {
    if (nodeType == NAME_NODE) {
      return namenodeStorageChecksums[nnIndex];
    } else {
      return datanodeStorageChecksum;
    }
  }
  
  public static long checksumDatanodeNSStorageContents() throws IOException {
    return checksumDatanodeNSStorageContents(0); 
  }

  /**
   * Return the checksum for the datanode storage directory for a namespace
   */
  public static long checksumDatanodeNSStorageContents(int nnIndex) throws IOException {
    return datanodeNSStorageChecksums[nnIndex];
  }
  
  public static long checksumContents(NodeType nodeType, File dir) throws IOException {
    return checksumContents(nodeType, dir, null);
  }

  private static boolean inList(String[] list, String search) {
    if (list == null) {
      return false;
    }
    for (String file : list) {
      if (file.equals(search)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compute the checksum of all the files in the specified directory. The
   * contents of subdirectories are not included. This method provides an easy
   * way to ensure equality between the contents of two directories.
   *
   * @param nodeType
   *          if DATA_NODE then any file named "VERSION" is ignored. This is
   *          because this file file is changed every time the Datanode is
   *          started.
   * @param dir
   *          must be a directory. Subdirectories are ignored.
   * @param skipFiles
   *          files to be skipped
   *
   * @throws IllegalArgumentException
   *           if specified directory is not a directory
   * @throws IOException
   *           if an IOException occurs while reading the files
   * @return the computed checksum value
   */
  public static long checksumContents(NodeType nodeType, File dir,
      String[] skipFiles) throws IOException {
    if (!dir.isDirectory()) {
      throw new IllegalArgumentException(
                                         "Given argument is not a directory:" + dir);
    }
    File[] list = dir.listFiles();
    Arrays.sort(list);
    CRC32 checksum = new CRC32();
    for (int i = 0; i < list.length; i++) {
      if (!list[i].isFile() || inList(skipFiles, list[i].getName())) {
        continue;
      }

      // skip VERSION file for DataNodes
      if (nodeType == DATA_NODE && list[i].getName().equals("VERSION")) {
        continue; 
      }
      FileInputStream fis = null;
      try {
        fis = new FileInputStream(list[i]);
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
          checksum.update(buffer, 0, bytesRead);
        }
      } finally {
        if(fis != null) {
          fis.close();
        }
      }
    }
    return checksum.getValue();
  }
  
  /*
   * Copy everything from namenode source directory 
   */
  public static void createFederatedNameNodeStorageDirs(String[] parents) 
      throws Exception {
    LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
    for (int i = 0; i < parents.length; i++) {
      File newDir = new File(parents[i]);
      createEmptyDirs(new String[] {newDir.toString()});
      localFS.copyToLocalFile(new Path(namenodeStorage.toString()),
          new Path(newDir.toString()),
          false);
    }
  }
  
  /**
   * Simulate the <code>dfs.name.dir</code> or <code>dfs.data.dir</code>
   * of a populated DFS filesystem.
   *
   * This method creates and populates the directory specified by
   *  <code>parent/dirName</code>, for each parent directory.
   * The contents of the new directories will be
   * appropriate for the given node type.  If the directory does not
   * exist, it will be created.  If the directory already exists, it
   * will first be deleted.
   *
   * By default, a singleton master populated storage
   * directory is created for a Namenode (contains edits, fsimage,
   * version, and time files) and a Datanode (contains version and
   * block files).  These directories are then
   * copied by this method to create new storage
   * directories of the appropriate type (Namenode or Datanode).
   *
   * @return the array of created directories
   */
  public static File[] createStorageDirs(NodeType nodeType, String[] parents,
      String dirName) throws Exception {
    switch (nodeType) {
    case NAME_NODE:
      return createStorageDirs(nodeType, parents, dirName, namenodeStorage);
    case DATA_NODE:
      return createStorageDirs(nodeType, parents, dirName, datanodeStorage);
    }
    return null;
  }
  
  public static File[] createStorageDirs(NodeType nodeType, String[] parents, String dirName,
      File srcFile) throws Exception {
    File[] retVal = new File[parents.length];
    for (int i = 0; i < parents.length; i++) {
      File newDir = new File(parents[i], dirName);
      createEmptyDirs(new String[] {newDir.toString()});
      LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
      switch (nodeType) {
      case NAME_NODE:
        localFS.copyToLocalFile(new Path(srcFile.toString(), "current"),
                                new Path(newDir.toString()),
                                false);
        Path newImgDir = new Path(newDir.getParent(), "image");
        if (!localFS.exists(newImgDir))
          localFS.copyToLocalFile(
              new Path(srcFile.toString(), "image"),
              newImgDir,
              false);
        break;
      case DATA_NODE:
        localFS.copyToLocalFile(new Path(srcFile.toString(), "current"),
                                new Path(newDir.toString()),
                                false);
        Path newStorageFile = new Path(newDir.getParent(), "storage");
        if (!localFS.exists(newStorageFile))
          localFS.copyToLocalFile(
              new Path(srcFile.toString(), "storage"),
              newStorageFile,
              false);
        break;
      }
      retVal[i] = newDir;
    }
    return retVal;
  }

  public static File[] createFederatedDatanodeDirs(String[] parents,
      String dirName, int namespaceId) throws IOException {
    File[] retVal = new File[parents.length];
    for (int i = 0; i < parents.length; i++) {
      File nsDir = new File(new File(parents[i], "current"), "NS-"
          + namespaceId);
      File newDir = new File(nsDir, dirName);
      File srcDir = new File(new File(datanodeStorage, "current"), "NS-"
          + namespaceId);

      LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
      localFS.copyToLocalFile(new Path(srcDir.toString(), "current"), new Path(
          newDir.toString()), false);
      retVal[i] = new File(parents[i], "current");
    }
    return retVal;
  }

  public static File[] createVersionFile(NodeType nodeType, File[] parent,
      StorageInfo version, int namespaceId) throws IOException {
    return createVersionFile(nodeType, parent, version, namespaceId, true);
  }
  
  public static void createFederatedDatanodesVersionFiles(File[] parents,
      int namespaceId, StorageInfo version, String dirName) throws IOException {
    for (File parent : parents) {
      File nsRoot = NameSpaceSliceStorage.getNsRoot(namespaceId, parent);
      Properties props = new Properties();
      props.setProperty(NameSpaceSliceStorage.NAMESPACE_ID,
          String.valueOf(version.getNamespaceID()));
      props.setProperty(NameSpaceSliceStorage.CHECK_TIME,
          String.valueOf(version.getCTime()));
      props.setProperty(NameSpaceSliceStorage.LAYOUT_VERSION,
          String.valueOf(version.getLayoutVersion()));
      File nsVersionFile = new File(new File(nsRoot,
          dirName), "VERSION");
      Storage.writeProps(nsVersionFile, props);
    }
  }

  /**
   * Create a <code>version</code> file inside the specified parent
   * directory.  If such a file already exists, it will be overwritten.
   * The given version string will be written to the file as the layout
   * version. None of the parameters may be null.
   *
   * @param version
   *
   * @return the created version file
   */
  public static File[] createVersionFile(NodeType nodeType, File[] parent,
      StorageInfo version, int namespaceId, boolean nsLayout)
      throws IOException
  {
    Storage storage = null;
    File[] versionFiles = new File[parent.length];
    for (int i = 0; i < parent.length; i++) {
      File versionFile = new File(parent[i], "VERSION");
      FileUtil.fullyDelete(versionFile);
      switch (nodeType) {
      case NAME_NODE:
        storage = new NNStorage(version);
        break;
      case DATA_NODE:
        storage = new DataStorage(version, "doNotCare", null);
        if (version.layoutVersion <= FSConstants.FEDERATION_VERSION) {
          File nsRoot = NameSpaceSliceStorage.getNsRoot(namespaceId, parent[i]);
          Properties props = new Properties();
          props.setProperty(NameSpaceSliceStorage.NAMESPACE_ID,
              String.valueOf(version.getNamespaceID()));
          props.setProperty(NameSpaceSliceStorage.CHECK_TIME,
              String.valueOf(version.getCTime()));
          if (nsLayout) {
            props.setProperty(NameSpaceSliceStorage.LAYOUT_VERSION,
                String.valueOf(version.getLayoutVersion()));
          }
          File nsVersionFile = new File(new File(nsRoot, DataStorage.STORAGE_DIR_CURRENT), "VERSION");
          Storage.writeProps(nsVersionFile, props);
        }
        break;
      }
      StorageDirectory sd = storage.new StorageDirectory(parent[i].getParentFile());
      sd.write(versionFile);
      versionFiles[i] = versionFile;
    }
    return versionFiles;
  }
  
  /**
   * Corrupt the specified file.  Some random bytes within the file
   * will be changed to some random values.
   *
   * @throws IllegalArgumentException if the given file is not a file
   * @throws IOException if an IOException occurs while reading or writing the file
   */
  public static void corruptFile(File file) throws IOException {
    if (!file.isFile()) {
      throw new IllegalArgumentException(
                                         "Given argument is not a file:" + file);
    }
    RandomAccessFile raf = new RandomAccessFile(file,"rws");
    Random random = new Random();
    for (long i = 0; i < raf.length(); i++) {
      raf.seek(i);
      if (random.nextBoolean()) {
        raf.writeByte(random.nextInt());
      }
    }
    raf.close();
  }
  
  /**
   * Return the layout version inherent in the current version
   * of the Namenode, whether it is running or not.
   */
  public static int getCurrentLayoutVersion() {
    return FSConstants.LAYOUT_VERSION;
  }
  
  /**
   * Return the namespace ID inherent in the currently running
   * Namenode.  If no Namenode is running, return the namespace ID of
   * the master Namenode storage directory.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static int getCurrentNamespaceID(MiniDFSCluster cluster) throws IOException {
    if (cluster != null) {
      return cluster.getNameNode().versionRequest().getNamespaceID();
    }
    return namenodeStorageNamespaceID;
  }
  
  /**
   * Return the File System State Creation Timestamp (FSSCTime) inherent
   * in the currently running Namenode.  If no Namenode is running,
   * return the FSSCTime of the master Namenode storage directory.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static long getCurrentFsscTime(MiniDFSCluster cluster) throws IOException {
    if (cluster != null) {
      return cluster.getNameNode().versionRequest().getCTime();
    }
    return namenodeStorageFsscTime;
  }
}

