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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.BufferedByteInputStream;
import org.apache.hadoop.io.BufferedByteOutputStream;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.raid.RaidCodec;
import org.apache.hadoop.util.FlushableLogger;
import org.apache.hadoop.hdfs.server.namenode.INodeRaidStorage.RaidBlockInfo;

import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

/**
 * Contains inner classes for reading or writing the on-disk format for FSImages.
 */
class FSImageFormat {
  private static final Log LOG = FSImage.LOG;
  
  // immediate flush logger
  private static final Log FLOG = FlushableLogger.getLogger(LOG);
  
  // Static-only class
  private FSImageFormat() {}
  
  /**
   * A one-shot class responsible for loading an image. The load() function
   * should be called once, after which the getter methods may be used to retrieve
   * information about the image that was loaded, if loading was successful.
   */
  static class Loader {
    private final Configuration conf;
    /** which namesystem this loader is working for */
    private final FSNamesystem namesystem;

    /** Set to true once a file has been loaded using this loader. */
    private boolean loaded = false;

    /** The transaction ID of the last edit represented by the loaded file */
    private long imgTxId;
    /** The MD5 sum of the loaded file */
    private MD5Hash imgDigest;
    
    private boolean needToSave = true;
    
    private StorageInfo storage;

    Loader(Configuration conf, FSNamesystem namesystem, StorageInfo storage) {
      this.conf = conf;
      this.namesystem = namesystem;
      this.storage = storage;
    }

    /**
     * Return the MD5 checksum of the image that has been loaded.
     * @throws IllegalStateException if load() has not yet been called.
     */
    MD5Hash getLoadedImageMd5() {
      checkLoaded();
      return imgDigest;
    }

    long getLoadedImageTxId() {
      checkLoaded();
      return imgTxId;
    }
    
    boolean getNeedToSave() {
      return needToSave;
    }

    /**
     * Throw IllegalStateException if load() has not yet been called.
     */
    private void checkLoaded() {
      if (!loaded) {
        throw new IllegalStateException("Image not yet loaded!");
      }
    }

    /**
     * Throw IllegalStateException if load() has already been called.
     */
    private void checkNotLoaded() {
      if (loaded) {
        throw new IllegalStateException("Image already loaded!");
      }
    }

    void load(ImageInputStream iis, DataInputStream in) throws IOException {
      checkNotLoaded();
      DigestInputStream fin = null;
      if (iis == null )
        throw new IOException("curFile is null");

      long startTime = now();

      //
      // Load in bits
      //
      MessageDigest digester = null;
      
      if (in == null) {
        InputStream fis = iis.getInputStream();
        digester = MD5Hash.getDigester();
        fin = new DigestInputStream(fis, digester);
        in = new DataInputStream(fin);
      }     
      try {
        /*
         * Note: Remove any checks for version earlier than 
         * Storage.LAST_UPGRADABLE_LAYOUT_VERSION since we should never get 
         * to here with older images.
         */

        /*
         * TODO we need to change format of the image file
         * it should not contain version and namespace fields
         */
        // read image version: first appeared in version -1
        int imgVersion = in.readInt();
        needToSave = (imgVersion != FSConstants.LAYOUT_VERSION);

        // read namespaceID: first appeared in version -2
        storage.namespaceID = in.readInt();

        // read number of files
        long numFiles;
        if (imgVersion <= -16) {
          numFiles = in.readLong();
        } else {
          numFiles = in.readInt();
        }
        
        storage.layoutVersion = imgVersion;
        // read in the last generation stamp.
        if (imgVersion <= -12) {
          long genstamp = in.readLong();
          namesystem.setGenerationStamp(genstamp); 
        }
        
        // read the transaction ID of the last edit represented by
        // this image
        if (LayoutVersion.supports(Feature.STORED_TXIDS, imgVersion)) {
          imgTxId = in.readLong();
        } else {
          imgTxId = -1;
        }
        
        // read the last allocated inode id in the fsimage
        if (LayoutVersion.supports(Feature.ADD_INODE_ID, imgVersion)) {
          long lastInodeId = in.readLong();
          namesystem.dir.resetLastInodeId(lastInodeId);
          LOG.info("load last allocated InodeId from fsimage:" + lastInodeId);
        } else {
          LOG.info("Old layout version doesn't have last inode id."
                + " Will assign new id for each inode.");
        }
        
        // read compression related info
        FSImageCompression compression;
        if (LayoutVersion.supports(Feature.FSIMAGE_COMPRESSION, imgVersion)) {
          compression = FSImageCompression.readCompressionHeader(conf, in);
        } else {
          compression = FSImageCompression.createNoopCompression();
        }
        InputStream is = compression.unwrapInputStream(fin != null ? fin : in);
        
        // have a separate thread for reading and decompression
        in = BufferedByteInputStream.wrapInputStream(is,
            FSImage.LOAD_SAVE_BUFFER_SIZE, FSImage.LOAD_SAVE_CHUNK_SIZE);

        FLOG.info("Loading image file " + iis + " using " + compression);
        
        // load all inodes
        FLOG.info("Number of files = " + numFiles);
        
        // create loading context
        FSImageLoadingContext context = new FSImageLoadingContext(this.namesystem.dir);         
        // cache support options
        context.supportsFileAccessTime = LayoutVersion.supports(
            Feature.FILE_ACCESS_TIME, imgVersion);
        context.supportsNamespaceQuota = LayoutVersion.supports(
            Feature.NAMESPACE_QUOTA, imgVersion);
        context.supportsDiskspaceQuota = LayoutVersion.supports(
            Feature.DISKSPACE_QUOTA, imgVersion);
        context.supportsHardlink = LayoutVersion.supports(Feature.HARDLINK,
            imgVersion);
        context.supportsAddInodeId = LayoutVersion.supports(Feature.ADD_INODE_ID,
            imgVersion);
        context.supportsRaid = LayoutVersion.supports(Feature.ADD_RAID, 
            imgVersion);
        
        if (LayoutVersion.supports(Feature.FSIMAGE_NAME_OPTIMIZATION,
            imgVersion)) {          
          loadLocalNameINodes(numFiles, in, context);
        } else {
          loadFullNameINodes(numFiles, in, context);
        }

        // load datanode info
        this.loadDatanodes(in);

        // load Files Under Construction
        this.loadFilesUnderConstruction(in);

        // make sure to read to the end of file
        int eof = in.read();
        assert eof == -1 : "Should have reached the end of image file " + iis;
      } finally {
        in.close();
      }

      if (digester != null)
        imgDigest = new MD5Hash(digester.digest());
      loaded = true;
      namesystem.dir.imageLoaded();
      
      FLOG.info("Image file " + iis + " of size " + iis.getSize()
          + " loaded in " + (now() - startTime) / 1000 + " seconds.");
    }

    /** Update the root node's attributes */
    private void updateRootAttr(INode root) throws QuotaExceededException{                                                           
      long nsQuota = root.getNsQuota();
      long dsQuota = root.getDsQuota();
      FSDirectory fsDir = namesystem.dir;
      if (nsQuota != -1 || dsQuota != -1) {
        fsDir.rootDir.setQuota(nsQuota, dsQuota);
      }
      fsDir.rootDir.setModificationTime(root.getModificationTime());
      fsDir.rootDir.setPermissionStatus(root.getPermissionStatus());    
    }
  
    /** 
     * load fsimage files assuming only local names are stored
     *   
     * @param numFiles number of files expected to be read
     * @param in image input stream
     * @param context The context when loading the FSImage
     * @throws IOException
     */  
     private void loadLocalNameINodes(long numFiles, DataInputStream in,
         FSImageLoadingContext context) 
     throws IOException {
       assert LayoutVersion.supports(Feature.FSIMAGE_NAME_OPTIMIZATION,
           getLayoutVersion());
       assert numFiles > 0;
       long filesLoaded = 0;
       // load root
       if (in.readShort() != 0) {
         throw new IOException("First node is not root");
       }
       
       INode root = loadINode(in, context, true);
       
       // update the root's attributes
       updateRootAttr(root);
       filesLoaded++;
  
       // load rest of the nodes directory by directory
       int percentDone = 0;
       while (filesLoaded < numFiles) {
         filesLoaded += loadDirectory(in, context);
         percentDone = printProgress(filesLoaded, numFiles, percentDone);
       }
       if (numFiles != filesLoaded) {
         throw new IOException("Read unexpect number of files: " + filesLoaded);
       }
     }
     
     /**
      * Load all children of a directory
      * 
      * @param in
      * @param context The context when loading the FSImage
      * @return number of child inodes read
      * @throws IOException
      */
     private int loadDirectory(DataInputStream in, FSImageLoadingContext context) 
         throws IOException {
       // read the parent 
       byte[] parentName = new byte[in.readShort()];
       in.readFully(parentName);
       
       FSDirectory fsDir = namesystem.dir;
       INode parent = fsDir.rootDir.getNode(parentName);
       if (parent == null || !parent.isDirectory()) {
         throw new IOException("Path " + new String(parentName, "UTF8")
             + " is not a directory.");
       }
  
       int numChildren = in.readInt();
       for(int i=0; i<numChildren; i++) {
         // load single inode
         byte[] localName = new byte[in.readShort()];
         in.readFully(localName); // read local name
         INode newNode = loadINode(in, context, false); // read rest of inode
  
         // add to parent
          namesystem.dir.addToParent(localName, (INodeDirectory) parent, newNode,
              false, i);
       }
       return numChildren;
     }
  
    /**
     * load fsimage files assuming full path names are stored
     * 
     * @param numFiles total number of files to load
     * @param in data input stream
     * @throws IOException if any error occurs
     */
    private void loadFullNameINodes(long numFiles, DataInputStream in,
        FSImageLoadingContext context) throws IOException {
      byte[][] pathComponents;
      byte[][] parentPath = {{}};      
      FSDirectory fsDir = namesystem.dir;
      INodeDirectory parentINode = fsDir.rootDir;
      int percentDone = 0;
      for (long i = 0; i < numFiles; i++) {
        percentDone = printProgress(i, numFiles, percentDone);
        
        pathComponents = FSImageSerialization.readPathComponents(in);
        
        boolean rootNode = isRoot(pathComponents);
        INode newNode = loadINode(in, context, rootNode);
  
        if (isRoot(pathComponents)) { // it is the root
          // update the root's attributes
          updateRootAttr(newNode);
          continue;
        }
        // check if the new inode belongs to the same parent
        if(!isParent(pathComponents, parentPath)) {
          parentINode = fsDir.getParent(pathComponents);
          parentPath = getParent(pathComponents);
        }
  
        // add new inode
        parentINode = fsDir.addToParent(pathComponents[pathComponents.length-1], 
            parentINode, newNode, false, INodeDirectory.UNKNOWN_INDEX);
      }
    }
    
    /**
     * load an inode from fsimage except for its name
     * 
     * @param in data input stream from which image is read
     * @param context The context when loading the FSImage
     * @param rootNode whether the node is loading is root node
     * @return an inode
     */
    private INode loadINode(DataInputStream in, FSImageLoadingContext context, boolean rootNode)
        throws IOException {
      long modificationTime = 0;
      long atime = 0;
      long blockSize = 0;
      int imgVersion = getLayoutVersion();
      
      long inodeId;
      if (context.supportsAddInodeId) {
        inodeId = in.readLong();
      } else {
        inodeId = rootNode ? INodeId.ROOT_INODE_ID : namesystem.dir.allocateNewInodeId();
      }
      
      byte inodeType = INode.INodeType.REGULAR_INODE.type;
      long hardLinkID = -1; 
      RaidCodec codec = null;
      
      if (context.supportsHardlink) {  
        inodeType = in.readByte();  
        if (inodeType == INode.INodeType.HARDLINKED_INODE.type) {  
          hardLinkID = WritableUtils.readVLong(in); 
        } else {
          if (context.supportsRaid) {
            if (inodeType == INode.INodeType.RAIDED_INODE.type) {
              String codecId = WritableUtils.readString(in);
              codec = RaidCodec.getCodec(codecId);
              if (codec == null) {
                throw new IOException("Couldn't find the codec for " + codecId);
              }
            } else if (inodeType == INode.INodeType.HARDLINK_RAIDED_INODE.type) {
              // At this moment, we don't support hardlinking raided files
              throw new IOException("We don't support hardlink raided inode");
            }
          }
        }
      }
      
      short replication = in.readShort();
      replication = namesystem.adjustReplication(replication);
      modificationTime = in.readLong();
      if (context.supportsFileAccessTime) {
        atime = in.readLong();
      }
      if (imgVersion <= -8) {
        blockSize = in.readLong();
      }
      int numBlocks = in.readInt();
      BlockInfo blocks[] = null;
  
      // for older versions, a blocklist of size 0
      // indicates a directory.
      if ((-9 <= imgVersion && numBlocks > 0) ||
          (imgVersion < -9 && numBlocks >= 0)) {
        blocks = new BlockInfo[numBlocks];
        for (int j = 0; j < numBlocks; j++) {
          if (inodeType == INode.INodeType.RAIDED_INODE.type) {
            blocks[j] = new RaidBlockInfo(replication, j);
          } else {
            blocks[j] = new BlockInfo();
            blocks[j].setReplication(replication);
          }
          if (-14 < imgVersion) {
            blocks[j].set(in.readLong(), in.readLong(), 
                          Block.GRANDFATHER_GENERATION_STAMP);
          } else {
            blocks[j].readFields(in);
            if (LayoutVersion.supports(Feature.BLOCK_CHECKSUM, imgVersion)) {
              blocks[j].setChecksum(in.readInt());
            }
          }
        }
      }
      // Older versions of HDFS does not store the block size in inode.
      // If the file has more than one block, use the size of the 
      // first block as the blocksize. Otherwise use the default block size.
      //
      if (-8 <= imgVersion && blockSize == 0) {
        if (numBlocks > 1) {
          blockSize = blocks[0].getNumBytes();
        } else {
          long first = ((numBlocks == 1) ? blocks[0].getNumBytes(): 0);
          blockSize = Math.max(namesystem.getDefaultBlockSize(), first);
        }
      }
      
      // get quota only when the node is a directory
      long nsQuota = -1L;
      if (context.supportsNamespaceQuota
          && blocks == null) {
        nsQuota = in.readLong();
      }
      long dsQuota = -1L;
      if (context.supportsDiskspaceQuota
          && blocks == null) {
        dsQuota = in.readLong();
      }
  
      PermissionStatus permissions = namesystem.getUpgradePermission();
      if (imgVersion <= -11) {
        permissions = PermissionStatus.read(in);
      }
  
      INode newINode = INode.newINode(inodeId, permissions, blocks, replication,
          modificationTime, atime, nsQuota, dsQuota, blockSize, inodeType,
          hardLinkID, codec, context);
      namesystem.dir.addToInodeMap(newINode);
      return newINode;
    }

    private void loadDatanodes(DataInputStream in)
        throws IOException {
      int imgVersion = getLayoutVersion();

      if (imgVersion > -3) // pre datanode image version
        return;
      if (imgVersion <= -12) {
        return; // new versions do not store the datanodes any more.
      }
      int size = in.readInt();
      for(int i = 0; i < size; i++) {
        // We don't need to add these descriptors any more.
        FSImageSerialization.DatanodeImage.skipOne(in);
      }
    }

    private void loadFilesUnderConstruction(DataInputStream in)
    throws IOException {
      FSDirectory fsDir = namesystem.dir;
      int imgVersion = getLayoutVersion();
      if (imgVersion > -13) // pre lease image version
        return;
      int size = in.readInt();

      FLOG.info("Number of files under construction = " + size);

      for (int i = 0; i < size; i++) {
        INodeFileUnderConstruction cons =
          FSImageSerialization.readINodeUnderConstruction(in, fsDir, imgVersion);

        // verify that file exists in namespace
        String path = cons.getLocalName();
        INode old = fsDir.getFileINode(path);
        if (old == null) {
          throw new IOException("Found lease for non-existent file " + path);
        }
        if (old.isDirectory()) {
          throw new IOException("Found lease for directory " + path);
        }
        INodeFile oldnode = (INodeFile) old;
        fsDir.replaceNode(path, oldnode, cons);
        namesystem.leaseManager.addLease(cons.getClientName(), path,
            cons.getModificationTime()); 
      }
      FLOG.info("Loaded files under construction");
    }

    private int getLayoutVersion() {
      return storage.getLayoutVersion();
    }



    private boolean isRoot(byte[][] path) {
      return path.length == 1 &&
        path[0] == null;    
    }

    private boolean isParent(byte[][] path, byte[][] parent) {
      if (path == null || parent == null)
        return false;
      if (parent.length == 0 || path.length != parent.length + 1)
        return false;
      boolean isParent = true;
      for (int i = 0; i < parent.length; i++) {
        isParent = isParent && Arrays.equals(path[i], parent[i]); 
      }
      return isParent;
    }

    /**
     * Return string representing the parent of the given path.
     */
    String getParent(String path) {
      return path.substring(0, path.lastIndexOf(Path.SEPARATOR));
    }
    
    byte[][] getParent(byte[][] path) {
      byte[][] result = new byte[path.length - 1][];
      for (int i = 0; i < result.length; i++) {
        result[i] = new byte[path[i].length];
        System.arraycopy(path[i], 0, result[i], 0, path[i].length);
      }
      return result;
    }
  }
  
  /**
   * A one-shot class responsible for writing an image file.
   * The write() function should be called once, after which the getter
   * functions may be used to retrieve information about the file that was written.
   */
  static class Saver {
    private final SaveNamespaceContext context;
    /** Set to true once an image has been written */
    private boolean saved = false;
    
    /** The MD5 checksum of the file that was written */
    private MD5Hash savedDigest;

    static private final byte[] PATH_SEPARATOR = DFSUtil.string2Bytes(Path.SEPARATOR);

    /** @throws IllegalStateException if the instance has not yet saved an image */
    private void checkSaved() {
      if (!saved) {
        throw new IllegalStateException("FSImageSaver has not saved an image");
      }
    }
    
    /** @throws IllegalStateException if the instance has already saved an image */
    private void checkNotSaved() {
      if (saved) {
        throw new IllegalStateException("FSImageSaver has already saved an image");
      }
    }
    

    Saver(SaveNamespaceContext context) {
      this.context = context;
    }

    /**
     * Return the MD5 checksum of the image file that was saved.
     */
    MD5Hash getSavedDigest() {
      checkSaved();
      return savedDigest;
    }

    void save(OutputStream fout,
              FSImageCompression compression,
              DataOutputStream out,
              String name)
      throws IOException {
      checkNotSaved();

      final FSNamesystem sourceNamesystem = context.getSourceNamesystem();
      FSDirectory fsDir = sourceNamesystem.dir;
      long startTime = now();
      //
      // Write out data
      //
      MessageDigest digester = MD5Hash.getDigester();
      DigestOutputStream fos = null;
      if (out == null) {
        // for snapshot we pass out directly
        fos = new DigestOutputStream(fout, digester);
        out = BufferedByteOutputStream.wrapOutputStream(fos,
            FSImage.LOAD_SAVE_BUFFER_SIZE, FSImage.LOAD_SAVE_CHUNK_SIZE);
      } 
      try {
        out.writeInt(FSConstants.LAYOUT_VERSION);
        // We use the non-locked version of getNamespaceInfo here since
        // the coordinating thread of saveNamespace already has read-locked
        // the namespace for us. If we attempt to take another readlock
        // from the actual saver thread, there's a potential of a
        // fairness-related deadlock. See the comments on HDFS-2223.
        //out.writeInt(sourceNamesystem.getNamespaceId());
        
        //We can access namespace id directly since the NS is locked above
        out.writeInt(fsDir.fsImage.getNamespaceID());
        out.writeLong(fsDir.rootDir.numItemsInTree());
        out.writeLong(sourceNamesystem.getGenerationStamp());
        out.writeLong(context.getTxId());
        out.writeLong(fsDir.getLastInodeId());

        // write compression info and set up compressed stream
        out = compression.writeHeaderAndWrapStream(out);
        FLOG.info("Saving image file " + name + " using " + compression);

        byte[] byteStore = new byte[4*FSConstants.MAX_PATH_LENGTH];
        ByteBuffer strbuf = ByteBuffer.wrap(byteStore);
        // save the root
        FSImageSerialization.saveINode2Image(fsDir.rootDir, out);
        // save the rest of the nodes
        saveImage(strbuf, fsDir.rootDir, out, fsDir.totalInodes());
        // save files under construction
        sourceNamesystem.saveFilesUnderConstruction(context, out);
        strbuf = null;
        out.flush();
        if(fout != null) {
          if (fout instanceof FileOutputStream) {
            ((FileOutputStream)fout).getChannel().force(true);
          }
        }
      } finally {
        out.close();
      }

      saved = true;
      // set md5 of the saved image
      savedDigest = new MD5Hash(digester.digest());

      FLOG.info("Image file: " + name + " saved in " + (now() - startTime)/1000 + " seconds.");
    }
    
    /**
     * Save file tree image starting from the given root.
     * This is a recursive procedure, which first saves all children of
     * a current directory and then moves inside the sub-directories.
     */
    private void saveImage(ByteBuffer currentDirName,
                                  INodeDirectory current,
                                  DataOutputStream out,
                                  long inodesTotal) throws IOException {
      long inodesProcessed = 
        saveImage(currentDirName, current, out, inodesTotal, 1);
      if (inodesTotal != inodesProcessed) {
        throw new IOException("NameNode corrupted: saved inodes = "
            + inodesProcessed + " expected inodes = " + inodesTotal);
      }
    }

    private long saveImage(ByteBuffer currentDirName,
                                  INodeDirectory current,
                                  DataOutputStream out,
                                  long inodesTotal,
                                  long inodesProcessed) throws IOException {
      // if cancellation received - cancel operation
      context.checkCancelled();
      List<INode> children = current.getChildrenRaw();
      if (children == null || children.isEmpty())  // empty directory
        return inodesProcessed;
      // print prefix (parent directory name)
      int prefixLen = currentDirName.position();
      if (prefixLen == 0) {  // root
        out.writeShort(PATH_SEPARATOR.length);
        out.write(PATH_SEPARATOR);
      } else {  // non-root directories
        out.writeShort(prefixLen);
        out.write(currentDirName.array(), 0, prefixLen);
      }
      // print all children first
      out.writeInt(children.size());
      int percentDone = (int)(inodesProcessed * 100 / inodesTotal);
      for(INode child : children) {
        percentDone = printProgress(++inodesProcessed, inodesTotal, percentDone, "Saved");
        FSImageSerialization.saveINode2Image(child, out);
      }
      // print sub-directories
      for(INode child : children) {
        if(!child.isDirectory())
          continue;
        currentDirName.put(PATH_SEPARATOR).put(child.getLocalNameBytes());
        inodesProcessed = saveImage(currentDirName, (INodeDirectory)child, out, inodesTotal, 
            inodesProcessed);
        currentDirName.position(prefixLen);
      }
      return inodesProcessed;
    }
  }
  
  private static int printProgress(long numOfFilesProcessed, long totalFiles, int percentDone) {
    return printProgress(numOfFilesProcessed, totalFiles, percentDone, "Loaded");
  }

  private static int printProgress(long numOfFilesProcessed, long totalFiles, int percentDone, 
      String message) {
    int newPercentDone = (int)(numOfFilesProcessed * 100 / totalFiles);
    if  (newPercentDone > percentDone) {
      FLOG.info(message + " " + newPercentDone + "% of the image");
    }
    return newPercentDone;
  }
  
  static class FSImageLoadingContext {
    Map<Long, HardLinkFileInfo> hardLinkIDToFileInfoMap = new HashMap<Long, HardLinkFileInfo>();
    final FSDirectory dir;
    
    FSImageLoadingContext(FSDirectory dir) {
      this.dir = dir;
    }
    
    FSDirectory getFSDirectory() {
      return this.dir;
    }
    
    HardLinkFileInfo getHardLinkFileInfo(Long hardLinkID) {
      return hardLinkIDToFileInfoMap.get(hardLinkID);
    }
    
    void associateHardLinkIDWithFileInfo(Long hardLinkID, HardLinkFileInfo fileInfo) {
      hardLinkIDToFileInfoMap.put(hardLinkID, fileInfo);
    }
    
    // to avoid repeated calls to LayoutVersion.supports
    // we cache this in the context
    boolean supportsNamespaceQuota = false;
    boolean supportsDiskspaceQuota = false;
    boolean supportsFileAccessTime = false;
    boolean supportsHardlink = false;
    boolean supportsAddInodeId = false;
    boolean supportsRaid = false;
    
  }
}
