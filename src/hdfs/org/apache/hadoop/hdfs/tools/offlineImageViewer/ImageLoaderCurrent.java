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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor.ImageElement;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.BufferedByteInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.InjectionHandler;

/**
 * ImageLoaderCurrent processes Hadoop FSImage files and walks over
 * them using a provided ImageVisitor, calling the visitor at each element
 * enumerated below.
 *
 * The only difference between v18 and v19 was the utilization of the
 * stickybit.  Therefore, the same viewer can reader either format.
 *
 * fsimage layout (with changes from -16 up):
 * Image version (int)
 * Namepsace ID (int)
 * NumFiles (long)
 * Generation stamp (long)
 * Last Transaction ID (long) // added in -37
 * Last INode ID (long) // added in -42
 * INodes (count = NumFiles)
 *  INode
 *    Path (String)
 *    INode ID (long) // added in -42
 *    Replication (short)
 *    Modification Time (long as date)
 *    Access Time (long) // added in -16
 *    Block size (long)
 *    Num blocks (int)
 *    Blocks (count = Num blocks)
 *      Block
 *        Block ID (long)
 *        Num bytes (long)
 *        Generation stamp (long)
 *        Block checksum (int) // added in -43
 *    Namespace Quota (long)
 *    Diskspace Quota (long) // added in -18
 *    Permissions
 *      Username (String)
 *      Groupname (String)
 *      OctalPerms (short -> String)  // Modified in -19
 *    Symlink (String) // added in -23
 * NumINodesUnderConstruction (int)
 * INodesUnderConstruction (count = NumINodesUnderConstruction)
 *  INodeUnderConstruction
 *    Path (bytes as string)
 *    INode ID (long) // added in -42
 *    Replication (short)
 *    Modification time (long as date)
 *    Preferred block size (long)
 *    Num blocks (int)
 *    Blocks
 *      Block
 *        Block ID (long)
 *        Num bytes (long)
 *        Generation stamp (long)
 *        Block checksum (int)
 *    Permissions
 *      Username (String)
 *      Groupname (String)
 *      OctalPerms (short -> String)
 *    Client Name (String)
 *    Client Machine (String)
 *    NumLocations (int)
 *    DatanodeDescriptors (count = numLocations) // not loaded into memory
 *      short                                    // but still in file
 *      long
 *      string
 *      long
 *      int
 *      string
 *      string
 *      enum
 *
 */
class ImageLoaderCurrent implements ImageLoader {
  static final int BASE_BUFFER_SIZE = 512 * 1024; // 512KB
  private static final Date tempDate = new Date(0);
  protected final static DateFormat dateFormat = 
                                      new SimpleDateFormat("yyyy-MM-dd HH:mm");
  private static int[] versions = { -16, -17, -18, -19, -20, -21, -22, -23,
      -24, -25, -26, -27, -28, -30, -31, -32, -33, -34, -35, -36, -37, -38,
      -39, -40, -41, -42, -43, -44 };
  private int imageVersion = 0;

  /* (non-Javadoc)
   * @see ImageLoader#canProcessVersion(int)
   */
  @Override
  public boolean canLoadVersion(int version) {
    for(int v : versions)
      if(v == version) return true;

    return false;
  }

  /* (non-Javadoc)
   * @see ImageLoader#processImage(java.io.DataInputStream, ImageVisitor, boolean)
   */
  @Override
  public void loadImage(DataInputStream in, ImageVisitor v,
      boolean skipBlocks) throws IOException {
    try {
      InjectionHandler.processEvent(InjectionEvent.IMAGE_LOADER_CURRENT_START);
      v.start();
      v.visitEnclosingElement(ImageElement.FS_IMAGE);

      imageVersion = in.readInt();
      if( !canLoadVersion(imageVersion))
        throw new IOException("Cannot process fslayout version " + imageVersion);

      v.visit(ImageElement.IMAGE_VERSION, imageVersion);
      v.visit(ImageElement.NAMESPACE_ID, in.readInt());

      long numInodes = in.readLong();
      v.setNumberOfFiles(numInodes);

      v.visit(ImageElement.GENERATION_STAMP, in.readLong());
      if (imageVersion <= FSConstants.STORED_TXIDS) {
        v.visit(ImageElement.LAST_TXID, in.readLong());
      }
      
      if (LayoutVersion.supports(Feature.ADD_INODE_ID, imageVersion)) {
        v.visit(ImageElement.LAST_INODE_ID, in.readLong());
      }

      if (LayoutVersion.supports(Feature.FSIMAGE_COMPRESSION, imageVersion)) {
        boolean isCompressed = in.readBoolean();
        v.visit(ImageElement.IS_COMPRESSED, imageVersion);
        if (isCompressed) {
          String codecClassName = Text.readString(in);
          v.visit(ImageElement.COMPRESS_CODEC, codecClassName);
          CompressionCodecFactory codecFac = new CompressionCodecFactory(
              new Configuration());
          CompressionCodec codec = codecFac.getCodecByClassName(codecClassName);
          if (codec == null) {
            throw new IOException("Image compression codec not supported: "
                + codecClassName);
          }
          in = new DataInputStream(codec.createInputStream(in));
        }
      }
      in = BufferedByteInputStream.wrapInputStream(in, 8 * BASE_BUFFER_SIZE, BASE_BUFFER_SIZE);
      processINodes(in, v, numInodes, skipBlocks);

      processINodesUC(in, v, skipBlocks);

      v.leaveEnclosingElement(); // FSImage
      v.finish();
    } catch(IOException e) {
      // Tell the visitor to clean up, then re-throw the exception
      v.finishAbnormally();
      throw e;
    }
  }

  /**
   * Process the INodes under construction section of the fsimage.
   *
   * @param in DataInputStream to process
   * @param v Visitor to walk over inodes
   * @param skipBlocks Walk over each block?
   */
  private void processINodesUC(DataInputStream in, ImageVisitor v,
      boolean skipBlocks) throws IOException {
    int numINUC = in.readInt();

    v.visitEnclosingElement(ImageElement.INODES_UNDER_CONSTRUCTION,
                           ImageElement.NUM_INODES_UNDER_CONSTRUCTION, numINUC);

    for(int i = 0; i < numINUC; i++) {
      checkInterruption();
      v.visitEnclosingElement(ImageElement.INODE_UNDER_CONSTRUCTION);
      
      byte [] name = FSImageSerialization.readBytes(in);
      String n = new String(name, "UTF8");
      v.visit(ImageElement.INODE_PATH, n);
      
      if (LayoutVersion.supports(Feature.ADD_INODE_ID, imageVersion)) {
        v.visit(ImageElement.INODE_ID, in.readLong());
      }
      
      v.visit(ImageElement.REPLICATION, in.readShort());
      v.visit(ImageElement.MODIFICATION_TIME, formatDate(in.readLong()));

      v.visit(ImageElement.PREFERRED_BLOCK_SIZE, in.readLong());
      int numBlocks = in.readInt();
      processBlocks(in, v, numBlocks, skipBlocks);

      processPermission(in, v);
      v.visit(ImageElement.CLIENT_NAME, FSImageSerialization.readString(in));
      v.visit(ImageElement.CLIENT_MACHINE, FSImageSerialization.readString(in));

      // Skip over the datanode descriptors, which are still stored in the
      // file but are not used by the datanode or loaded into memory
      int numLocs = in.readInt();
      for(int j = 0; j < numLocs; j++) {
        in.readShort();
        in.readLong();
        in.readLong();
        in.readLong();
        in.readInt();
        FSImageSerialization.readString(in);
        FSImageSerialization.readString(in);
        WritableUtils.readEnum(in, AdminStates.class);
      }

      v.leaveEnclosingElement(); // INodeUnderConstruction
    }

    v.leaveEnclosingElement(); // INodesUnderConstruction
  }

  /**
   * Process the blocks section of the fsimage.
   *
   * @param in Datastream to process
   * @param v Visitor to walk over inodes
   * @param skipBlocks Walk over each block?
   */
  private void processBlocks(DataInputStream in, ImageVisitor v,
      int numBlocks, boolean skipBlocks) throws IOException {
    v.visitEnclosingElement(ImageElement.BLOCKS,
                            ImageElement.NUM_BLOCKS, numBlocks);
    
    // directory or symlink, no blocks to process    
    if(numBlocks == -1 || numBlocks == -2) { 
      v.leaveEnclosingElement(); // Blocks
      return;
    }
    
    if(skipBlocks) {
      int fieldsBytes = Long.SIZE * 3;
      if (LayoutVersion.supports(Feature.BLOCK_CHECKSUM, imageVersion)) {
        // For block checksum
        fieldsBytes += Integer.SIZE;
      }
      
      int bytesToSkip = ((fieldsBytes /* fields */) / 8 /*bits*/) * numBlocks;
      if(in.skipBytes(bytesToSkip) != bytesToSkip)
        throw new IOException("Error skipping over blocks");
      
    } else {
      for(int j = 0; j < numBlocks; j++) {
        v.visitEnclosingElement(ImageElement.BLOCK);
        v.visit(ImageElement.BLOCK_ID, in.readLong());
        v.visit(ImageElement.NUM_BYTES, in.readLong());
        v.visit(ImageElement.GENERATION_STAMP, in.readLong());
        if (LayoutVersion.supports(Feature.BLOCK_CHECKSUM, imageVersion)) {
          v.visit(ImageElement.BLOCK_CHECKSUM, in.readInt());
        }
        v.leaveEnclosingElement(); // Block
      }
    }
    v.leaveEnclosingElement(); // Blocks
  }

  /**
   * Extract the INode permissions stored in the fsimage file.
   *
   * @param in Datastream to process
   * @param v Visitor to walk over inodes
   */
  private void processPermission(DataInputStream in, ImageVisitor v)
      throws IOException {
    v.visitEnclosingElement(ImageElement.PERMISSIONS);
    v.visit(ImageElement.USER_NAME, Text.readStringOpt(in));
    v.visit(ImageElement.GROUP_NAME, Text.readStringOpt(in));
    FsPermission fsp = new FsPermission(in.readShort());
    v.visit(ImageElement.PERMISSION_STRING, fsp.toString());
    v.leaveEnclosingElement(); // Permissions
  }

  /**
   * Process the INode records stored in the fsimage.
   *
   * @param in Datastream to process
   * @param v Visitor to walk over INodes
   * @param numInodes Number of INodes stored in file
   * @param skipBlocks Process all the blocks within the INode?
   * @throws VisitException
   * @throws IOException
   */
  private void processINodes(DataInputStream in, ImageVisitor v,
      long numInodes, boolean skipBlocks) throws IOException {
    v.visitEnclosingElement(ImageElement.INODES,
        ImageElement.NUM_INODES, numInodes);
    
    if (LayoutVersion.supports(Feature.FSIMAGE_NAME_OPTIMIZATION, imageVersion)) {
      processLocalNameINodes(in, v, numInodes, skipBlocks);
    } else { // full path name
      processFullNameINodes(in, v, numInodes, skipBlocks);
    }

    
    v.leaveEnclosingElement(); // INodes
  }
  
  /**
   * Process image with full path name
   * 
   * @param in image stream
   * @param v visitor
   * @param numInodes number of indoes to read
   * @param skipBlocks skip blocks or not
   * @throws IOException if there is any error occurs
   */
  private void processLocalNameINodes(DataInputStream in, ImageVisitor v,
      long numInodes, boolean skipBlocks) throws IOException {
    // process root
    processINode(in, v, skipBlocks, "");
    numInodes--;
    while (numInodes > 0) {
      numInodes -= processDirectory(in, v, skipBlocks);
    }
  }
  
  private int processDirectory(DataInputStream in, ImageVisitor v,
     boolean skipBlocks) throws IOException {
    String parentName = FSImageSerialization.readString(in);
    int numChildren = in.readInt();
    for (int i=0; i<numChildren; i++) {
      processINode(in, v, skipBlocks, parentName);
    }
    return numChildren;
  }
  
   /**
    * Process image with full path name
    * 
    * @param in image stream
    * @param v visitor
    * @param numInodes number of indoes to read
    * @param skipBlocks skip blocks or not
    * @throws IOException if there is any error occurs
    */
   private void processFullNameINodes(DataInputStream in, ImageVisitor v,
       long numInodes, boolean skipBlocks) throws IOException {
     for(long i = 0; i < numInodes; i++) {
       processINode(in, v, skipBlocks, null);
     }
   }
   
   /**
    * Process an INode
    * 
    * @param in image stream
    * @param v visitor
    * @param skipBlocks skip blocks or not
    * @param parentName the name of its parent node
    * @throws IOException
    */
  private void processINode(DataInputStream in, ImageVisitor v,
      boolean skipBlocks, String parentName) throws IOException {
    checkInterruption();
    v.visitEnclosingElement(ImageElement.INODE);

    String pathName = FSImageSerialization.readString(in);
    if (parentName != null) {  // local name
      pathName = "/" + pathName;
      if (!"/".equals(parentName)) { // children of non-root directory
        pathName = parentName + pathName;
      }
    }

    v.visit(ImageElement.INODE_PATH, pathName);
    
    
    if (LayoutVersion.supports(Feature.ADD_INODE_ID, imageVersion)) {
      v.visit(ImageElement.INODE_ID, in.readLong());
    }
    
    if (LayoutVersion.supports(Feature.HARDLINK, imageVersion)) {
      byte inodeType = in.readByte();
      if (inodeType == INode.INodeType.HARDLINKED_INODE.type) {
        v.visit(ImageElement.INODE_TYPE, INode.INodeType.HARDLINKED_INODE.toString());
        long hardlinkID =  WritableUtils.readVLong(in);
        v.visit(ImageElement.INODE_HARDLINK_ID, hardlinkID);
      } else if (inodeType == INode.INodeType.RAIDED_INODE.type) {
        v.visit(ImageElement.INODE_TYPE, INode.INodeType.RAIDED_INODE.toString());
        String codecId = WritableUtils.readString(in);
        v.visit(ImageElement.RAID_CODEC_ID, codecId);
      } else {
        v.visit(ImageElement.INODE_TYPE, INode.INodeType.REGULAR_INODE.toString());
      }
    }
    
    v.visit(ImageElement.REPLICATION, in.readShort());
    v.visit(ImageElement.MODIFICATION_TIME, in.readLong());
    if(LayoutVersion.supports(Feature.FILE_ACCESS_TIME, imageVersion))
      v.visit(ImageElement.ACCESS_TIME, in.readLong());
    v.visit(ImageElement.BLOCK_SIZE, in.readLong());
    int numBlocks = in.readInt();

    processBlocks(in, v, numBlocks, skipBlocks);

    // File or directory
    if (numBlocks > 0 || numBlocks == -1) {
      v.visit(ImageElement.NS_QUOTA, numBlocks == -1 ? in.readLong() : -1);
      if (LayoutVersion.supports(Feature.DISKSPACE_QUOTA, imageVersion))
        v.visit(ImageElement.DS_QUOTA, numBlocks == -1 ? in.readLong() : -1);
    }
    if (numBlocks == -2) {
      v.visit(ImageElement.SYMLINK, Text.readString(in));
    }

    processPermission(in, v);
    v.leaveEnclosingElement(); // INode
  }

  /**
   * Helper method to format dates during processing.
   * @param date Date as read from image file
   * @return String version of date format
   */
  static String formatDate(long date) {
    tempDate.setTime(date);
    return dateFormat.format(tempDate);
  }
  
  private void checkInterruption() throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      InjectionHandler.processEvent(InjectionEvent.IMAGE_LOADER_CURRENT_INTERRUPT);
      throw new InterruptedIOException("Image loader interrupted");
    }
  }
}
