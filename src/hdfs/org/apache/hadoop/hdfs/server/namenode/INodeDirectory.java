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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

/**
 * Directory INode class.
 */
class INodeDirectory extends INode {
  protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;
  protected static final int UNKNOWN_INDEX = -1;
  final static String ROOT_NAME = "";

  private List<INode> children;

  INodeDirectory(long id, String name, PermissionStatus permissions) {
    super(id, name, permissions);
    this.children = null;
  }

  public INodeDirectory(long id, PermissionStatus permissions, long mTime) {
    super(id, permissions, mTime, 0);
    this.children = null;
  }

  /** constructor */
  INodeDirectory(long id, byte[] localName, PermissionStatus permissions, long mTime) {
    this(id, permissions, mTime);
    this.name = localName;
  }
  
  /** copy constructor
   * 
   * @param other
   */
  INodeDirectory(INodeDirectory other) {
    super(other);
    this.children = other.getChildren();
  }
  
  /**
   * Check whether it's a directory
   */
  @Override
  public boolean isDirectory() {
    return true;
  }
  
  public void setChildrenCapacity(int size){  
    this.children = new ArrayList<INode>(size); 
  }

  INode removeChild(INode node) {
    assert children != null;
    int low = Collections.binarySearch(children, node.name);
    if (low >= 0) {
      return children.remove(low);
    } else {
      return null;
    }
  }
  
  /** 
   * Replace a child that has the same name as newChild by newChild. This is only working on one
   * child case
   * 
   * @param newChild Child node to be added
   */
  void replaceChild(INode newChild) {
    if ( children == null ) {
      throw new IllegalArgumentException("The directory is empty");
    }
    
    int low = Collections.binarySearch(children, newChild.name);
    if (low>=0) { // an old child exists so replace by the newChild
      INode oldChild = children.get(low);
      
      // Need to make sure we are replacing the oldChild with newChild that is the same as oldChild
      // which means they reference to the same children or they are null or empty array
      children.set(low, newChild);
      
      // newChild should point to current instance (parent of oldChild)
      newChild.parent = this;
      
      // if both are directory, all the children from oldChild should point to newChild
      if (newChild.isDirectory() && oldChild.isDirectory()) {
        if (((INodeDirectory)oldChild).getChildren() != null) {
          for (INode oldGrandChild : ((INodeDirectory)oldChild).getChildren()) {
            oldGrandChild.parent = (INodeDirectory)newChild;
          }
        }
      }
      
    } else {
      throw new IllegalArgumentException("No child exists to be replaced");
    }
  }

  INode getChild(String name) {
    return getChildINode(DFSUtil.string2Bytes(name));
  }

  INode getChildINode(byte[] name) {
    if (children == null) {
      return null;
    }
    int low = Collections.binarySearch(children, name);
    if (low >= 0) {
      return children.get(low);
    }
    return null;
  }

  /**
   */
  INode getNode(byte[][] components) {
    INode[] inode  = new INode[1];
    getExistingPathINodes(components, inode);
    return inode[0];
  }

  /**
   * This is the external interface
   */
  INode getNode(String path) {
    return getNode(getPathComponents(path));
  }
  
  INode getNode(byte[] path) {
    return getNode(getPathComponents(path));
  }

  /**
   * Retrieve existing INodes from a path. If existing is big enough to store
   * all path components (existing and non-existing), then existing INodes
   * will be stored starting from the root INode into existing[0]; if
   * existing is not big enough to store all path components, then only the
   * last existing and non existing INodes will be stored so that
   * existing[existing.length-1] refers to the target INode.
   * 
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"],
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?])</code> should fill the
   * array with [c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?])</code> should fill the
   * array with [null]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?])</code> should fill the
   * array with [c1,c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?])</code> should fill
   * the array with [c2,null]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?,?,?])</code> should fill
   * the array with [rootINode,c1,c2,null], <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?,?,?])</code> should
   * fill the array with [rootINode,c1,c2,null]
   * @param components array of path component name
   * @param existing INode array to fill with existing INodes
   * @return number of existing INodes in the path
   */
  int getExistingPathINodes(byte[][] components, INode[] existing) {
    assert compareTo(components[0]) == 0 :
      "Incorrect name " + getLocalName() + " expected " + components[0];

    INode curNode = this;
    int count = 0;
    int index = existing.length - components.length;
    if (index > 0)
      index = 0;
    while ((count < components.length) && (curNode != null)) {
      if (index >= 0)
        existing[index] = curNode;
      if (!curNode.isDirectory() || (count == components.length - 1))
        break; // no more child, stop here
      INodeDirectory parentDir = (INodeDirectory)curNode;
      curNode = parentDir.getChildINode(components[count + 1]);
      count += 1;
      index += 1;
    }
    return count;
  }

  /**
   * Retrieve the existing INodes along the given path. The first INode
   * always exist and is this INode.
   * 
   * @param path the path to explore
   * @return INodes array containing the existing INodes in the order they
   *         appear when following the path from the root INode to the
   *         deepest INodes. The array size will be the number of expected
   *         components in the path, and non existing components will be
   *         filled with null
   *         
   * @see #getExistingPathINodes(byte[][], INode[])
   */
  public INode[] getExistingPathINodes(String path) {
    byte[][] components = getPathComponents(path);
    INode[] inodes = new INode[components.length];

    this.getExistingPathINodes(components, inodes);
    
    return inodes;
  }

  /**
   * Add a child inode to the directory.
   * 
   * @param node INode to insert
   * @param inheritPermission inherit permission from parent?
   * @return  null if the child with this name already exists; 
   *          node, otherwise
   */
  <T extends INode> T addChild(final T node, boolean inheritPermission) {
    return addChild(node, inheritPermission, true, UNKNOWN_INDEX);
  }
  /**
   * Add a child inode to the directory.
   * 
   * @param node INode to insert
   * @param inheritPermission inherit permission from parent?
   * @param propagateModTime set parent's mod time to that of a child?
   * @param childIndex index of the inserted child if known
   * @return  null if the child with this name already exists; 
   *          node, otherwise
   */
  <T extends INode> T addChild(final T node, boolean inheritPermission,
                                              boolean propagateModTime,
                                              int childIndex) {
    if (inheritPermission) {
      FsPermission p = getFsPermission();
      //make sure the  permission has wx for the user
      if (!p.getUserAction().implies(FsAction.WRITE_EXECUTE)) {
        p = new FsPermission(p.getUserAction().or(FsAction.WRITE_EXECUTE),
            p.getGroupAction(), p.getOtherAction());
      }
      node.setPermission(p);
    }

    if (children == null) {
      children = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
    }
    int index;  
    if (childIndex >= 0) {
      index = childIndex; 
    } else {
      int low = Collections.binarySearch(children, node.name);
      if(low >= 0)
        return null;
      index = -low - 1;
    }
    node.parent = this;
    children.add(index, node);
    if (propagateModTime) {
      // update modification time of the parent directory
      setModificationTime(node.getModificationTime());      
    }
    if (childIndex < 0) {
      // if child Index is provided (>=0), this is a result of 
      // loading the image, and the group name is set, no need
      // to check
      if (node.getGroupName() == null) {
        node.setGroup(getGroupName());
      }
    } 
    return node;
  }

  /**
   * Search all children for the first child whose name is greater than
   * the given name.
   * 
   * If the given name is one of children's name, the next child's index
   * is returned; Otherwise, return the insertion point: the index of the 
   * first child whose name's greater than the given name.
   *
   * @param name a name
   * @return the index of the next child
   */
  int nextChild(byte[] name) {
    if (name.length == 0) { // empty name
      return 0;
    }
    int nextPos = Collections.binarySearch(children, name) + 1;
    if (nextPos >= 0) {  // the name is in the list of children
      return nextPos;
    }
    return -nextPos; // insert point
  }

  /**
   * Equivalent to addNode(path, newNode, false).
   * @see #addNode(String, INode, boolean)
   */
  <T extends INode> T addNode(String path, T newNode) throws FileNotFoundException {
    return addNode(path, newNode, false);
  }
  /**
   * Add new INode to the file tree.
   * Find the parent and insert 
   * 
   * @param path file path
   * @param newNode INode to be added
   * @param inheritPermission If true, copy the parent's permission to newNode.
   * @return null if the node already exists; inserted INode, otherwise
   * @throws FileNotFoundException if parent does not exist or 
   * is not a directory.
   */
  <T extends INode> T addNode(String path, T newNode, boolean inheritPermission
      ) throws FileNotFoundException {
    byte[][] pathComponents = getPathComponents(path);
    if(addToParent(pathComponents, newNode, inheritPermission, true) == null)
      return null;
    return newNode;
  }

  /**
   * Add new inode to the parent if specified.
   * Optimized version of addNode() if parent is not null.
   * 
   * @return  parent INode if new inode is inserted
   *          or null if it already exists.
   * @throws  FileNotFoundException if parent does not exist or 
   *          is not a directory.
   */
  INodeDirectory addToParent(byte[] localname,
                             INode newNode,
                             INodeDirectory parent,
                             boolean inheritPermission,
                             boolean propagateModTime,
                             int childIndex
                            ) throws FileNotFoundException {
    // insert into the parent children list
    newNode.name = localname;
    if(parent.addChild(newNode, inheritPermission, propagateModTime, childIndex) == null)
      return null;
    return parent;
  }

  INodeDirectory getParent(byte[][] pathComponents)
  throws FileNotFoundException {
    int pathLen = pathComponents.length;
    if (pathLen < 2)  // add root
      return null;
    // Gets the parent INode
    INode[] inodes  = new INode[2];
    getExistingPathINodes(pathComponents, inodes);
    INode inode = inodes[0];
    if (inode == null) {
      throw new FileNotFoundException("Parent path does not exist: "+
          DFSUtil.byteArray2String(pathComponents));
    }
    if (!inode.isDirectory()) {
      throw new FileNotFoundException("Parent path is not a directory: "+
          DFSUtil.byteArray2String(pathComponents));
    }
    return (INodeDirectory)inode;
  }
  
  <T extends INode> INodeDirectory addToParent(
                                      byte[][] pathComponents,
                                      T newNode,
                                      boolean inheritPermission,
                                      boolean propagateModTime
                                    ) throws FileNotFoundException {
  
    int pathLen = pathComponents.length;
    if (pathLen < 2)  // add root
      return null;
    newNode.name = pathComponents[pathLen-1];
    // insert into the parent children list
    INodeDirectory parent = getParent(pathComponents);
    if(parent.addChild(newNode, inheritPermission, propagateModTime, UNKNOWN_INDEX) == null)
      return null;
    return parent;
  }

  /** {@inheritDoc} */
  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    Set<Long> visitedCtx = new HashSet<Long>(); 
    return spaceConsumedInTree(counts, visitedCtx);
  }
  
  DirCounts spaceConsumedInTree(DirCounts counts, Set<Long> visitedCtx) {
    counts.nsCount += 1;
    if (children != null) {
      for (INode child : children) {
        if (child.isDirectory()) {
          // Process the directory with the visited hard link context
          ((INodeDirectory) child).spaceConsumedInTree(counts, visitedCtx);
        } else {
          // Process the file
          if (child instanceof INodeHardLinkFile) {
            // Get the current hard link ID
            long hardLinkID = ((INodeHardLinkFile) child).getHardLinkID();

            if (visitedCtx.contains(hardLinkID)) {
              // The current hard link file has been visited, so skip processing
              // But update the nsCount
              counts.nsCount++;
              continue;
            } else {
              // Add the current hard link file to the visited set
              visitedCtx.add(hardLinkID);
            }
          }
          // compute the current child
          child.spaceConsumedInTree(counts);
        }
      }
    }
    return counts;
  }

  /** {@inheritDoc} */  
  @Override
  long[] computeContentSummary(long[] summary) {  
    Set<Long> visitedCtx = new HashSet<Long>(); 
    return this.computeContentSummary(summary, visitedCtx); 
  } 
    
  /** 
   * Compute the content summary and skip calculating the visited hard link file. 
   */ 
  private long[] computeContentSummary(long[] summary, Set<Long> visitedCtx) {  
    if (children != null) { 
      for (INode child : children) {
        if (child.isDirectory()) {
          // Process the directory with the visited hard link context 
          ((INodeDirectory)child).computeContentSummary(summary, visitedCtx); 
        } else {  
          // Process the file 
          if (child instanceof INodeHardLinkFile) {
            // Get the current hard link ID 
            long hardLinkID = ((INodeHardLinkFile) child).getHardLinkID();  
            if (visitedCtx.contains(hardLinkID)) {
              // The current hard link file has been visited, so only increase the file count.
              summary[1] ++;
              continue; 
            } else {
              // Add the current hard link file to the visited set  
              visitedCtx.add(hardLinkID);
              // Compute the current hardlink file  
              child.computeContentSummary(summary); 
            } 
          } else {
            // compute the current child for non hard linked files
            child.computeContentSummary(summary); 
          }

        } 
      } 
    } 
    summary[2]++; 
    return summary; 
  }

  /**
   */
  List<INode> getChildren() {
    return children==null ? new ArrayList<INode>() : children;
  }
  
  List<INode> getChildrenRaw() {
    return children;
  }

  private static boolean isBlocksLimitReached(List<BlockInfo> v, int blocksLimit) {
    return blocksLimit != FSDirectory.BLOCK_DELETION_NO_LIMIT
        && blocksLimit <= v.size();
  }
  
  @Override
  int collectSubtreeBlocksAndClear(List<BlockInfo> v, 
                                   int blocksLimit, 
                                   List<INode> removedINodes) {
    if (isBlocksLimitReached(v, blocksLimit)) {
      return 0;
    }
    int total = 0;
    if (children == null) {
      parent = null;
      name = null;
      removedINodes.add(this);
      return ++total;
    }
    int i;
    for (i=0; i<children.size(); i++) {
      INode child = children.get(i);
      total += child.collectSubtreeBlocksAndClear(v, blocksLimit, removedINodes);
      if (isBlocksLimitReached(v, blocksLimit)) {
        // reached blocks limit
        if (child.parent != null) {
          i--; // this child has not finished yet
        }
        break;
      }
    }
    if (i<children.size()-1) { // partial children are processed
      // Remove children [0,i]
      children = children.subList(i+1, children.size());
      return total;
    }
    // all the children are processed
    parent = null;
    name = null;
    children = null;
    removedINodes.add(this);
    return ++total;
  }
  
  /**
   * Numbers of all blocks, files and directories under this directory.
   * Current directory will be counted as well.
   */
  public static class ItemCounts {
    int numBlocks;
    int numDirectories;
    int numFiles;
    long startTime;  // time stamp when counting started
    long finishTime; // time stamp when counting finished
  }
  
  private ItemCounts itemCounts = null;
  
  /**
   * Get item counts of the current directory. Need to do countItems() first
   * if you need updated item counts.
   * @return numbers of blocks, files and directories
   */
  public ItemCounts getItemCounts() {
    return itemCounts;
  }
  
  /**
   * Count items under the current directory
   */
  public void countItems() {
    itemCounts = new ItemCounts();
    itemCounts.startTime = System.currentTimeMillis();
    itemCounts.numDirectories = 1; // count the current directory
    itemCounts.numFiles = 0;
    itemCounts.numBlocks = 0;
    if (children != null) {
      for (INode child : children) {
        countItemsRecursively(child);
      }
    }
    itemCounts.finishTime = System.currentTimeMillis();
  }
  
  private void countItemsRecursively(INode curr) {
    if (curr == null) {
      return;
    }
    itemCounts.numDirectories++;
    if (curr instanceof INodeDirectory) {
      itemCounts.numDirectories++;
      if (((INodeDirectory) curr).children != null) {
        for (INode child : ((INodeDirectory) curr).children) {
          countItemsRecursively(child);
        }
      }
    } else {
      itemCounts.numFiles++;
      if (((INodeFile) curr).getBlocks() != null) {
        itemCounts.numBlocks += ((INodeFile) curr).getBlocks().length;
      }
    }
  }
}
