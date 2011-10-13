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

package org.apache.hadoop.hdfs;

import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.Comparator;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;

public class DFSUtil {
  /**
   * Compartor for sorting DataNodeInfo[] based on decommissioned states.
   * Decommissioned nodes are moved to the end of the array on sorting with
   * this compartor.
   */
  public static final Comparator<DatanodeInfo> DECOM_COMPARATOR = 
    new Comparator<DatanodeInfo>() {
    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      return a.isDecommissioned() == b.isDecommissioned() ? 0 :
        a.isDecommissioned() ? 1 : -1;
    }
  };
  
  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2String(byte[][] pathComponents) {
    if (pathComponents.length == 0)
      return "";
    if (pathComponents.length == 1 && pathComponents[0].length == 0) {
      return Path.SEPARATOR;
    }
    try {
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < pathComponents.length; i++) {
        result.append(new String(pathComponents[i], "UTF-8"));
        if (i < pathComponents.length - 1) {
          result.append(Path.SEPARATOR_CHAR);
        }
      }
      return result.toString();
    } catch (UnsupportedEncodingException ex) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Splits the array of bytes into array of arrays of bytes on byte separator
   * 
   * @param bytes
   *          the array of bytes to split
   * @param separator
   *          the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes, byte separator) {
    return bytes2byteArray(bytes, bytes.length, separator);
  }

   /**
    * Converts a byte array to a string using UTF8 encoding.
    */
   public static String bytes2String(byte[] bytes) {
     try {
       return new String(bytes, "UTF8");
     } catch(UnsupportedEncodingException e) {
       assert false : "UTF8 encoding is not supported ";
     }
     return null;
   }

   /**
    * Converts a string to a byte array using UTF8 encoding.
    */
   public static byte[] string2Bytes(String str) {
     try {
       return str.getBytes("UTF8");
     } catch(UnsupportedEncodingException e) {
       assert false : "UTF8 encoding is not supported ";
     }
     return null;
   }

  /**
   * Splits first len bytes in bytes to array of arrays of bytes on byte
   * separator
   * 
   * @param bytes
   *          the byte array to split
   * @param len
   *          the number of bytes to split
   * @param separator
   *          the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes, int len, byte separator) {
    assert len <= bytes.length;
    int splits = 0;
    if (len == 0) {
      return new byte[][] { null };
    }
    // Count the splits. Omit multiple separators and the last one
    for (int i = 0; i < len; i++) {
      if (bytes[i] == separator) {
        splits++;
      }
    }
    int last = len - 1;
    while (last > -1 && bytes[last--] == separator) {
      splits--;
    }
    if (splits == 0 && bytes[0] == separator) {
      return new byte[][] { null };
    }
    splits++;
    byte[][] result = new byte[splits][];
    int startIndex = 0;
    int nextIndex = 0;
    int index = 0;
    // Build the splits
    while (index < splits) {
      while (nextIndex < len && bytes[nextIndex] != separator) {
        nextIndex++;
      }
      result[index] = new byte[nextIndex - startIndex];
      System.arraycopy(bytes, startIndex, result[index], 0, nextIndex
          - startIndex);
      index++;
      startIndex = nextIndex + 1;
      nextIndex = startIndex;
    }
    return result;
  }

  /**
   * Convert a LocatedBlocks to BlockLocations[]
   * @param blocks a LocatedBlocks
   * @return an array of BlockLocations
   */
  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.locatedBlockCount();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    if (nrBlocks == 0) {
      return blkLocations;
    }
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] names = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        names[hCnt] = locations[hCnt].getName();
        NodeBase node = new NodeBase(names[hCnt],
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] = new BlockLocation(names, hosts, racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize(),
                                            blk.isCorrupt());
      idx++;
    }
    return blkLocations;
  }

  /**
   * @return all corrupt files in dfs
   */
  public static String[] getCorruptFiles(DistributedFileSystem dfs)
    throws IOException {
    return getCorruptFiles(dfs, "/");
  }

  /**
   * @return all corrupt files in dfs under a path.
   */
  public static String[] getCorruptFiles(DistributedFileSystem dfs, String path)
      throws IOException {
    Set<String> corruptFiles = new HashSet<String>();
    RemoteIterator<Path> cfb = dfs.listCorruptFileBlocks(new Path(path));
    while (cfb.hasNext()) {
      corruptFiles.add(cfb.next().toUri().getPath());
    }

    return corruptFiles.toArray(new String[corruptFiles.size()]);
  }
  
  /**
   * Check if it is a deleted block or not
   */
  private final static long DELETED = Long.MAX_VALUE - 1;
  public static boolean isDeleted(Block block) {
    return block.getNumBytes() == DELETED;
  }
  
  public static void markAsDeleted(Block block) {
    block.setNumBytes(DELETED);
  }
}

