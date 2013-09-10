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

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.RaidCodec;


public abstract class RaidDFSUtil {
  public static final String[] codes = new String[] {"xor", "rs"};
  final static Log LOG = LogFactory.getLog(RaidDFSUtil.class);
  final static Random random = new Random();
  
  /**
   * This function create two mock source and parity files and merge them.
   */
  public static void constructFakeRaidFile(DistributedFileSystem dfs,
      String filePath, RaidCodec codec) throws IOException {
    long blockSize = 512L;
    byte[] buffer = new byte[(int) (codec.numDataBlocks * blockSize)];
    int[] checksum = new int[codec.numDataBlocks];
    
    OutputStream out = dfs.create(new Path(filePath), true, 1, 
        codec.parityReplication, blockSize);
    random.nextBytes(buffer);
    out.write(buffer);
    out.close();
    
    Path parityTmp = new Path(filePath + "_parity");
    buffer = new byte[(int) (codec.numParityBlocks * blockSize)];
    out = dfs.create(parityTmp, true, 1, 
        codec.parityReplication, blockSize);
    random.nextBytes(buffer);
    out.write(buffer);
    out.close();
    
    FileStatus stat = dfs.getFileStatus(new Path(filePath));
    dfs.setTimes(parityTmp, stat.getModificationTime(), stat.getAccessTime());
    
    dfs.merge(parityTmp, new Path(filePath), codec.id, checksum);
    
  }
  
  /**
   * Returns the corrupt blocks in a file.
   */
  public static List<LocatedBlock> corruptBlocksInFile(
    DistributedFileSystem dfs, String path, long offset, long length)
  throws IOException {
    List<LocatedBlock> corrupt = new LinkedList<LocatedBlock>();
    LocatedBlocks locatedBlocks =
      getBlockLocations(dfs, path, offset, length);
    for (LocatedBlock b: locatedBlocks.getLocatedBlocks()) {
      if (b.isCorrupt() ||
         (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
        corrupt.add(b);
      }
    }
    return corrupt;
  }

  public static LocatedBlocks getBlockLocations(
    DistributedFileSystem dfs, String path, long offset, long length)
    throws IOException {
    return dfs.getClient().namenode.getBlockLocations(path, offset, length);
  }
  
  public static long getCRC(FileSystem fs, Path p) throws IOException {
    CRC32 crc = new CRC32();
    FSDataInputStream stm = fs.open(p);
    int b;
    while ((b = stm.read())>=0) {
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }
  
  public static void cleanUp(FileSystem fileSys, Path dir) throws IOException {
    if (fileSys.exists(dir)) {
      fileSys.delete(dir, true);
    }
  }

  public static void reportCorruptBlocks(FileSystem fs, Path file, int[] idxs,
      long blockSize) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem)fs;
    LocatedBlocks lbs = dfs.getLocatedBlocks(file, 0L,
        fs.getFileStatus(file).getLen());
    LocatedBlock[] lbArray = new LocatedBlock[idxs.length];
    for (int i = 0; i < idxs.length; i++) {
      lbArray[i] = lbs.get(idxs[i]);
    }
    reportCorruptBlocksToNN(dfs, lbArray);
  }
  
  public static void reportCorruptBlocksToNN(DistributedFileSystem dfs,
      LocatedBlock[] blocks) throws IOException {
    dfs.getClient().namenode.reportBadBlocks(blocks);
  }
}
