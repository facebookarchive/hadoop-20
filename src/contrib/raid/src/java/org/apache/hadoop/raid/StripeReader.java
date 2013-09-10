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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithFileName;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.StripeStore.StripeInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class StripeReader {
  public static final Log LOG = LogFactory.getLog(StripeReader.class);
  Codec codec;
  Configuration conf;
  FileSystem fs;
  long stripeStartIdx;
  long stripeEndIdx;
  int bufferSize;
  
  protected long currentStripeIdx;
  
  public static class LocationPair {
    private int stripeIdx;
    private int blockIdxInStripe;
    private List<FileStatus> lfs;
    public LocationPair(int stripeIdx, int blockIdxInStripe, 
        List<FileStatus> lfs) {
      this.stripeIdx = stripeIdx;
      this.blockIdxInStripe = blockIdxInStripe;
      this.lfs = lfs;
    }
    
    int getStripeIdx() {
      return stripeIdx;
    }
    
    int getBlockIdxInStripe() {
      return blockIdxInStripe;
    }
    
    List<FileStatus> getListFileStatus() {
      return lfs;
    }
  }
  
  public static class StripeInputInfo {
    private final InputStream[] inputs;
    private final Path[] srcPaths;
    private final long[] offSets;
    
    public StripeInputInfo(InputStream[] inputs, 
                           Path[] srcPaths, 
                           long[] offSets) {
      this.inputs = inputs;
      this.srcPaths = srcPaths;
      this.offSets = offSets;
    }
    
    InputStream[] getInputs() {
      return inputs;
    }
    
    Path[] getSrcPaths() {
      return srcPaths;
    }
    
    long[] getBlockOffsets() {
      return offSets;
    }
  }
  
  public StripeReader(Configuration conf, Codec codec,
      FileSystem fs, long stripeStartIdx) {
    this.codec = codec;
    this.conf = conf;
    this.fs = fs;
    this.stripeStartIdx = stripeStartIdx;
    this.currentStripeIdx = stripeStartIdx;
    this.bufferSize = conf.getInt("io.file.buffer.size", 64 * 1024);
  }
  
  /**
   * get current Stripe index
   */
  public long getCurrentStripeIdx() {
    return this.currentStripeIdx;
  }

  /**
   * Has next stripe to read?
   */
  public boolean hasNext() {
    return currentStripeIdx < stripeEndIdx;
  }
  
  /**
   * Get the input streams for the next stripe
   */
  public StripeInputInfo getNextStripeInputs() throws IOException {
    StripeInputInfo stripeInputInfo = getStripeInputs(currentStripeIdx);
    currentStripeIdx ++;
    return stripeInputInfo;
  }
  
  public abstract StripeInputInfo getStripeInputs(long StripeIdx) 
      throws IOException; 
  
  /**
   * Get the index of a block in a file, according to the blockId.
   * 
   * Any better ways to do this?
   */
  private static int getBlockIdInFile(DistributedFileSystem srcFs, 
      Path srcPath, long blockId) throws IOException {
    FileStatus srcStat = srcFs.getFileStatus(srcPath);
    LocatedBlocks lbs =
        srcFs.getClient().getLocatedBlocks(srcPath.toUri().getPath(), 0, 
            srcStat.getLen());
    
    int i = 0;
    LOG.info("Look for block " + blockId + " in file " + srcPath);
    for (LocatedBlock lb: lbs.getLocatedBlocks()) {
      if (lb.getBlock().getBlockId() == blockId) {
        return i;
      }
      i++;
    }
    
    return -1;
  }
  
  /**
   * Build the InputStream arrays from the StripeInfo.
   * 
   * @return
   * @throws IOException
   */
  public static InputStream[] buildInputsFromStripeInfo(
      DistributedFileSystem srcFs, 
      FileStatus srcStat, Codec codec,
      StripeInfo si, long offsetInBlock, long limit, 
      List<Integer> erasedLocations, 
      Set<Integer> locationsToNotRead, ErasureCode code
      ) throws IOException{
    
    InputStream[] inputs = new InputStream[codec.stripeLength + 
                                           codec.parityLength];
    
    boolean redo = false;
    do {
      redo = false;
      locationsToNotRead.clear();
      List<Integer> locationsToRead = 
          code.locationsToReadForDecode(erasedLocations);
      
      for (int i = 0; i < inputs.length; i++) {
        boolean isErased = (erasedLocations.indexOf(i) != -1);
        boolean shouldRead = (locationsToRead.indexOf(i) != -1);
        try {
          InputStream stm = null;
          if (isErased || !shouldRead) {
            if (isErased) {
              LOG.info("Location " + i + " is erased, using zeros");
            } else {
              LOG.info("Location " + i + " need not be read, using zeros");
            }
            locationsToNotRead.add(i);
            stm = new RaidUtils.ZeroInputStream(limit);
          } else {
            long blockId;
            if (i < codec.parityLength) {
              blockId = si.parityBlocks.get(i).getBlockId();
            } else if ((i - codec.parityLength) < si.srcBlocks.size()) {
              blockId = si.srcBlocks.get(i - codec.parityLength).getBlockId();
            } else {
              LOG.info("Using zeros for location " + i);
              inputs[i] = new RaidUtils.ZeroInputStream(limit);
              continue;
            }
            LocatedBlockWithFileName lb = 
                srcFs.getClient().getBlockInfo(blockId);
            if (lb == null) {
              throw new BlockMissingException(String.valueOf(blockId), 
                "Location " + i + " can not be found. Block id: " + blockId, 0);
            } else {
              Path filePath = new Path(lb.getFileName());
              FileStatus stat = srcFs.getFileStatus(filePath);
              long blockSize = stat.getBlockSize();
              if (offsetInBlock > blockSize) {
                stm = new RaidUtils.ZeroInputStream(limit);
              } else {
                if (srcFs.exists(filePath)) {
                  long startOffset = 
                      getBlockIdInFile(srcFs, filePath, blockId) * blockSize;
                  long offset = startOffset + offsetInBlock;
                  LOG.info("Opening " + lb.getFileName() + ":" + offset +
                      " for location " + i);
                  FSDataInputStream is = srcFs.open(filePath);
                  is.seek(offset);
                  stm = is;
                } else {
                  LOG.info("Location " + i + ", File " + lb.getFileName() + 
                      " does not exist, using zeros");
                  locationsToNotRead.add(i);
                  stm = new RaidUtils.ZeroInputStream(limit);
                }
              }
            }
          }
          inputs[i] = stm;
        } catch (IOException e) {
          if (e instanceof BlockMissingException || 
              e instanceof ChecksumException) {
            erasedLocations.add(i);
            redo = true;
            RaidUtils.closeStreams(inputs);
            break;
          } else {
            throw e;
          }
        }
      } 
    } while (redo);
    assert(locationsToNotRead.size() == codec.parityLength);
    return inputs;
  }
  
  /**
   * Builds (codec.stripeLength + codec.parityLength) inputs given some erased locations.
   * Outputs:
   *  - the array of input streams @param inputs
   *  - the list of erased locations @param erasedLocations.
   *  - the list of locations that are not read @param locationsToNotRead.
   */
  public InputStream[] buildInputs(
    FileSystem srcFs, Path srcFile, FileStatus srcStat,
    FileSystem parityFs, Path parityFile, FileStatus parityStat,
    int stripeIdx, long offsetInBlock, List<Integer> erasedLocations,
    Set<Integer> locationsToNotRead, ErasureCode code)
      throws IOException {
    InputStream[] inputs = new InputStream[codec.stripeLength +
                                           codec.parityLength]; 
    boolean redo = false;
    do {
      locationsToNotRead.clear();
      List<Integer> locationsToRead =
        code.locationsToReadForDecode(erasedLocations);
      for (int i = 0; i < inputs.length; i++) {
        boolean isErased = (erasedLocations.indexOf(i) != -1);
        boolean shouldRead = (locationsToRead.indexOf(i) != -1);
        try {
          InputStream stm = null;
          if (isErased || !shouldRead) {
            if (isErased) {
              LOG.info("Location " + i + " is erased, using zeros");
            } else {
              LOG.info("Location " + i + " need not be read, using zeros");
            }
            locationsToNotRead.add(i);
            stm = new RaidUtils.ZeroInputStream(srcStat.getBlockSize() * (
              (i < codec.parityLength) ?
              stripeIdx * codec.parityLength + i :
              stripeIdx * codec.stripeLength + i - codec.parityLength));
          } else {
            stm = buildOneInput(i, offsetInBlock,
                                srcFs, srcFile, srcStat,
                                parityFs, parityFile, parityStat);
          }
          inputs[i] = stm;
        } catch (IOException e) {
          if (e instanceof BlockMissingException || e instanceof ChecksumException) {
            erasedLocations.add(i);
            redo = true;
            RaidUtils.closeStreams(inputs);
            break;
          } else {
            throw e;
          }
        }
      }
    } while (redo);
    assert(locationsToNotRead.size() == codec.parityLength);
    return inputs;
  }
  
  public static LocationPair getBlockLocation(Codec codec, FileSystem srcFs,
      Path srcFile, int blockIdxInFile, Configuration conf)
          throws IOException {
    return getBlockLocation(codec, srcFs, srcFile, blockIdxInFile, conf, null);
  }
  
  /**
   * Given a block in the file and specific codec, return the LocationPair
   * object which contains id of the stripe it belongs to and its
   * location in the stripe
   */
  public static LocationPair getBlockLocation(Codec codec, FileSystem srcFs,
      Path srcFile, int blockIdxInFile, Configuration conf, 
      List<FileStatus> lfs) throws IOException {
    int stripeIdx = 0; 
    int blockIdxInStripe = 0;
    int blockIdx = blockIdxInFile; 
    if (codec.isDirRaid) {
      Path parentPath = srcFile.getParent();
      if (lfs == null) {
        lfs = RaidNode.listDirectoryRaidFileStatus(conf, srcFs, parentPath);
      }
      if (lfs == null) {
        throw new IOException("Couldn't list files under " + parentPath);
      }
      int blockNum = 0;
      Path qSrcFile = srcFs.makeQualified(srcFile);
      for (FileStatus fsStat: lfs) {
        if (!fsStat.getPath().equals(qSrcFile)) {
          blockNum += RaidNode.getNumBlocks(fsStat);
        } else {
          blockNum += blockIdxInFile;
          break;
        }
      }
      blockIdx = blockNum;
    }
    stripeIdx = blockIdx / codec.stripeLength;
    blockIdxInStripe = blockIdx % codec.stripeLength; 
    return new LocationPair(stripeIdx, blockIdxInStripe, lfs);
  }
  
  public static LocationPair getParityBlockLocation(Codec codec, 
      final int blockIdxInFile) {
    
    int stripeIdx = blockIdxInFile / codec.parityLength;
    int blockIdxInStripe = blockIdxInFile % codec.parityLength;
    
    return new LocationPair(stripeIdx, blockIdxInStripe, null);
  }

  public static StripeReader getStripeReader(Codec codec, Configuration conf, 
      long blockSize, FileSystem fs, long stripeIdx, FileStatus srcStat)
          throws IOException {
    if (codec.isDirRaid) {
      Path srcDir = srcStat.isDir()? srcStat.getPath():
        srcStat.getPath().getParent();
      return new DirectoryStripeReader(conf, codec, fs, stripeIdx, -1L, srcDir,
          RaidNode.listDirectoryRaidFileStatus(conf, fs, srcDir)); 
    } else {
      return new FileStripeReader(conf, blockSize, codec, fs, stripeIdx, -1L, 
          srcStat.getPath(), srcStat.getLen());
    }
  }
  
  protected abstract InputStream buildOneInput(
      int locationIndex, long offsetInBlock,
      FileSystem srcFs, Path srcFile, FileStatus srcStat,
      FileSystem parityFs, Path parityFile, FileStatus parityStat
      ) throws IOException;
  
  protected InputStream getParityFileInput(int locationIndex, Path parityFile,
      FileSystem parityFs, FileStatus parityStat, long offsetInBlock,
      long parityBlockSize)
          throws IOException {
    // Dealing with a parity file here.
    int parityBlockIdx = (int)(codec.parityLength * stripeStartIdx + locationIndex);
    long offset = parityBlockSize * parityBlockIdx + offsetInBlock;
    assert(offset < parityStat.getLen());
    LOG.info("Opening " + parityFile + ":" + offset +
      " for location " + locationIndex);
    FSDataInputStream s = parityFs.open(
      parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
    s.seek(offset);
    return s;
  }
}
