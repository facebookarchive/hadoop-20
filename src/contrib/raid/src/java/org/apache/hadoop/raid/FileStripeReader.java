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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class FileStripeReader extends StripeReader {
  long srcSize;
  FileSystem fs;
  Path srcFile;
  long blockSize;
  public FileStripeReader(Configuration conf, long blockSize,
      Codec codec, FileSystem fs, long stripeStartIdx, long encodingUnit,
      Path srcFile, long srcSize) {
    super(conf, codec, fs, stripeStartIdx);
    this.blockSize = blockSize;
    this.fs = fs;
    this.srcFile = srcFile;
    this.srcSize = srcSize;
    long numBlocks = (long) Math.ceil(srcSize * 1.0 / blockSize); 
    long totalStripe = RaidNode.numStripes(numBlocks, codec.stripeLength);
    if (encodingUnit < 0) {
      encodingUnit = totalStripe - stripeStartIdx;
    }
    stripeEndIdx = Math.min(totalStripe, stripeStartIdx + encodingUnit);
  }

  @Override
  public StripeInputInfo getStripeInputs(long stripeIdx) throws IOException {
    
    InputStream[] blocks = new InputStream[codec.stripeLength];
    Path[] srcPaths = new Path[codec.stripeLength];
    long[] offsets = new long[codec.stripeLength];
    long stripeStartOffset = stripeIdx * codec.stripeLength * blockSize;
    try {
      for (int i = 0; i < codec.stripeLength; i++) {
        long seekOffset = stripeStartOffset + i * blockSize;
        if (seekOffset < srcSize) {
          FSDataInputStream in = fs.open(srcFile, bufferSize);
          in.seek(seekOffset);
          LOG.info("Opening stream at " + srcFile + ":" + seekOffset);
          blocks[i] = in;
          srcPaths[i] = srcFile;
          offsets[i] = seekOffset;
        } else {
          LOG.info("Using zeros at offset " + seekOffset);
          // We have no src data at this offset.
          blocks[i] = new RaidUtils.ZeroInputStream(
                            seekOffset + blockSize);
          srcPaths[i] = null;
          offsets[i] = -1;
        }
      }
      return new StripeInputInfo(blocks, srcPaths, offsets);
    } catch (IOException e) {
      // If there is an error during opening a stream, close the previously
      // opened streams and re-throw.
      RaidUtils.closeStreams(blocks);
      throw e;
    }
  }
  
  @Override
  public InputStream buildOneInput(
      int locationIndex, long offsetInBlock,
      FileSystem srcFs, Path srcFile, FileStatus srcStat,
      FileSystem parityFs, Path parityFile, FileStatus parityStat
      ) throws IOException {
    final long blockSize = srcStat.getBlockSize();

    LOG.info("buildOneInput srcfile " + srcFile + " srclen " + srcStat.getLen() + 
        " parityfile " + parityFile + " paritylen " + parityStat.getLen() +
        " stripeindex " + stripeStartIdx + " locationindex " + locationIndex +
        " offsetinblock " + offsetInBlock);
    if (locationIndex < codec.parityLength) {
      // we should use srcfile's block size because parity file could be hared
      return this.getParityFileInput(locationIndex, parityFile,
          parityFs, parityStat, offsetInBlock, blockSize);
    } else {
      // Dealing with a src file here.
      int blockIdxInStripe = locationIndex - codec.parityLength;
      int blockIdx = (int)(codec.stripeLength * stripeStartIdx + blockIdxInStripe);
      long offset = blockSize * blockIdx + offsetInBlock;
      if (offset >= srcStat.getLen()) {
        LOG.info("Using zeros for " + srcFile + ":" + offset +
          " for location " + locationIndex);
        return new RaidUtils.ZeroInputStream(blockSize * (blockIdx + 1));
      } else {
        LOG.info("Opening " + srcFile + ":" + offset +
                 " for location " + locationIndex);
        FSDataInputStream s = fs.open(
            srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
        s.seek(offset);
        return s;
      }
    }
  }
}
