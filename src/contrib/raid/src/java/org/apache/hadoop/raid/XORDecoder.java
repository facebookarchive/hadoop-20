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

import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class XORDecoder extends Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.XORDecoder");

  public XORDecoder(
    Configuration conf, int stripeSize) {
    super(conf, stripeSize, 1);
  }

  @Override
  protected void fixErasedBlockImpl(
      FileSystem fs, Path srcFile, FileSystem parityFs, Path parityFile,
      long blockSize, long errorOffset, long limit,
      OutputStream out, Progressable reporter) throws IOException {
    LOG.info("Fixing block at " + srcFile + ":" + errorOffset +
             ", limit " + limit);
    FileStatus srcStat = fs.getFileStatus(srcFile);
    FSDataInputStream[] inputs = new FSDataInputStream[stripeSize + paritySize];

    try {
      long errorBlockOffset = (errorOffset / blockSize) * blockSize;
      long[] srcOffsets = stripeOffsets(errorOffset, blockSize);
      for (int i = 0; i < srcOffsets.length; i++) {
        if (srcOffsets[i] == errorBlockOffset) {
          inputs[i] = new FSDataInputStream(
            new RaidUtils.ZeroInputStream(blockSize));
          LOG.info("Using zeros at " + srcFile + ":" + errorBlockOffset);
          continue;
        }
        if (srcOffsets[i] < srcStat.getLen()) {
          FSDataInputStream in = fs.open(srcFile);
          in.seek(srcOffsets[i]);
          inputs[i] = in;
        } else {
          inputs[i] = new FSDataInputStream(
            new RaidUtils.ZeroInputStream(blockSize));
          LOG.info("Using zeros at " + srcFile + ":" + errorBlockOffset);
        }
      }
      FSDataInputStream parityFileIn = parityFs.open(parityFile);
      parityFileIn.seek(parityOffset(errorOffset, blockSize));
      inputs[inputs.length - 1] = parityFileIn;
    } catch (IOException e) {
      RaidUtils.closeStreams(inputs);
      throw e;
    }

    int boundedBufferCapacity = 1;
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, inputs, bufSize, parallelism, boundedBufferCapacity, blockSize);
    parallelReader.start();
    try {
      // Loop while the number of skipped + written bytes is less than the max.
      for (long written = 0; written < limit; ) {
        ParallelStreamReader.ReadResult readResult;
        try {
          readResult = parallelReader.getReadResult();
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while waiting for read result");
        }
        // Cannot tolerate any IO errors.
        IOException readEx = readResult.getException();
        if (readEx != null) {
          throw readEx;
        }

        int toWrite = (int)Math.min((long)bufSize, limit - written);

        XOREncoder.xor(readResult.readBufs, writeBufs[0]);

        out.write(writeBufs[0], 0, toWrite);
        written += toWrite;
      }
    } finally {
      // Inputs will be closed by parallelReader.shutdown().
      parallelReader.shutdown();
    }
  }

  protected long[] stripeOffsets(long errorOffset, long blockSize) {
    long[] offsets = new long[stripeSize];
    long stripeIdx = errorOffset / (blockSize * stripeSize);
    long startOffsetOfStripe = stripeIdx * stripeSize * blockSize;
    for (int i = 0; i < stripeSize; i++) {
      offsets[i] = startOffsetOfStripe + i * blockSize;
    }
    return offsets;
  }

  protected long parityOffset(long errorOffset, long blockSize) {
    long stripeIdx = errorOffset / (blockSize * stripeSize);
    return stripeIdx * blockSize;
  }

}
