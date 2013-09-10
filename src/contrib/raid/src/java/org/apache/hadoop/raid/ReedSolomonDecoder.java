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
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.raid.StripeStore.StripeInfo;
import org.apache.hadoop.util.Progressable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.zip.CRC32;

public class ReedSolomonDecoder extends Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.ReedSolomonDecoder");
  private ErasureCode[] reedSolomonCode;
  private long decodeTime;
  private long waitTime;
  ExecutorService parallelDecoder;
  Semaphore decodeOps;
  private int stripeSize;
  private int paritySize;

  public ReedSolomonDecoder(
    Configuration conf) {
    super(conf, Codec.getCodec("rs"));
    this.reedSolomonCode = new ReedSolomonCode[parallelism];
    stripeSize = this.codec.stripeLength;
    paritySize = this.codec.parityLength;
    for (int i = 0; i < parallelism; i++) {
      reedSolomonCode[i] = new ReedSolomonCode(stripeSize, paritySize);
    }
    decodeOps = new Semaphore(parallelism);
  }

  @Override
  protected long fixErasedBlockImpl(
      FileSystem fs, Path srcFile,
      FileSystem parityFs, Path parityFile, boolean fixSource,
       long blockSize, long errorOffset, long limit,  boolean
       partial, OutputStream out, Context context, CRC32 crc,
       StripeInfo si, boolean recoverFromStripeStore, Block lostBlock)
           throws IOException {
    
    Progressable reporter = context;
    if (reporter == null) {
      reporter = RaidUtils.NULL_PROGRESSABLE;
    }
    
    if (partial) {
      throw new IOException ("We don't support partial reconstruction");
    }
    
    long start = System.currentTimeMillis();
    FSDataInputStream[] inputs = new FSDataInputStream[stripeSize + paritySize];
    int[] erasedLocations = buildInputs(fs, srcFile, parityFs, parityFile, 
                                        fixSource, errorOffset, inputs);
    int erasedLocationToFix;
    if (fixSource) {
      int blockIdxInStripe = ((int)(errorOffset/blockSize)) % stripeSize;
      erasedLocationToFix = paritySize + blockIdxInStripe;
    } else {
      // generate the idx for parity fixing.
      int blockIdxInStripe = ((int)(errorOffset/blockSize)) % paritySize;
      erasedLocationToFix = blockIdxInStripe;
    }

    // Allows network reads to go on while decode is going on.
    int boundedBufferCapacity = 2;
    ThreadFactory reedSolomonDecoderFactory = new ThreadFactoryBuilder()
      .setNameFormat("ReedSolomonDecoder-%d")
      .build();
    parallelDecoder = Executors.newFixedThreadPool(parallelism, reedSolomonDecoderFactory);
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, inputs, bufSize, parallelism, boundedBufferCapacity, blockSize);
    parallelReader.start();
    decodeTime = 0;
    waitTime = 0;
    try {
      return writeFixedBlock(inputs, erasedLocations, erasedLocationToFix,
                      limit, out, reporter, parallelReader, crc);
    } finally {
      // Inputs will be closed by parallelReader.shutdown().
      parallelReader.shutdown();
      LOG.info("Time spent in read " + parallelReader.readTime +
        ", decode " + decodeTime + ", wait " + waitTime + ", delay " +
          (System.currentTimeMillis() - start));
      parallelDecoder.shutdownNow();
    }
  }

  protected int[] buildInputs(FileSystem fs, Path srcFile, 
                              FileSystem parityFs, Path parityFile,
                              boolean fixSource,
                              long errorOffset, FSDataInputStream[] inputs)
      throws IOException {
    LOG.info("Building inputs to recover block starting at " + errorOffset);
    try {
      FileStatus srcStat = fs.getFileStatus(srcFile);
      FileStatus parityStat = parityFs.getFileStatus(parityFile);
      long blockSize = srcStat.getBlockSize();
      long blockIdx = (int)(errorOffset / blockSize);
      long stripeIdx;
      if (fixSource) {
        stripeIdx = blockIdx / stripeSize;
      } else {
        stripeIdx = blockIdx / paritySize;
      }

      LOG.info("FileSize = " + srcStat.getLen() + ", blockSize = " + blockSize +
               ", blockIdx = " + blockIdx + ", stripeIdx = " + stripeIdx);
      ArrayList<Integer> erasedLocations = new ArrayList<Integer>();
      // First open streams to the parity blocks.
      for (int i = 0; i < paritySize; i++) {
        long offset = blockSize * (stripeIdx * paritySize + i);
        if ((!fixSource) && offset == errorOffset) {
          LOG.info(parityFile + ":" + offset + 
              " is known to have error, adding zeros as input " + i);
          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
                offset + blockSize));
          erasedLocations.add(i);
        } else if (offset > parityStat.getLen()) {
          LOG.info(parityFile + ":" + offset +
                   " is past file size, adding zeros as input " + i);
          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
              offset + blockSize));
        } else {
          FSDataInputStream in = parityFs.open(
              parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
          in.seek(offset);
          LOG.info("Adding " + parityFile + ":" + offset + " as input " + i);
          inputs[i] = in;
        }
      }
      // Now open streams to the data blocks.
      for (int i = paritySize; i < paritySize + stripeSize; i++) {
        long offset = blockSize * (stripeIdx * stripeSize + i - paritySize);
        if (fixSource && offset == errorOffset) {
          LOG.info(srcFile + ":" + offset +
              " is known to have error, adding zeros as input " + i);
          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
              offset + blockSize));
          erasedLocations.add(i);
        } else if (offset > srcStat.getLen()) {
          LOG.info(srcFile + ":" + offset +
                   " is past file size, adding zeros as input " + i);
          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
              offset + blockSize));
        } else {
          FSDataInputStream in = fs.open(
            srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
          in.seek(offset);
          LOG.info("Adding " + srcFile + ":" + offset + " as input " + i);
          inputs[i] = in;
        }
      }
      if (erasedLocations.size() > paritySize) {
        String msg = "Too many erased locations: " + erasedLocations.size();
        LOG.error(msg);
        throw new IOException(msg);
      }
      int[] locs = new int[erasedLocations.size()];
      for (int i = 0; i < locs.length; i++) {
        locs[i] = erasedLocations.get(i);
      }
      return locs;
    } catch (IOException e) {
      RaidUtils.closeStreams(inputs);
      throw e;
    }

  }

  /**
   * Decode the inputs provided and write to the output.
   * @param inputs array of inputs.
   * @param erasedLocations indexes in the inputs which are known to be erased.
   * @param erasedLocationToFix index in the inputs which needs to be fixed.
   * @param limit maximum number of bytes to be written.
   * @param out the output.
   * @return size of recovered bytes
   * @throws IOException
   */
  long writeFixedBlock(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          int erasedLocationToFix,
          long limit,
          OutputStream out,
          Progressable reporter,
          ParallelStreamReader parallelReader, CRC32 crc) throws IOException {

    LOG.info("Need to write " + limit +
             " bytes for erased location index " + erasedLocationToFix);
    if (crc != null) {
      crc.reset();
    }
    int[] tmp = new int[inputs.length];
    int[] decoded = new int[erasedLocations.length];
    // Loop while the number of written bytes is less than the max.
    long written; 
    for (written = 0; written < limit; ) {
      erasedLocations = readFromInputs(
        inputs, erasedLocations, limit, reporter, parallelReader);
      if (decoded.length != erasedLocations.length) {
        decoded = new int[erasedLocations.length];
      }

      int toWrite = (int)Math.min((long)bufSize, limit - written);
      int partSize = (int) Math.ceil(bufSize * 1.0 / parallelism);
      
      try {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < parallelism; i++) {
          decodeOps.acquire(1);
          int start = i * partSize;
          int count = Math.min(bufSize - start, partSize);
          parallelDecoder.execute(new DecodeOp(
            readBufs, writeBufs, start, count,
            erasedLocations, reedSolomonCode[i]));
        }
        decodeOps.acquire(parallelism);
        decodeOps.release(parallelism);
        decodeTime += (System.currentTimeMillis() - startTime);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for read result");
      }


      for (int i = 0; i < erasedLocations.length; i++) {
        if (erasedLocations[i] == erasedLocationToFix) {
          out.write(writeBufs[i], 0, toWrite);
          if (crc != null) {
            crc.update(writeBufs[i], 0, toWrite);
          }
          written += toWrite;
          break;
        }
      }
    }
    return written;
  }

  int[] readFromInputs(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          long limit,
          Progressable reporter,
          ParallelStreamReader parallelReader) throws IOException {
    ParallelStreamReader.ReadResult readResult;
    try {
      long start = System.currentTimeMillis();
      readResult = parallelReader.getReadResult();
      waitTime += (System.currentTimeMillis() - start);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for read result");
    }

    // Process io errors, we can tolerate upto paritySize errors.
    for (int i = 0; i < readResult.ioExceptions.length; i++) {
      IOException e = readResult.ioExceptions[i];
      if (e == null) {
        continue;
      }
      if (e instanceof BlockMissingException) {
        LOG.warn("Encountered BlockMissingException in stream " + i);
      } else if (e instanceof ChecksumException) {
        LOG.warn("Encountered ChecksumException in stream " + i);
      } else {
        throw e;
      }

      // Found a new erased location.
      if (erasedLocations.length == paritySize) {
        String msg = "Too many read errors";
        LOG.error(msg);
        throw new IOException(msg);
      }

      // Add this stream to the set of erased locations.
      int[] newErasedLocations = new int[erasedLocations.length + 1];
      for (int j = 0; j < erasedLocations.length; j++) {
        newErasedLocations[j] = erasedLocations[j];
      }
      newErasedLocations[newErasedLocations.length - 1] = i;
      erasedLocations = newErasedLocations;
    }
    readBufs = readResult.readBufs;
    return erasedLocations;
  }

  class DecodeOp implements Runnable {
    byte[][] readBufs;
    byte[][] writeBufs;
    int startIdx;
    int count;
    int[] erasedLocations;
    int[] tmpInput;
    int[] tmpOutput;
    ErasureCode rs;
    DecodeOp(byte[][] readBufs, byte[][] writeBufs,
             int startIdx, int count, int[] erasedLocations,
             ErasureCode rs) {
      this.readBufs = readBufs;
      this.writeBufs = writeBufs;
      this.startIdx = startIdx;
      this.count = count;
      this.erasedLocations = erasedLocations;
      this.tmpInput = new int[readBufs.length];
      this.tmpOutput = new int[erasedLocations.length];
      this.rs = rs;
    }

    public void run() {
      try {
        performDecode();
      } finally {
        decodeOps.release();
      }
    }

    private void performDecode() {
      for (int idx = startIdx; idx < startIdx + count; idx++) {
        for (int i = 0; i < tmpOutput.length; i++) {
          tmpOutput[i] = 0;
        }
        for (int i = 0; i < tmpInput.length; i++) {
          tmpInput[i] = readBufs[i][idx] & 0x000000FF;
        }
        rs.decode(tmpInput, erasedLocations, tmpOutput);
        for (int i = 0; i < tmpOutput.length; i++) {
          writeBufs[i][idx] = (byte)tmpOutput[i];
        }
      }
    }
  }
  
}
