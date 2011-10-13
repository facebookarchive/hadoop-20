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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

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
import org.apache.hadoop.util.Progressable;

/**
 * Represents a generic decoder that can be used to read a file with
 * corrupt blocks by using the parity file.
 * This is an abstract class, concrete subclasses need to implement
 * fixErasedBlockImpl.
 */
public abstract class Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.Decoder");
  public static final int DEFAULT_PARALLELISM = 4;
  protected Configuration conf;
  protected int parallelism;
  protected int stripeSize;
  protected int paritySize;
  protected Random rand;
  protected int bufSize;
  protected byte[][] readBufs;
  protected byte[][] writeBufs;

  Decoder(Configuration conf, int stripeSize, int paritySize) {
    this.conf = conf;
    this.parallelism = conf.getInt("raid.encoder.parallelism",
                                   DEFAULT_PARALLELISM);
    this.stripeSize = stripeSize;
    this.paritySize = paritySize;
    this.rand = new Random();
    this.bufSize = conf.getInt("raid.decoder.bufsize", 1024 * 1024);
    this.writeBufs = new byte[paritySize][];
    allocateBuffers();
  }

  private void allocateBuffers() {
    for (int i = 0; i < paritySize; i++) {
      writeBufs[i] = new byte[bufSize];
    }
  }

  private void configureBuffers(long blockSize) {
    if ((long)bufSize > blockSize) {
      bufSize = (int)blockSize;
      allocateBuffers();
    } else if (blockSize % bufSize != 0) {
      bufSize = (int)(blockSize / 256L); // heuristic.
      if (bufSize == 0) {
        bufSize = 1024;
      }
      bufSize = Math.min(bufSize, 1024 * 1024);
      allocateBuffers();
    }
  }

  /**
   * Recovers a corrupt block to local file.
   *
   * @param srcFs The filesystem containing the source file.
   * @param srcPath The damaged source file.
   * @param parityPath The filesystem containing the parity file. This could be
   *        different from fs in case the parity file is part of a HAR archive.
   * @param parityFile The parity file.
   * @param blockSize The block size of the file.
   * @param blockOffset Known location of error in the source file. There could
   *        be additional errors in the source file that are discovered during
   *        the decode process.
   * @param localBlockFile The file to write the block to.
   * @param limit The maximum number of bytes to be written out.
   *              This is to prevent writing beyond the end of the file.
   * @param reporter A mechanism to report progress.
   */
  public void recoverBlockToFile(
    FileSystem srcFs, Path srcPath, FileSystem parityFs, Path parityPath,
    long blockSize, long blockOffset, File localBlockFile, long limit,
    Progressable reporter)
      throws IOException {
    OutputStream out = new FileOutputStream(localBlockFile);
    fixErasedBlock(srcFs, srcPath, parityFs, parityPath,
                  blockSize, blockOffset, limit, out, reporter);
    out.close();
  }

  /**
   * Wraps around fixErasedBlockImpl in order to configure buffers.
   * Having buffers of the right size is extremely important. If the the
   * buffer size is not a divisor of the block size, we may end up reading
   * across block boundaries.
   */
  void fixErasedBlock(
      FileSystem fs, Path srcFile, FileSystem parityFs, Path parityFile,
      long blockSize, long errorOffset, long limit,
      OutputStream out, Progressable reporter) throws IOException {
    configureBuffers(blockSize);
    fixErasedBlockImpl(fs, srcFile, parityFs, parityFile,
      blockSize, errorOffset, limit, out, reporter);
  }

  /**
   * Implementation-specific mechanism of writing a fixed block.
   * @param fs The filesystem containing the source file.
   * @param srcFile The damaged source file.
   * @param parityFs The filesystem containing the parity file. This could be
   *        different from fs in case the parity file is part of a HAR archive.
   * @param parityFile The parity file.
   * @param blockSize The maximum size of a block.
   * @param errorOffset Known location of error in the source file. There could
   *        be additional errors in the source file that are discovered during
   *        the decode process.
   * @param limit The maximum number of bytes to be written out.
   *        This is to prevent writing beyond the end of the file.
   * @param out The output.
   * @param reporter A mechanism to report progress.
   */
  protected abstract void fixErasedBlockImpl(
      FileSystem fs, Path srcFile, FileSystem parityFs, Path parityFile,
      long blockSize, long errorOffset, long limit,
      OutputStream out, Progressable reporter) throws IOException;
}
