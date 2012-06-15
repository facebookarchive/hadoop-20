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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Progressable;

/**
 * Represents a generic encoder that can generate a parity file for a source
 * file.
 */
public class Encoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.Encoder");
  public static final int DEFAULT_PARALLELISM = 4;
  protected Configuration conf;
  protected int parallelism;
  protected Codec codec;
  protected ErasureCode code;
  protected Random rand;
  protected int bufSize;
  protected byte[][] readBufs;
  protected byte[][] writeBufs;

  /**
   * A class that acts as a sink for data, similar to /dev/null.
   */
  static class NullOutputStream extends OutputStream {
    public void write(byte[] b) throws IOException {}
    public void write(int b) throws IOException {}
    public void write(byte[] b, int off, int len) throws IOException {}
  }

  Encoder(Configuration conf, Codec codec) {
    this.conf = conf;
    this.parallelism = conf.getInt("raid.encoder.parallelism",
                                   DEFAULT_PARALLELISM);
    this.codec = codec;
    this.code = codec.createErasureCode(conf);
    this.rand = new Random();
    this.bufSize = conf.getInt("raid.encoder.bufsize", 1024 * 1024);
    this.writeBufs = new byte[codec.parityLength][];
    allocateBuffers();
  }

  private void allocateBuffers() {
    for (int i = 0; i < codec.parityLength; i++) {
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
   * The interface to use to generate a parity file.
   * This method can be called multiple times with the same Encoder object,
   * thus allowing reuse of the buffers allocated by the Encoder object.
   *
   * @param fs The filesystem containing the source file.
   * @param srcFile The source file.
   * @param parityFile The parity file to be generated.
   */
  public void encodeFile(
    Configuration jobConf, FileSystem fs, Path srcFile, FileSystem parityFs,
    Path parityFile, short parityRepl, long numStripes, long blockSize, 
    Progressable reporter, StripeReader sReader)
        throws IOException {
    long expectedParityBlocks = numStripes * codec.parityLength;
    long expectedParityFileSize = numStripes * blockSize * codec.parityLength;
    
    // Create a tmp file to which we will write first.
    String jobID = RaidNode.getJobID(jobConf);
    Path tmpDir = new Path(codec.tmpParityDirectory, jobID);
    if (!parityFs.mkdirs(tmpDir)) {
      throw new IOException("Could not create tmp dir " + tmpDir);
    }
    Path parityTmp = new Path(tmpDir,
        parityFile.getName() + rand.nextLong());
    // Writing out a large parity file at replication 1 is difficult since
    // some datanode could die and we would not be able to close() the file.
    // So write at replication 2 and then reduce it after close() succeeds.
    short tmpRepl = parityRepl;
    if (expectedParityBlocks >=
        conf.getInt("raid.encoder.largeparity.blocks", 20)) {
      if (parityRepl == 1) {
        tmpRepl = 2;
      }
    }
    FSDataOutputStream out = parityFs.create(
                               parityTmp,
                               true,
                               conf.getInt("io.file.buffer.size", 64 * 1024),
                               tmpRepl,
                               blockSize);

    try {
      encodeFileToStream(sReader, blockSize, out, reporter);
      out.close();
      out = null;
      LOG.info("Wrote temp parity file " + parityTmp);
      FileStatus tmpStat = parityFs.getFileStatus(parityTmp);
      if (tmpStat.getLen() != expectedParityFileSize) {
        throw new IOException("Expected parity size " + expectedParityFileSize +
          " does not match actual " + tmpStat.getLen());
      }

      // delete destination if exists
      if (parityFs.exists(parityFile)){
        parityFs.delete(parityFile, false);
      }
      parityFs.mkdirs(parityFile.getParent());
      if (tmpRepl > parityRepl) {
        parityFs.setReplication(parityTmp, parityRepl);
      }
      if (!parityFs.rename(parityTmp, parityFile)) {
        String msg = "Unable to rename file " + parityTmp + " to " + parityFile;
        throw new IOException (msg);
      }
      LOG.info("Wrote parity file " + parityFile);
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } finally {
        parityFs.delete(parityTmp, false);
      }
    }
  }

  /**
   * Recovers a corrupt block in a parity file to a local file.
   *
   * The encoder generates codec.parityLength parity blocks for a source file stripe.
   * Since we want only one of the parity blocks, this function creates
   * null outputs for the blocks to be discarded.
   *
   * @param fs The filesystem in which both srcFile and parityFile reside.
   * @param srcFile The source file.
   * @param srcSize The size of the source file.
   * @param blockSize The block size for the source/parity files.
   * @param corruptOffset The location of corruption in the parity file.
   * @param localBlockFile The destination for the reovered block.
   * @param progress A reporter for progress.
   */
  public void recoverParityBlockToFile(
    FileSystem fs,
    Path srcFile, long srcSize, long blockSize,
    Path parityFile, long corruptOffset,
    File localBlockFile, Progressable progress) throws IOException {
    OutputStream out = new FileOutputStream(localBlockFile);
    try {
      recoverParityBlockToStream(fs, srcFile, srcSize, blockSize, parityFile,
        corruptOffset, out, progress);
    } finally {
      out.close();
    }
  }

  /**
   * Recovers a corrupt block in a parity file to a local file.
   *
   * The encoder generates codec.parityLength parity blocks for a source file stripe.
   * Since we want only one of the parity blocks, this function creates
   * null outputs for the blocks to be discarded.
   *
   * @param fs The filesystem in which both srcFile and parityFile reside.
   * @param srcFile The source file.
   * @param srcSize The size of the source file.
   * @param blockSize The block size for the source/parity files.
   * @param corruptOffset The location of corruption in the parity file.
   * @param out The destination for the reovered block.
   * @param progress A reporter for progress.
   */
  public void recoverParityBlockToStream(
    FileSystem fs,
    Path srcFile, long srcSize, long blockSize,
    Path parityFile, long corruptOffset,
    OutputStream out, Progressable progress) throws IOException {
    LOG.info("Recovering parity block" + parityFile + ":" + corruptOffset);
    // Get the start offset of the corrupt block.
    corruptOffset = (corruptOffset / blockSize) * blockSize;
    // Output streams to each block in the parity file stripe.
    OutputStream[] outs = new OutputStream[codec.parityLength];
    long indexOfCorruptBlockInParityStripe =
      (corruptOffset / blockSize) % codec.parityLength;
    LOG.info("Index of corrupt block in parity stripe: " +
              indexOfCorruptBlockInParityStripe);
    // Create a real output stream for the block we want to recover,
    // and create null streams for the rest.
    for (int i = 0; i < codec.parityLength; i++) {
      if (indexOfCorruptBlockInParityStripe == i) {
        outs[i] = out;
      } else {
        outs[i] = new NullOutputStream();
      }
    }
    // Get the stripe index and start offset of stripe.
    long stripeIdx = corruptOffset / (codec.parityLength * blockSize);
    StripeReader sReader = StripeReader.getStripeReader(codec, conf, 
        blockSize, fs, stripeIdx, srcFile, srcSize);
    // Get input streams to each block in the source file stripe.
    assert sReader.hasNext() == true;
    InputStream[] blocks = sReader.getNextStripeInputs();
    LOG.info("Starting recovery by using source stripe " +
              srcFile + ": stripe " + stripeIdx);
    try {
      // Read the data from the blocks and write to the parity file.
      encodeStripe(blocks, blockSize, outs, progress);
    } finally {
      RaidUtils.closeStreams(blocks);
    }
  }

  /**
   * Recovers a corrupt block in a parity file to an output stream.
   *
   * The encoder generates codec.parityLength parity blocks for a source file stripe.
   * Since there is only one output provided, some blocks are written out to
   * files before being written out to the output.
   *
   * @param blockSize The block size for the source/parity files.
   * @param out The destination for the reovered block.
   */
  private void encodeFileToStream(StripeReader sReader,
    long blockSize, OutputStream out, Progressable reporter) throws IOException {
    OutputStream[] tmpOuts = new OutputStream[codec.parityLength];
    // One parity block can be written directly to out, rest to local files.
    tmpOuts[0] = out;
    File[] tmpFiles = new File[codec.parityLength - 1];
    for (int i = 0; i < codec.parityLength - 1; i++) {
      tmpFiles[i] = File.createTempFile("parity", "_" + i);
      LOG.info("Created tmp file " + tmpFiles[i]);
      tmpFiles[i].deleteOnExit();
    }
    try {
      // Loop over stripe
      while (sReader.hasNext()) {
        reporter.progress();
        // Create input streams for blocks in the stripe.
        InputStream[] blocks = sReader.getNextStripeInputs();
        try {
          // Create output streams to the temp files.
          for (int i = 0; i < codec.parityLength - 1; i++) {
            tmpOuts[i + 1] = new FileOutputStream(tmpFiles[i]);
          }
          // Call the implementation of encoding.
          encodeStripe(blocks, blockSize, tmpOuts, reporter);
        } finally {
          RaidUtils.closeStreams(blocks);
        }
        // Close output streams to the temp files and write the temp files
        // to the output provided.
        for (int i = 0; i < codec.parityLength - 1; i++) {
          tmpOuts[i + 1].close();
          tmpOuts[i + 1] = null;
          InputStream in  = new FileInputStream(tmpFiles[i]);
          RaidUtils.copyBytes(in, out, writeBufs[i], blockSize);
          reporter.progress();
        }
      }
    } finally {
      for (int i = 0; i < codec.parityLength - 1; i++) {
        if (tmpOuts[i + 1] != null) {
          tmpOuts[i + 1].close();
        }
        tmpFiles[i].delete();
        LOG.info("Deleted tmp file " + tmpFiles[i]);
      }
    }
  }

  /**
   * Wraps around encodeStripeImpl in order to configure buffers.
   * Having buffers of the right size is extremely important. If the the
   * buffer size is not a divisor of the block size, we may end up reading
   * across block boundaries.
   */
  void encodeStripe(
    InputStream[] blocks,
    long blockSize,
    OutputStream[] outs,
    Progressable reporter) throws IOException {
    configureBuffers(blockSize);
    int boundedBufferCapacity = 1;
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, blocks, bufSize, parallelism, boundedBufferCapacity, blockSize);
    parallelReader.start();
    try {
      for (long encoded = 0; encoded < blockSize; encoded += bufSize) {
        ParallelStreamReader.ReadResult readResult = null;
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

        code.encodeBulk(readResult.readBufs, writeBufs);
        reporter.progress();

        // Now that we have some data to write, send it to the temp files.
        for (int i = 0; i < codec.parityLength; i++) {
          outs[i].write(writeBufs[i], 0, bufSize);
          reporter.progress();
        }
      }
    } finally {
      parallelReader.shutdown();
    }
  }
}
