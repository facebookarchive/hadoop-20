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

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.raid.DistRaid.EncodingCandidate;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.BlockReconstructor.CorruptBlockReconstructor;
import org.apache.hadoop.raid.StripeReader.StripeInputInfo;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.io.MD5Hash;

/**
 * Represents a generic encoder that can generate a parity file for a source
 * file.
 */
public class Encoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.Encoder");
  public static final int DEFAULT_PARALLELISM = 4;
  public static final int DEFAULT_MAX_BUFFER_SIZE = 1024*1024; 
  public static final String ENCODING_MAX_BUFFER_SIZE_KEY =
      "raid.encoder.max.buffer.size";
  public static final String FINAL_TMP_PARITY_NAME = "0";
  public static final int DEFAULT_RETRY_COUNT_PARTIAL_ENCODING = 3;
  public static final String RETRY_COUNT_PARTIAL_ENCODING_KEY =
      "raid.encoder.retry.count.partial.encoding";
  protected Configuration conf;
  protected int parallelism;
  protected Codec codec;
  protected ErasureCode code;
  protected Random rand;
  protected int bufSize;
  protected int maxBufSize;
  protected int retryCountPartialEncoding;
  protected byte[][] readBufs;
  protected byte[][] writeBufs;
  protected ChecksumStore checksumStore;
  protected StripeStore stripeStore;
  protected boolean requiredChecksumStore = false;

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
    this.maxBufSize = conf.getInt(ENCODING_MAX_BUFFER_SIZE_KEY, 
        DEFAULT_MAX_BUFFER_SIZE);
    this.bufSize = conf.getInt("raid.encoder.bufsize", maxBufSize);
    this.writeBufs = new byte[codec.parityLength][];
    this.checksumStore = RaidNode.createChecksumStore(conf, false);
    this.requiredChecksumStore = conf.getBoolean(
        RaidNode.RAID_CHECKSUM_STORE_REQUIRED_KEY,
        false);
    if (codec.isDirRaid) {
      // only need by directory raid
      this.stripeStore = RaidNode.createStripeStore(conf, false, null);
    }
    this.retryCountPartialEncoding = conf.getInt(RETRY_COUNT_PARTIAL_ENCODING_KEY,
        DEFAULT_RETRY_COUNT_PARTIAL_ENCODING);
    allocateBuffers();
  }
  
  public void verifyStore() throws IOException {
    if (this.requiredChecksumStore && checksumStore == null) {
      throw new IOException("Checksum store is required but is null");
    }
    if (codec.isDirRaid && stripeStore == null) {
      throw new IOException("Stripe store is required but is null");
    }
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
      bufSize = Math.min(bufSize, maxBufSize);
      allocateBuffers();
    }
  }
  
  private void writeToChecksumStore(DistributedFileSystem dfs,
      CRC32[] crcOuts, Path parityTmp, long expectedParityFileSize,
      Progressable reporter) throws IOException {
    LocatedBlocks lbks = dfs.getLocatedBlocks(parityTmp, 0L,
        expectedParityFileSize);
    for (int i = 0; i < crcOuts.length; i++) {
      this.checksumStore.putIfAbsentChecksum(lbks.get(i).getBlock(), 
          crcOuts[i].getValue());
      reporter.progress();
    }
    LOG.info("Wrote checksums of parity file into checksum store");
  }
  
  private void writeToStripeStore(List<List<Block>> srcStripes, DistributedFileSystem
      dfs, FileSystem srcFs, Path srcFile, FileSystem parityFs, 
      Path parityFile, long expectedParityFileSize, Progressable reporter,
      Path finalTmpParity)
          throws IOException {
    try {
      if (srcStripes == null) {
        throw new IOException("source blocks are null");
      }
      LocatedBlocks lbks = dfs.getLocatedBlocks(parityFile, 0L,
        expectedParityFileSize);
      if (srcStripes.size() * codec.parityLength !=
          lbks.locatedBlockCount()) {
        throw new IOException("The number of stripes " + srcStripes.size()
            + "doesn't match the number of parity blocks " +
            lbks.locatedBlockCount() + " and parity length is " +
            codec.parityLength);
      }
      InjectionHandler.processEventIO(
          InjectionEvent.RAID_ENCODING_FAILURE_PUT_STRIPE, parityFile,
              finalTmpParity); 
      for (int i = 0, j = 0; i < srcStripes.size(); i++, 
          j+=codec.parityLength) {
        ArrayList<Block> parityBlocks =
            new ArrayList<Block>(codec.parityLength);
        for (int k = 0; k < codec.parityLength; k++) {
          parityBlocks.add(lbks.get(j + k).getBlock());
        }
        stripeStore.putStripe(codec, parityBlocks, srcStripes.get(i));
        reporter.progress();
      }
      LOG.info("Wrote stripe information into stripe store");
    } catch (Exception ex) {
      LogUtils.logRaidEncodingMetrics(LOGRESULTS.FAILURE, codec,
          -1L, -1L, -1L, -1L, -1L, -1L, 
          srcFile, LOGTYPES.ENCODING, srcFs, ex, reporter);
      if (!dfs.rename(parityFile, finalTmpParity)) {
        LOG.warn("Fail to rename " + parityFile + " back to " + finalTmpParity);
      } else {
        LOG.info("Rename parity file " + parityFile + 
            " back to " + finalTmpParity + " so that we could retry putStripe " + 
            " in the next round");
      }
      throw new IOException(ex);
    }
  }
  
  private Vector<Path> getPartialPaths(int encodingUnit, int expectedNum,
      FileStatus[] stats, Codec codec, long numStripes) throws IOException {
    Vector<Path> partialPaths = new Vector<Path>(expectedNum);
    partialPaths.setSize(expectedNum);
    for (FileStatus stat : stats) {
      int startStripeIdx;
      try {
        startStripeIdx = Integer.parseInt(stat.getPath().getName());
      } catch (NumberFormatException nfe) {
        throw new IOException("partial file " + stat.getPath() +
            " is not a number");
      }
      if (startStripeIdx % encodingUnit != 0) {
        throw new IOException("partial file " + stat.getPath() + " couldn't " +
            "match " + encodingUnit); 
      }
      long numBlocks = RaidNode.numBlocks(stat);
      long expectedNumBlocks = Math.min(encodingUnit, numStripes - startStripeIdx)
          * codec.parityLength;
      if (numBlocks != expectedNumBlocks) {
        throw new IOException("partial file " + stat.getPath() + " has " + numBlocks + 
            " blocks, but it should be " + expectedNumBlocks);
      }
      partialPaths.set(startStripeIdx /encodingUnit, stat.getPath()); 
    }
    return partialPaths;
  }
  
  private List<List<Block>> getSrcStripes(Configuration jobConf, 
      DistributedFileSystem dfs, Path srcFile, Codec codec, long numStripes,
      StripeReader sReader, Progressable reporter)
          throws IOException, InterruptedException {
    List<List<Block>> srcStripes = new ArrayList<List<Block>>();
    List<FileStatus> lfs =
        RaidNode.listDirectoryRaidFileStatus(jobConf, dfs, srcFile);
    if (lfs == null) {
      return null;
    }
    ArrayList<Block> currentBlocks = new ArrayList<Block>();
    long totalBlocks = 0L;
    int index = 0;
    for (FileStatus stat : lfs) {
      LocatedBlocks lbs = dfs.getLocatedBlocks(stat.getPath(),
          0L, stat.getLen());
      for (LocatedBlock lb : lbs.getLocatedBlocks()) {
        currentBlocks.add(lb.getBlock());
        if (currentBlocks.size() == codec.stripeLength) {
          srcStripes.add(currentBlocks);
          totalBlocks += currentBlocks.size();
          currentBlocks = new ArrayList<Block>();
        }
      }
      index++;
      if (index % 10 == 0) {
        Thread.sleep(1000);
      }
      reporter.progress();
    }
    if (currentBlocks.size() > 0) {
      srcStripes.add(currentBlocks);
      totalBlocks += currentBlocks.size();
    }
    if (srcStripes.size() != numStripes || 
        totalBlocks != ((DirectoryStripeReader)sReader).numBlocks) {
      StringBuilder sb = new StringBuilder();
      for (List<Block> lb : srcStripes) {
        for (Block blk : lb) {
          sb.append(blk.toString());
          sb.append(" ");
        }
        sb.append(";");
      }
      throw new IOException("srcStripes has " + srcStripes.size() + 
          " stripes and " + totalBlocks + " blocks : " + sb + 
          " Doesn't match " + srcFile);
    }
    return srcStripes;
  }
  
  /*
   * Create the temp parity file and rename to the partial parity directory
   */
  public boolean encodeTmpParityFile(Configuration jobConf, StripeReader sReader,
      FileSystem parityFs, Path partialTmpParity, Path parityFile,
      short tmpRepl, long blockSize, long expectedPartialParityBlocks,
      long expectedPartialParityFileSize, Progressable reporter)
          throws IOException, InterruptedException {
    // Create a tmp file to which we will write first.
    String jobID = RaidNode.getJobID(jobConf);
    Path tmpDir = new Path(codec.tmpParityDirectory, jobID);
    if (!parityFs.mkdirs(tmpDir)) {
      throw new IOException("Could not create tmp dir " + tmpDir);
    }
    Path parityTmp = new Path(tmpDir, parityFile.getName() + rand.nextLong());
    FSDataOutputStream out = parityFs.create(
                               parityTmp,
                               true,
                               conf.getInt("io.file.buffer.size", 64 * 1024),
                               tmpRepl,
                               blockSize);
    try {
      CRC32[] crcOuts = null;
      if (checksumStore != null) {
        crcOuts = new CRC32[(int)expectedPartialParityBlocks];
      }
      encodeFileToStream(sReader, blockSize, out, crcOuts, reporter);
      out.close();
      out = null;
      LOG.info("Wrote temp parity file " + parityTmp);
      FileStatus tmpStat = parityFs.getFileStatus(parityTmp);
      if (tmpStat.getLen() != expectedPartialParityFileSize) {
        InjectionHandler.processEventIO(
            InjectionEvent.RAID_ENCODING_FAILURE_PARTIAL_PARITY_SIZE_MISMATCH);
        throw new IOException("Expected partial parity size " +
          expectedPartialParityFileSize + " does not match actual " +
          tmpStat.getLen() + " in path " + tmpStat.getPath());
      }
      InjectionHandler.processEventIO(
          InjectionEvent.RAID_ENCODING_FAILURE_PUT_CHECKSUM);
      if (checksumStore != null) {
        this.writeToChecksumStore((DistributedFileSystem)parityFs, crcOuts,
            parityTmp, expectedPartialParityFileSize, reporter);
      }
      if (!parityFs.rename(parityTmp, partialTmpParity)) {
        LOG.warn("Fail to rename file " + parityTmp + " to " + partialTmpParity);
        return false;
      }
      LOG.info("renamed " + parityTmp + " to " + partialTmpParity);
      return true;
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
  
  public boolean finishAllPartialEncoding(FileSystem parityFs,
      Path tmpPartialParityDir, long expectedNum)
          throws IOException, InterruptedException {
    //Verify if we finish all partial encoding
    try {
      FileStatus[] stats = null;
      long len = 0;
      for (int i = 0; i < this.retryCountPartialEncoding; i++) {
        stats = parityFs.listStatus(tmpPartialParityDir);
        len = stats != null ? stats.length : 0;
        if (len == expectedNum) {
          return true;
        }
        if (i + 1 == this.retryCountPartialEncoding) {
          Thread.sleep(rand.nextInt(2000));
        }
      }
      LOG.info("Number of partial files in the directory " + 
          tmpPartialParityDir + " is " + len + 
          ". It's different from expected number " + expectedNum);
      return false;
    } catch (FileNotFoundException fnfe) {
      LOG.info("The temp directory is already moved to final partial" + 
               " directory");
      return false;
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
   * @throws InterruptedException 
   */
  public boolean encodeFile(
    Configuration jobConf, FileSystem fs, FileSystem parityFs,
    Path parityFile, short parityRepl, long numStripes, long blockSize, 
    Progressable reporter, StripeReader sReader, EncodingCandidate ec)
        throws IOException, InterruptedException {
    DistributedFileSystem dfs = DFSUtil.convertToDFS(parityFs);
    Path srcFile = ec.srcStat.getPath();
    long expectedParityFileSize = numStripes * blockSize * codec.parityLength;
    long expectedPartialParityBlocks =
        (sReader.stripeEndIdx - sReader.stripeStartIdx) * codec.parityLength;
    long expectedPartialParityFileSize = expectedPartialParityBlocks
        * blockSize;
    
    // Create a tmp file to which we will write first.
    String jobID = RaidNode.getJobID(jobConf);
    Path tmpDir = new Path(codec.tmpParityDirectory, jobID);
    if (!parityFs.mkdirs(tmpDir)) {
      throw new IOException("Could not create tmp dir " + tmpDir);
    }

    String partialParityName = "partial_" + MD5Hash.digest(srcFile.toUri().getPath()) +
        "_" + ec.srcStat.getModificationTime() + "_" + ec.encodingUnit + "_" +
        ec.encodingId;
    Path partialParityDir = new Path(tmpDir, partialParityName);
    Path tmpPartialParityDir = new Path(partialParityDir, "tmp");
    Path finalPartialParityDir = new Path(partialParityDir, "final");
    if (!parityFs.mkdirs(partialParityDir)) {
      throw new IOException("Could not create partial parity directory " +
          partialParityDir);
    }
    // If we write a parity for a large directory, 
    // Use 3 replicas to guarantee the durability by default
    short tmpRepl = (short)conf.getInt(RaidNode.RAID_PARITY_INITIAL_REPL_KEY, 
        RaidNode.DEFAULT_RAID_PARITY_INITIAL_REPL);
    
    Path finalTmpParity = null;
    /**
     * To support retriable encoding, we use three checkpoints to represent
     * the last success state. 
     * 1. isEncoded: Set to true when partial partiy is generated
     * 2. isRenamed: Set to true when all partial parity are generated and
     *               tmpPartialParityDir is moved to finalPartialParityDir
     * 3. isConcated: Set to true when partial parities are concatenated into
     *                a final parity. 
     */
    if (!ec.isConcated) {
      if (!ec.isEncoded) {
        if (!parityFs.mkdirs(tmpPartialParityDir)) {
          throw new IOException("Could not create " + tmpPartialParityDir);
        }
        Path partialTmpParity = new Path(tmpPartialParityDir, 
          Long.toString(sReader.getCurrentStripeIdx()));
        LOG.info("Encoding partial parity " + partialTmpParity);
        if (!encodeTmpParityFile(jobConf, sReader, dfs,
                           partialTmpParity, parityFile, tmpRepl, blockSize,
                           expectedPartialParityBlocks, expectedPartialParityFileSize,
                           reporter)) {
          return false;
        }
        LOG.info("Encoded partial parity " + partialTmpParity);
      }
      ec.isEncoded = true;
      long expectedNum = (long) Math.ceil(numStripes * 1.0 / ec.encodingUnit);
      if (!ec.isRenamed) {
        if (!finishAllPartialEncoding(parityFs, tmpPartialParityDir, expectedNum)) {
          return false;
        }
        InjectionHandler.processEventIO(
            InjectionEvent.RAID_ENCODING_FAILURE_RENAME_FILE);
        // Move the directory to final
        if (!dfs.rename(tmpPartialParityDir, finalPartialParityDir)) {
          LOG.info("Fail to rename " + tmpPartialParityDir + " to " +
              finalPartialParityDir);
          return false;
        }
        LOG.info("Renamed " + tmpPartialParityDir + " to " +
                 finalPartialParityDir);
        ec.isRenamed = true;
      }
      FileStatus[] stats = parityFs.listStatus(finalPartialParityDir);
      // Verify partial parities are correct
      Vector<Path> partialPaths = getPartialPaths((int)ec.encodingUnit,
          (int)expectedNum, stats, codec, numStripes);
      finalTmpParity = partialPaths.get(0);
      InjectionHandler.processEventIO(
          InjectionEvent.RAID_ENCODING_FAILURE_CONCAT_FILE);
      if (partialPaths.size() > 1) {
        Path[] restPaths = partialPaths.subList(1,
            partialPaths.size()).toArray(new Path[partialPaths.size() - 1]);
        try {
          // Concat requires source and target files are in the same directory
          dfs.concat(finalTmpParity, restPaths, true);
          LOG.info("Concated " + partialPaths.size() + " files into " + finalTmpParity);
          
        } catch (IOException ioe) {
          // Maybe other tasks already finish concating. 
          LOG.info("Fail to concat " + partialPaths.size() +
              " files into " + finalTmpParity, ioe);
          throw ioe;
        }
      }
      ec.isConcated = true;
    } else {
      FileStatus[] stats = parityFs.listStatus(finalPartialParityDir);
      if (stats == null || stats.length == 0) {
        return false;
      }
      if (stats.length > 1) {
        throw new IOException("We shouldn't have more than 1 files under" 
            + finalPartialParityDir);
      } 
      finalTmpParity = stats[0].getPath();
    }
    FileStatus tmpStat = parityFs.getFileStatus(finalTmpParity);
    if (tmpStat.getBlockSize() != blockSize) {
      throw new IOException("Expected parity block size " +
          blockSize + " does not match actual " + 
          tmpStat.getBlockSize() + " in path " + finalTmpParity);
    }
    if (tmpStat.getLen() != expectedParityFileSize) {
      throw new IOException("Expected parity size " +
          expectedParityFileSize + " does not match actual " +
          tmpStat.getLen() + " in path " + finalTmpParity);
    }
    if (ec.srcStripes == null && stripeStore != null) {
      InjectionHandler.processEventIO(
        InjectionEvent.RAID_ENCODING_FAILURE_GET_SRC_STRIPES);
      ec.srcStripes = getSrcStripes(jobConf, dfs, srcFile, codec, numStripes,
          sReader, reporter);
      if (ec.srcStripes == null) {
        LOG.error("Cannot get srcStripes for " + srcFile);
        return false;
      }
    }
    
    // delete destination if exists
    if (dfs.exists(parityFile)){
      dfs.delete(parityFile, false);
    }
    dfs.mkdirs(parityFile.getParent());
    if (!dfs.rename(finalTmpParity, parityFile)) {
      String msg = "Unable to rename file " + finalTmpParity + " to " + parityFile;
      throw new IOException (msg);
    }
    LOG.info("Wrote parity file " + parityFile);
    
    if (stripeStore != null) {
      this.writeToStripeStore(ec.srcStripes, dfs, fs,
          srcFile, parityFs, parityFile, expectedParityFileSize, reporter,
          finalTmpParity);
    }
    if (tmpRepl != parityRepl) {
      dfs.setReplication(parityFile, parityRepl);
      LOG.info("Reduce replication of " + parityFile + " to " + parityRepl);
    }
    dfs.delete(partialParityDir, true);
    return true;
  } 

  /**
   * Recovers a corrupt block in a parity file to a local file.
   *
   * The encoder generates codec.parityLength parity blocks for a source file stripe.
   * Since we want only one of the parity blocks, this function creates
   * null outputs for the blocks to be discarded.
   *
   * @param fs The filesystem in which both srcFile and parityFile reside.
   * @param srcStat fileStatus of The source file.
   * @param blockSize The block size for the parity files.
   * @param corruptOffset The location of corruption in the parity file.
   * @param out The destination for the reovered block.
   * @param progress A reporter for progress.
   */
  public CRC32 recoverParityBlockToStream(
    FileSystem fs, FileStatus srcStat, long blockSize,
    Path parityFile, long corruptOffset,
    OutputStream out, Progressable progress) throws IOException {
    LOG.info("Recovering parity block" + parityFile + ":" + corruptOffset);
    Path srcFile = srcStat.getPath();
    // Get the start offset of the corrupt block.
    corruptOffset = (corruptOffset / blockSize) * blockSize;
    // Output streams to each block in the parity file stripe.
    OutputStream[] outs = new OutputStream[codec.parityLength];
    long indexOfCorruptBlockInParityStripe =
      (corruptOffset / blockSize) % codec.parityLength;
    LOG.info("Index of corrupt block in parity stripe: " +
              indexOfCorruptBlockInParityStripe);
    CRC32[] crcOuts = null;
    if (checksumStore != null) {
      crcOuts = new CRC32[codec.parityLength];
    }
    // Create a real output stream for the block we want to recover,
    // and create null streams for the rest.
    for (int i = 0; i < codec.parityLength; i++) {
      if (indexOfCorruptBlockInParityStripe == i) {
        outs[i] = out;
        if (checksumStore != null) {
          crcOuts[i] = new CRC32();
        }
      } else {
        outs[i] = new NullOutputStream();
      }
    }
    // Get the stripe index and start offset of stripe.
    long stripeIdx = corruptOffset / (codec.parityLength * blockSize);
    StripeReader sReader = StripeReader.getStripeReader(codec, conf, 
        blockSize, fs, stripeIdx, srcStat);
    // Get input streams to each block in the source file stripe.
    assert sReader.hasNext() == true;
    InputStream[] blocks = sReader.getNextStripeInputs().getInputs();
    LOG.info("Starting recovery by using source stripe " +
              srcFile + ": stripe " + stripeIdx);
    
    try {
      // Read the data from the blocks and write to the parity file.
      encodeStripe(blocks, blockSize, outs, crcOuts, progress, false, null);
      if (checksumStore != null) {
        return crcOuts[(int)indexOfCorruptBlockInParityStripe];
      } else {
        return null;
      }
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
   * @throws InterruptedException 
   */
  private void encodeFileToStream(StripeReader sReader,
    long blockSize, FSDataOutputStream out, CRC32[] crcOuts,
    Progressable reporter) 
        throws IOException, InterruptedException {
    OutputStream[] tmpOuts = new OutputStream[codec.parityLength];
    // One parity block can be written directly to out, rest to local files.
    tmpOuts[0] = out;
    File[] tmpFiles = new File[codec.parityLength - 1];
    for (int i = 0; i < codec.parityLength - 1; i++) {
      tmpFiles[i] = File.createTempFile("parity", "_" + i);
      LOG.info("Created tmp file " + tmpFiles[i]);
      tmpFiles[i].deleteOnExit();
    }
    int finishedParityBlockIdx = 0;
    List<Integer> errorLocations = new ArrayList<Integer>();
    try {
      // Loop over stripe
      boolean redo;
      while (sReader.hasNext()) {
        reporter.progress();
        StripeInputInfo stripeInputInfo = null;
        InputStream[] blocks = null;
        // Create input streams for blocks in the stripe.
        long currentStripeIdx = sReader.getCurrentStripeIdx();
        stripeInputInfo = sReader.getNextStripeInputs();
        // The offset of first temporary output stream
        long encodeStartOffset = out.getPos();
        int retry = 3;
        do {
          redo = false;
          retry --;
          try {
            blocks = stripeInputInfo.getInputs();
            CRC32[] curCRCOuts = new CRC32[codec.parityLength];
            
            if (crcOuts != null) {
              for (int i = 0; i < codec.parityLength; i++) {
                crcOuts[finishedParityBlockIdx + i] = curCRCOuts[i]
                    = new CRC32();
              }
            }
            // Create output streams to the temp files.
            for (int i = 0; i < codec.parityLength - 1; i++) {
              tmpOuts[i + 1] = new FileOutputStream(tmpFiles[i]);
            }
            // Call the implementation of encoding.
            encodeStripe(blocks, blockSize, tmpOuts, curCRCOuts, reporter, 
                true, errorLocations);
          } catch (IOException e) {
            if (out.getPos() > encodeStartOffset) {
              // Partial data is already written, throw the exception
              InjectionHandler.processEventIO(
                  InjectionEvent.RAID_ENCODING_PARTIAL_STRIPE_ENCODED);
              throw e;
            }
            // try to fix the missing block in the stripe using stripe store.
            if ((e instanceof BlockMissingException || 
                e instanceof ChecksumException) && codec.isDirRaid) {
              if (retry <= 0) {
                throw e;
              }
              redo = true;
              CorruptBlockReconstructor constructor = 
                  new CorruptBlockReconstructor(conf);
              
              Set<Path> srcPaths = new HashSet<Path>();
              for (int idx : errorLocations) {
                Path srcPath = stripeInputInfo.getSrcPaths()[idx];
                if (srcPath != null) {
                  srcPaths.add(srcPath);
                }
              }
             
              for (Path srcPath : srcPaths) {
                Decoder decoder = new Decoder(conf, codec);
                decoder.connectToStore(srcPath);
                LOG.info("In Encoding: try to reconstruct the file: " + srcPath);
                // will throw exception if it fails to reconstruct the lost
                // blocks.
                constructor.processFile(srcPath, null, decoder, true, null);
                LOG.info("In Encoding: finished to reconstruct the file: " + srcPath);
              }
            } else {
              throw e;
            } 
          } finally {
            if (blocks != null) {
              RaidUtils.closeStreams(blocks);
            }
          }
          if (redo) {
            // rebuild the inputs.
            stripeInputInfo = sReader.getStripeInputs(currentStripeIdx);
          }
        } while (redo);
        
        // Close output streams to the temp files and write the temp files
        // to the output provided.
        for (int i = 0; i < codec.parityLength - 1; i++) {
          tmpOuts[i + 1].close();
          tmpOuts[i + 1] = null;
          InputStream in  = new FileInputStream(tmpFiles[i]);
          RaidUtils.copyBytes(in, out, writeBufs[i], blockSize);
          reporter.progress();
        }
        finishedParityBlockIdx += codec.parityLength;
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
    CRC32[] crcOuts,
    Progressable reporter,
    boolean computeSrcChecksum,
    List<Integer> errorLocations) throws IOException {
    configureBuffers(blockSize);
    int boundedBufferCapacity = 1;
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, blocks, bufSize, 
      parallelism, boundedBufferCapacity, blockSize, computeSrcChecksum,
      outs);
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
          if (errorLocations != null) {
            errorLocations.clear();
            for (int idx : readResult.getErrorIdx()) {
              errorLocations.add(idx);
            }
          }
          throw readEx;
        }

        code.encodeBulk(readResult.readBufs, writeBufs);
        reporter.progress();
        // Assume each byte is independently encoded
        int toWrite = (int)Math.min(blockSize - encoded, bufSize);

        // Now that we have some data to write, send it to the temp files.
        for (int i = 0; i < codec.parityLength; i++) {
          outs[i].write(writeBufs[i], 0, toWrite);
          if (crcOuts != null && crcOuts[i] != null) {
            crcOuts[i].update(writeBufs[i], 0, toWrite);
          }
          reporter.progress();
        }
      }
      if (computeSrcChecksum) {
        parallelReader.collectSrcBlocksChecksum(checksumStore);
      }
    } finally {
      parallelReader.shutdown();
    }
  }
}
