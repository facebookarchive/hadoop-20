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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.zip.CRC32;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.net.NetUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.raid.StripeReader.LocationPair;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.RaidCounter;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.RaidUtils.ZeroInputStream;
import org.apache.hadoop.raid.StripeStore.StripeInfo;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents a generic decoder that can be used to read a file with
 * corrupt blocks by using the parity file.
 */
public class Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.Decoder");
  public static final int DEFAULT_PARALLELISM = 4;
  public static final int DEFAULT_MAX_BUFFER_SIZE = 1024*1024; 
  public static final String DECODING_MAX_BUFFER_SIZE_KEY =
      "raid.decoder.max.buffer.size";
  protected Configuration conf;
  protected int parallelism;
  protected Codec codec;
  protected ErasureCode code;
  protected Random rand;
  protected int bufSize;
  protected int maxBufSize;
  protected byte[][] readBufs;
  protected byte[][] writeBufs;
  private int numMissingBlocksInStripe;
  private long numReadBytes;
  protected ChecksumStore checksumStore;
  protected StripeStore stripeStore;
  // Require checksum verification 
  protected boolean requiredChecksumVerification = false;
  protected boolean requiredChecksumStore = false;
  private boolean[] remoteRackFlag;
  private long numReadBytesRemoteRack;
  private ErasureCode[] parallelCode;

  public Decoder(Configuration conf, Codec codec) {
    this.conf = conf;
    this.parallelism = conf.getInt("raid.encoder.parallelism",
                                   DEFAULT_PARALLELISM);
    this.codec = codec;
    this.code = codec.createErasureCode(conf);
    this.rand = new Random();
    this.maxBufSize = conf.getInt(DECODING_MAX_BUFFER_SIZE_KEY, 
        DEFAULT_MAX_BUFFER_SIZE);
    this.bufSize = conf.getInt("raid.decoder.bufsize", maxBufSize);
    this.writeBufs = new byte[codec.parityLength][];
    this.readBufs = new byte[codec.parityLength + codec.stripeLength][];
    
    parallelCode = new ErasureCode[parallelism];
    for (int i = 0; i < parallelism; i++) {
      parallelCode[i] = codec.createErasureCode(conf);
    }
    allocateBuffers();
  }
  
  public void connectToStore(Path srcPath) throws IOException {
    this.checksumStore = RaidNode.createChecksumStore(conf, false);
    this.requiredChecksumStore = conf.getBoolean(
        RaidNode.RAID_CHECKSUM_STORE_REQUIRED_KEY,
        false);
    this.requiredChecksumVerification = conf.getBoolean(
        RaidNode.RAID_CHECKSUM_VERIFICATION_REQUIRED_KEY,
        false);
    if (codec.isDirRaid) {
      // only needed by directory raid
      FileSystem fs = srcPath.getFileSystem(conf);
      this.stripeStore = RaidNode.createStripeStore(conf, false, fs);
    }
    
    if (this.requiredChecksumStore && checksumStore == null) {
      throw new IOException("Checksum store is required but is null");
    }
    if (codec.isDirRaid && stripeStore == null) {
      throw new IOException("Stripe store is required but is null");
    }
  }
  
  public int getNumMissingBlocksInStripe() {
    return numMissingBlocksInStripe;
  }

  public long getNumReadBytes() {
    return numReadBytes;
  }
  
  public long getNumReadBytesRemoteRack() {
    return numReadBytesRemoteRack;
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
  
  /**
   * Retrieve stripes from stripe store 
   */
  public StripeInfo retrieveStripe(Block lostBlock, Path p,
      long lostBlockOffset, FileSystem fs, Context context,
      boolean online)
          throws IOException {
    StripeInfo si = null;
    if (stripeStore != null) { 
      IOException caughtException = null;
      try {
        si = stripeStore.getStripe(codec, lostBlock);
      } catch (IOException ioe) {
        LOG.error(" Fail to get stripe " + codec 
            + " : " + lostBlock, ioe);
        caughtException = ioe;
      }
      if (si == null) {  
        // Stripe is not record, we should report  
        LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0,
            codec, p, lostBlockOffset,
            online? LOGTYPES.ONLINE_RECONSTRUCTION_GET_STRIPE: 
            LOGTYPES.OFFLINE_RECONSTRUCTION_GET_STRIPE,
            fs, caughtException, context); 
      }
    }
    return si;
  }
  
  /**
   * Retrieve checksums from checksum store and record checksum lost
   * if possible
   */
  public Long retrieveChecksum(Block lostBlock, Path p,
      long lostBlockOffset, FileSystem fs, Context context) 
          throws IOException {
    Long oldCRC = null;  
    if (checksumStore != null) { 
      IOException caughtException = null;
      try {
        oldCRC = checksumStore.getChecksum(lostBlock); 
      } catch (IOException ioe) {
        LOG.error(" Fail to get checksum for block " + lostBlock, ioe);
        caughtException = ioe;
      }
      // Checksum is not record, we should report
      if (oldCRC == null) {
        LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0,
            codec, p, lostBlockOffset,
            LOGTYPES.OFFLINE_RECONSTRUCTION_GET_CHECKSUM,
            fs, caughtException, context);
      }
    }
    return oldCRC;
  }
  
  public CRC32 recoverParityBlockToFile(
      FileSystem srcFs, FileStatus srcStat, FileSystem parityFs, Path parityPath,
      long blockSize, long blockOffset, File localBlockFile, 
      StripeInfo si, Context context) throws IOException, InterruptedException {
    OutputStream out = null;
    try {
      out = new FileOutputStream(localBlockFile);
      return fixErasedBlock(srcFs, srcStat, parityFs, parityPath,
                            false, blockSize, blockOffset, blockSize,
                            false, out, si, context, false);
    } finally {
      if (out != null) {
        out.close();
      }
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
   * @param context A mechanism to report progress.
   */
  public CRC32 recoverBlockToFile(
    FileSystem srcFs, FileStatus srcStat, FileSystem parityFs, Path parityPath,
    long blockSize, long blockOffset, File localBlockFile, long limit,
    StripeInfo si, Context context) throws IOException, InterruptedException {
    OutputStream out = null;
    try {
      out = new FileOutputStream(localBlockFile);
      return fixErasedBlock(srcFs, srcStat, parityFs, parityPath, true,
                            blockSize, blockOffset, limit, false, out,
                            si, context, false);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
  
  /**
   * Recover a corrupt block to local file. Using the stripe information 
   * stored in the Stripe Store.
   * 
   * @param srcFs The filesystem containing the source file.
   * @param srcPath The damaged source file.
   * @param lostBlock The missing/corrupted block
   * @param localBlockFile The file to write the block to.
   * @param blockSize The block size of the file.
   * @param lostBlockOffset The start offset of the block in the file.
   * @param limit The maximum number of bytes to be written out.
   * @param si  The StripeInfo retrieved from Stripe Store.
   * @param context
   * @return
   * @throws IOException
   */
  public CRC32 recoverBlockToFileFromStripeInfo(
      FileSystem srcFs, Path srcPath, Block lostBlock, File localBlockFile, 
      long blockSize, long lostBlockOffset, long limit, 
      StripeInfo si, Context context) throws IOException {
    OutputStream out = null;
    try {
      out = new FileOutputStream(localBlockFile);
      CRC32 crc = null;
      if (checksumStore != null) {
        crc = new CRC32();
      }
      fixErasedBlockImpl(srcFs, srcPath, srcFs, null, true, blockSize,
                        lostBlockOffset, limit, false, out, 
                        context, crc, si, true, lostBlock);
      return crc;
    } finally {
      if (null != out) {
        out.close();
      }
    }
  }

  /**
   * Return the old code id to construct a old decoder
   */
  private String getOldCodeId(FileStatus srcStat ) throws IOException {
    if (codec.id.equals("xor") || codec.id.equals("rs")) {
      return codec.id;
    } else {
      // Search for xor/rs parity files
      if (ParityFilePair.getParityFile(
        Codec.getCodec("xor"), srcStat, this.conf) != null)
        return "xor";
      if (ParityFilePair.getParityFile(
        Codec.getCodec("rs"), srcStat, this.conf) != null)
        return "rs";
    }
    return null;
  }

  DecoderInputStream generateAlternateStream(FileSystem srcFs, Path srcFile,
                      FileSystem parityFs, Path parityFile,
                      long blockSize, long errorOffset, long limit,
                      Block lostBlock, StripeInfo si, 
                      boolean recoverFromStripeInfo,
                      Context context) {
    configureBuffers(blockSize);
    Progressable reporter = context;
    if (reporter == null) {
      reporter = RaidUtils.NULL_PROGRESSABLE;
    }
    
    DecoderInputStream decoderInputStream = new DecoderInputStream(
        reporter, limit, blockSize, errorOffset, 
        srcFs, srcFile, parityFs, parityFile,
        lostBlock, si, recoverFromStripeInfo);
    
    return decoderInputStream;
  }

  /**
   * Having buffers of the right size is extremely important. If the the
   * buffer size is not a divisor of the block size, we may end up reading
   * across block boundaries.
   *
   * If codec's simulateBlockFix is true, we use the old code to fix blocks
   * and verify the new code's result is the same as the old one.
   */
  CRC32 fixErasedBlock(
      FileSystem srcFs, FileStatus srcStat, FileSystem parityFs, Path parityFile,
      boolean fixSource,
      long blockSize, long errorOffset, long limit, boolean partial,
      OutputStream out, StripeInfo si, Context context,
      boolean skipVerify) throws IOException, InterruptedException {
    configureBuffers(blockSize);
    Progressable reporter = context;
    if (reporter == null) {
      reporter = RaidUtils.NULL_PROGRESSABLE;
    }

    Path srcFile = srcStat.getPath();
    LOG.info("Code: " + this.codec.id + " simulation: " + this.codec.simulateBlockFix);
    if (this.codec.simulateBlockFix) {
      String oldId = getOldCodeId(srcStat);
      if (oldId == null) {
        // Couldn't find old codec for block fixing, throw exception instead
        throw new IOException("Couldn't find old parity files for " + srcFile
            + ". Won't reconstruct the block since code " + this.codec.id +
            " is still under test");
      }
      if (partial) {
        throw new IOException("Couldn't reconstruct the partial data because " 
            + "old decoders don't support it");
      }
      Decoder decoder = (oldId.equals("xor"))? new XORDecoder(conf):
                                               new ReedSolomonDecoder(conf);
      CRC32 newCRC = null;
      long newLen = 0;
      if (!skipVerify) {
        newCRC = new CRC32();
        newLen = this.fixErasedBlockImpl(srcFs, srcFile, parityFs,
            parityFile, fixSource, blockSize, errorOffset, limit, partial, null,
            context, newCRC, null, false, null);
      }
      CRC32 oldCRC = (skipVerify && checksumStore == null)? null: new CRC32();
      long oldLen = decoder.fixErasedBlockImpl(srcFs, srcFile, parityFs,
          parityFile, fixSource, blockSize, errorOffset, limit, partial, out,
          context, oldCRC, si, false, null);
      
      if (!skipVerify) {
        if (newCRC.getValue() != oldCRC.getValue() ||
            newLen != oldLen) {
          LOG.error(" New code " + codec.id +
                    " produces different data from old code " + oldId +
                    " during fixing " + 
                    (fixSource ? srcFile.toString() : parityFile.toString())
                    + " (offset=" + errorOffset +
                    ", limit=" + limit + ")" +
                    " checksum:" + newCRC.getValue() + ", " + oldCRC.getValue() +
                    " len:" + newLen + ", " + oldLen);
          LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0, codec, -1,
              -1, -1, numReadBytes, numReadBytesRemoteRack,
              (fixSource ? srcFile : parityFile), errorOffset,
              LOGTYPES.OFFLINE_RECONSTRUCTION_SIMULATION, 
              (fixSource ? srcFs : parityFs), null, context, -1);
          
          if (context != null) {
            context.getCounter(RaidCounter.BLOCK_FIX_SIMULATION_FAILED).increment(1L);
            // The key includes the file path and simulation failure state
            String outkey = DistBlockIntegrityMonitor.SIMULATION_FAILED_FILE + ",";
            if (fixSource) {
              outkey += srcFile.toUri().getPath();
            } else {
              outkey += parityFile.toUri().getPath();
            }
            // The value is the task id
            String outval = context.getConfiguration().get("mapred.task.id");
            context.write(new Text(outkey), new Text(outval));
          }
        } else {
          LOG.info(" New code " + codec.id +
                   " produces the same data with old code " + oldId +
                   " during fixing " + 
                   (fixSource ? srcFile.toString() : parityFile.toString())
                   + " (offset=" + errorOffset +
                   ", limit=" + limit + ")"
                   );
          if (context != null) {
            context.getCounter(RaidCounter.BLOCK_FIX_SIMULATION_SUCCEEDED).increment(1L);
          }
        }
      }
      return oldCRC;
    } else {
      CRC32 crc = null;
      if (checksumStore != null) {
        crc = new CRC32();
      }
      fixErasedBlockImpl(srcFs, srcFile, parityFs, parityFile, fixSource, blockSize,
                         errorOffset, limit, partial, out, context, crc, si, false, null);
      return crc;
   }
  }
  
  boolean analysisStream(ParallelStreamReader parallelReader, FileSystem srcFs,
      FileSystem parityFs, boolean stripeVerified, StripeInfo si) 
          throws StripeMismatchException {
    //setting remoteRackFlag for each of the input streams and verify the stripe
    for (int i = 0 ; i < codec.parityLength + codec.stripeLength ; i++) {
      if (parallelReader.streams[i] instanceof DFSDataInputStream) {
        DFSDataInputStream stream =
            (DFSDataInputStream) parallelReader.streams[i];  
        if (i < codec.parityLength) {
          //Dealing with parity blocks
          remoteRackFlag[i] = 
              !(((DistributedFileSystem)parityFs).getClient().
                  isInLocalRack(NetUtils.createSocketAddr(
                      stream.getCurrentDatanode().getName())));
          if (LOG.isDebugEnabled()) {
            LOG.debug("RemoteRackFlag at index " + i + " is " + 
                remoteRackFlag[i]);
          }
          // Verify with parity Blocks
          if (stripeVerified == false) {
            Block currentBlock = stream.getCurrentBlock();
            if (!currentBlock.equals(si.parityBlocks.get(i))) {
              throw new StripeMismatchException("current block " +
                  currentBlock.toString() + " in position " + i + " doesn't " 
                  + "match stripe info:" + si);
            }
          }
        } else {
          //Dealing with Source (file) block
          remoteRackFlag[i] = 
              !(((DistributedFileSystem)srcFs).getClient().
                  isInLocalRack(NetUtils.createSocketAddr(
                      stream.getCurrentDatanode().getName())));
          if (LOG.isDebugEnabled()) {
            LOG.debug("RemoteRackFlag at index " + i + " is " + 
                remoteRackFlag[i]);
          }
          // Verify with source Blocks
          if (stripeVerified == false) {
            Block currentBlock = stream.getCurrentBlock();
            if (!currentBlock.equals(si.srcBlocks.get(
                i - codec.parityLength))) {
              throw new StripeMismatchException("current block " +
                  currentBlock.toString() + " in position " + i + " doesn't " 
                  + "match stripe info:" + si);
            }
          }
        }              
      } 
    }
    return true;
  }

  long fixErasedBlockImpl(FileSystem srcFs, Path srcFile, FileSystem parityFs,
      Path parityFile, boolean fixSource, long blockSize, long errorOffset,
      long limit, boolean partial, OutputStream out, Context context,
      CRC32 crc, StripeInfo si, boolean recoverFromStripeStore, Block lostBlock)
          throws IOException {
    
    Progressable reporter = context;
    
    if (reporter == null) {
      reporter = RaidUtils.NULL_PROGRESSABLE;
    }
    
    long startTime = System.currentTimeMillis();
    long decodingTime = 0;
    if (crc != null) {
      crc.reset();
    }
    int blockIdx = (int) (errorOffset/blockSize);
    LocationPair lp = null;
    int erasedLocationToFix;
    
    if (recoverFromStripeStore) {
      erasedLocationToFix = si.getBlockIdxInStripe(lostBlock);
    } else if (fixSource) {
      lp = StripeReader.getBlockLocation(codec, srcFs,
          srcFile, blockIdx, conf);
      erasedLocationToFix = codec.parityLength + lp.getBlockIdxInStripe(); 
    } else {
      lp = StripeReader.getParityBlockLocation(codec, blockIdx);
      erasedLocationToFix = lp.getBlockIdxInStripe();
    }

    FileStatus srcStat = srcFs.getFileStatus(srcFile);
    FileStatus parityStat = null;
    if (!recoverFromStripeStore) {
      parityStat = parityFs.getFileStatus(parityFile);
    }

    InputStream[] inputs = null;
    List<Integer> erasedLocations = new ArrayList<Integer>();
    // Start off with one erased location.
    erasedLocations.add(erasedLocationToFix);
    Set<Integer> locationsToNotRead = new HashSet<Integer>();

    int boundedBufferCapacity = 2;
    ParallelStreamReader parallelReader = null;
    LOG.info("Need to write " + limit +
             " bytes for erased location index " + erasedLocationToFix);
    
    long startOffsetInBlock = 0;
    if (partial) {
      startOffsetInBlock = errorOffset % blockSize;
    }
    
    try {
      int[] locationsToFix = new int[codec.parityLength];
      numReadBytes = 0;
      numReadBytesRemoteRack = 0;
      remoteRackFlag = new boolean[codec.parityLength + codec.stripeLength];
      for (int id = 0 ; id < codec.parityLength + codec.stripeLength ; id++) {
        remoteRackFlag[id] = false;
      }
      boolean stripeVerified = (si == null);
      long written;
      // Loop while the number of written bytes is less than the max.
      for (written = 0; written < limit; ) {
        try {
          if (parallelReader == null) {
            long offsetInBlock = written + startOffsetInBlock;
            if (recoverFromStripeStore) {
              inputs = StripeReader.buildInputsFromStripeInfo((DistributedFileSystem)srcFs, 
                  srcStat, codec, si, offsetInBlock, 
                  limit, erasedLocations, locationsToNotRead, code);
            } else {
              StripeReader sReader = StripeReader.getStripeReader(codec,
                conf, blockSize, srcFs, lp.getStripeIdx(), srcStat);
              inputs = sReader.buildInputs(srcFs, srcFile,
                srcStat, parityFs, parityFile, parityStat,
                lp.getStripeIdx(), offsetInBlock, erasedLocations,
                locationsToNotRead, code);
            }
            int i = 0;
            for (int location : locationsToNotRead) {
              locationsToFix[i] = location;
              i++;
            }
            
            assert(parallelReader == null);
            parallelReader = new ParallelStreamReader(reporter, inputs, 
              (int)Math.min(bufSize, limit),
              parallelism, boundedBufferCapacity,
              Math.min(limit, blockSize));
            parallelReader.start();
          }
          ParallelStreamReader.ReadResult readResult = readFromInputs(
            erasedLocations, limit, reporter, parallelReader);         
          
          stripeVerified = analysisStream(parallelReader, srcFs, parityFs, 
              stripeVerified, si);
           
          //Calculate the number of bytes read from remote rack (through top of rack)
          for (int i = 0 ; i < codec.parityLength + codec.stripeLength ; i++) {
            if (remoteRackFlag[i]) {
              numReadBytesRemoteRack += readResult.numRead[i];
            }
          }
          
          if (LOG.isDebugEnabled()) {
            LOG.debug("Number of bytes read through the top of rack is " +
                numReadBytesRemoteRack);
          }
          
          long startDecoding = System.currentTimeMillis();
          int toWrite = (int)Math.min((long)bufSize, limit - written);
          doParallelDecoding(toWrite, readResult, parallelCode, locationsToFix);
          decodingTime += (System.currentTimeMillis() - startDecoding); 
          
          // get the number of bytes read through hdfs.
          for (int readNum : readResult.numRead) {
            numReadBytes += readNum;
          }

          for (int i = 0; i < locationsToFix.length; i++) {
            if (locationsToFix[i] == erasedLocationToFix) {
              if (out != null)
                out.write(writeBufs[i], 0, toWrite);
              if (crc != null) {
                crc.update(writeBufs[i], 0, toWrite);
              }
              written += toWrite;
              break;
            }
          }
        } catch (IOException e) {
          LOG.warn("Exception in fixErasedBlockImpl: " + e, e);
          if (e instanceof TooManyErasedLocations) {
            LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0, codec,
                System.currentTimeMillis() - startTime, decodingTime,
                erasedLocations.size(), numReadBytes, numReadBytesRemoteRack,
                (fixSource ? srcFile : parityFile), errorOffset,
                LOGTYPES.OFFLINE_RECONSTRUCTION_TOO_MANY_CORRUPTIONS, 
                (fixSource ? srcFs : parityFs), e, context, -1);
            throw e;
          } else if (e instanceof StripeMismatchException) {
            LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0, codec,
                System.currentTimeMillis() - startTime, 
                erasedLocations.size(), -1, numReadBytes, numReadBytesRemoteRack,
                (fixSource ? srcFile : parityFile), errorOffset,
                LOGTYPES.OFFLINE_RECONSTRUCTION_STRIPE_VERIFICATION, 
                (fixSource ? srcFs : parityFs), e, context, -1);
            throw e;
          }
          // Re-create inputs from the new erased locations.
          if (parallelReader != null) {
            parallelReader.shutdown();
            parallelReader = null;
          }
          if (inputs != null) {
            RaidUtils.closeStreams(inputs);
          }
        }
      }
      
      LogUtils.logRaidReconstructionMetrics(LOGRESULTS.SUCCESS, written, codec,
          System.currentTimeMillis() - startTime, decodingTime,
          erasedLocations.size(), numReadBytes, numReadBytesRemoteRack,
          (fixSource ? srcFile : parityFile), errorOffset,
          LOGTYPES.OFFLINE_RECONSTRUCTION_BLOCK, 
          (fixSource ? srcFs : parityFs), null, context, -1);
      return written; 
    } finally {
      numMissingBlocksInStripe = erasedLocations.size();
      if (parallelReader != null) {
        parallelReader.shutdown();
      }
      if (inputs != null) {
        RaidUtils.closeStreams(inputs);
      }
      if (context != null) {
        context.getCounter(RaidCounter.FILE_FIX_NUM_READBYTES_REMOTERACK).
            increment(numReadBytesRemoteRack);
      } 
    }
  }
  
  public void doParallelDecoding(int toWrite, 
      ParallelStreamReader.ReadResult readResult,
      ErasureCode[] parallelCode,
      int[] locationsToFix) throws IOException {
    int partSize = (int) Math.ceil(toWrite * 1.0 / parallelism);
    
    // parallel decoding
    try {
      Thread[] workers = new Thread[parallelism];
      int start = 0;
      for (int i = 0; i < parallelism; i++) {
        int count = Math.min(toWrite - start, partSize);
        workers[i] = new Thread(new DecodeOp(
              readResult.readBufs, writeBufs, start, count,
              locationsToFix, parallelCode[i]));
        workers[i].start();
        start += count;
      }
      
      // wait for all threads
      for (int i = 0; i < parallelism; i++) {
        workers[i].join();
        workers[i] = null;
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for decoding");
    }
  }

  class DecodeOp implements Runnable {
    byte[][] readBufs;
    byte[][] writeBufs;
    int startIdx;
    int count;
    int[] erasedLocations;
    ErasureCode code;
    DecodeOp(byte[][] readBufs, byte[][] writeBufs,
             int startIdx, int count, int[] erasedLocations, 
             ErasureCode code) {
      this.readBufs = readBufs;
      this.writeBufs = writeBufs;
      this.startIdx = startIdx;
      this.count = count;
      this.erasedLocations = erasedLocations;
      this.code = code;
    }

    public void run() {
      try {
        code.decodeBulk(readBufs, writeBufs, erasedLocations, startIdx, count);
      } catch (IOException e) {
        LOG.error("Encountered Exception in DecodeBulk: ", e);
      }
    }
  }

  ParallelStreamReader.ReadResult readFromInputs(
          List<Integer> erasedLocations,
          long limit,
          Progressable reporter,
          ParallelStreamReader parallelReader) throws IOException {
    ParallelStreamReader.ReadResult readResult;
    try {
      readResult = parallelReader.getReadResult();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for read result");
    }

    IOException exceptionToThrow = null;
    // Process io errors, we can tolerate upto codec.parityLength errors.
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
      int newErasedLocation = i;
      erasedLocations.add(newErasedLocation);
      exceptionToThrow = e;
    }
    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
    return readResult;
  }
  
  public class DecoderInputStream extends InputStream {
    
    private long limit;
    private ParallelStreamReader parallelReader = null;
    private byte[] buffer;
    private long bufferLen;
    private int position;
    private long streamOffset = 0;
    
    private final Progressable reporter;
    private InputStream[] inputs;
    private final int boundedBufferCapacity = 2;
    
    private final long blockSize;
    private final long errorOffset;
    private long startOffsetInBlock;
    
    private final FileSystem srcFs;
    private final Path srcFile;
    private final FileSystem parityFs;
    private final Path parityFile;
    
    private int blockIdx;
    private int erasedLocationToFix;
    private LocationPair locationPair;
    
    private long currentOffset;
    private long dfsNumRead = 0;
    private int constructedBytes = 0;
    private boolean success = false;
    private long startTime = -1;
    private ErasureCode[] parallelCode;
   
    private final Set<Integer> locationsToNotRead = new HashSet<Integer>();
    private final List<Integer> erasedLocations = new ArrayList<Integer>();
    private final int[] locationsToFix = new int[codec.parityLength];

    private long decodingTime = 0;
    private Block lostBlock;
    private StripeInfo si;
    private boolean recoverFromStripeInfo;
    
    public DecoderInputStream(
        final Progressable reporter,
        final long limit,
        final long blockSize,
        final long errorOffset,
        final FileSystem srcFs, 
        final Path srcFile,
        final FileSystem parityFs, 
        final Path parityFile,
        Block lostBlock, 
        StripeInfo si, 
        boolean recoverFromStripeInfo) {
      
      this.reporter = reporter;
      this.limit = limit;
      
      this.blockSize = blockSize;
      this.errorOffset = errorOffset;
      
      this.srcFile = srcFile;
      this.srcFs = srcFs;
      this.parityFile = parityFile;
      this.parityFs = parityFs;
      
      this.blockIdx = (int) (errorOffset/blockSize);
      this.startOffsetInBlock = errorOffset % blockSize;
      this.currentOffset = errorOffset;
      
      this.lostBlock = lostBlock;
      this.si = si;
      this.recoverFromStripeInfo = recoverFromStripeInfo;
    }

    public Codec getCodec() {
      return codec;
    }
    
    public long getCurrentOffset() {
      return currentOffset;
    }
    
    public long getAvailable() {
      return limit - streamOffset;
    }
    
    /**
     * Will init the required objects, start the parallel reader, and 
     * put the decoding result in buffer in this method.
     * 
     * @throws IOException
     */
    private void init() throws IOException {
      if (startTime == -1) {
        startTime = System.currentTimeMillis();
      }
      
      if (streamOffset >= limit) {
        buffer = null;
        return;
      }
      
      if (null == locationPair) {
        if (recoverFromStripeInfo) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Reconstruct the lost block: " + lostBlock.getBlockId() + 
               " from stripe: " + si.toString());
          }
          erasedLocationToFix = si.getBlockIdxInStripe(lostBlock);
        } else {
          locationPair = StripeReader.getBlockLocation(codec, srcFs, 
              srcFile, blockIdx, conf);
          erasedLocationToFix = codec.parityLength + 
              locationPair.getBlockIdxInStripe();
        }
        erasedLocations.add(erasedLocationToFix);
      }

      if (null == parallelReader) {
        
        long offsetInBlock = streamOffset + startOffsetInBlock;
        FileStatus srcStat = srcFs.getFileStatus(srcFile);
        
        if (recoverFromStripeInfo) {
          inputs = StripeReader.buildInputsFromStripeInfo(
              (DistributedFileSystem)srcFs, srcStat, codec, si,
              offsetInBlock, limit, erasedLocations, locationsToNotRead, code);
        } else {
          FileStatus parityStat = parityFs.getFileStatus(parityFile);
          StripeReader sReader = StripeReader.getStripeReader(codec, conf, 
              blockSize, srcFs, locationPair.getStripeIdx(), srcStat);
          
          inputs = sReader.buildInputs(srcFs, srcFile, srcStat,
              parityFs, parityFile, parityStat,
              locationPair.getStripeIdx(), offsetInBlock,
              erasedLocations, locationsToNotRead, code);
        }
        int i = 0;
        for (int location : locationsToNotRead) {
          locationsToFix[i] = location;
          i++;
        }

        this.parallelCode = new ErasureCode[parallelism];
        for (int j = 0; j < parallelism; j++) {
          parallelCode[j] = codec.createErasureCode(conf);
        }

        assert(parallelReader == null);
        parallelReader = new ParallelStreamReader(reporter, inputs, 
            (int)Math.min(bufSize, limit),
            parallelism, boundedBufferCapacity, limit);
        parallelReader.start();
      }
    
      if (null != buffer && position == bufferLen) {
        buffer = null;
      }

      if (null == buffer) {
        ParallelStreamReader.ReadResult readResult = 
            readFromInputs(erasedLocations, limit, reporter, parallelReader);
        
        // get the number of bytes read through hdfs.
        for (int readNum : readResult.numRead) {
          dfsNumRead += readNum;
        }

        long startDecoding = System.currentTimeMillis();
        int toWrite = (int)Math.min((long)bufSize, limit - streamOffset);
        doParallelDecoding(toWrite, readResult, parallelCode, locationsToFix);
        
        for (int i=0; i<locationsToFix.length; i++) {
          if (locationsToFix[i] == erasedLocationToFix) {
            buffer = writeBufs[i];
            constructedBytes += writeBufs[i].length;
            bufferLen = Math.min(bufSize, limit - streamOffset);
            position = 0;
            break;
          }
        }
        success = true;
        decodingTime += (System.currentTimeMillis() - startDecoding);
      }
    }
    
    /**
     * make sure we have the correct decoding data in the buffer.
     * 
     * @throws IOException
     */
    private void checkBuffer() throws IOException {
      try {
        while (streamOffset <= limit) {
          try {
            init();
            break;
          } catch (IOException e) {
            if (e instanceof TooManyErasedLocations) {
              throw e;
            }
            // Re-create inputs from the new erased locations.
            if (parallelReader != null) {
              parallelReader.shutdown();
              parallelReader = null;
            }
            if (inputs != null) {
              RaidUtils.closeStreams(inputs);
            }
          }
        }
      } catch (IOException e) {
        success = false;
        long delay = System.currentTimeMillis() - startTime;
        LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0, codec, delay,
            decodingTime, erasedLocations.size(), dfsNumRead, -1, srcFile, errorOffset,
            LOGTYPES.ONLINE_RECONSTRUCTION, srcFs, e, null, -1);
        throw e;
      }
    }

    @Override
    public int read() throws IOException {
      
      checkBuffer();
      if (null == buffer) {
        return -1;
      }

      int result = buffer[position] & 0xff;
      position ++;
      streamOffset ++;
      currentOffset ++;

      return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      
      int numRead = 0;
      while (numRead < len) {
        checkBuffer();
        if (null == buffer) {
          if (numRead > 0) {
            return (int)numRead;
          }
          return -1;
        }
        
        int numBytesToCopy = (int) Math.min(bufferLen - position, 
            len - numRead);
        System.arraycopy(buffer, position, b, off, numBytesToCopy);
        position += numBytesToCopy;
        currentOffset += numBytesToCopy;
        streamOffset += numBytesToCopy;
        off += numBytesToCopy;
        numRead += numBytesToCopy;
      }
      return (int)numRead;
    }
    
    @Override
    public void close() throws IOException {
      if (success) {
        LogUtils.logRaidReconstructionMetrics(LOGRESULTS.SUCCESS, constructedBytes,
            codec, System.currentTimeMillis() - startTime, decodingTime,
            erasedLocations.size(), dfsNumRead, -1, srcFile, errorOffset,
            LOGTYPES.ONLINE_RECONSTRUCTION, srcFs, null, null, -1);
      }
      if (parallelReader != null) {
        parallelReader.shutdown();
        parallelReader = null;
      }
      if (inputs != null) {
        RaidUtils.closeStreams(inputs);
      }
      super.close();
    }
  }  
}


