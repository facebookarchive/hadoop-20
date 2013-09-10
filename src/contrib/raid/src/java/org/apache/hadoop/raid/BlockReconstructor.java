package org.apache.hadoop.raid;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.VersionAndOpcode;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.hdfs.protocol.WriteBlockHeader;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockDataFile;
import org.apache.hadoop.hdfs.server.datanode.BlockSender;
import org.apache.hadoop.hdfs.server.datanode.BlockWithChecksumFileReader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.raid.StripeStore.StripeInfo;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

/**
 * this class implements the actual reconstructing functionality
 * we keep this in a separate class so that 
 * the distributed block fixer can use it
 */ 
abstract class BlockReconstructor extends Configured {

  public static final Log LOG = LogFactory.getLog(BlockReconstructor.class);
  public static final int SEND_BLOCK_MAX_RETRIES = 3;

  BlockReconstructor(Configuration conf) throws IOException {
    super(conf);
  }

  /**
   * Is the path a parity file of a given Codec?
   */
  boolean isParityFile(Path p, Codec c) {
    return isParityFile(p.toUri().getPath(), c);
  }

  boolean isParityFile(String pathStr, Codec c) {
    if (pathStr.contains(RaidNode.HAR_SUFFIX)) {
      return false;
    }
    return pathStr.startsWith(c.getParityPrefix());
  }
  
  /**
   * Fix a file, report progess.
   *
   * @return true if file was reconstructed, false if no reconstruction 
   * was necessary or possible.
   */
  boolean reconstructFile(Path srcPath, Context context)
      throws IOException, InterruptedException {
    Progressable progress = context;
    if (progress == null) {
      progress = RaidUtils.NULL_PROGRESSABLE;
    }
    
    FileSystem fs = srcPath.getFileSystem(getConf());
    FileStatus srcStat = null;
    try {
      srcStat = fs.getFileStatus(srcPath);
    } catch (FileNotFoundException ex) {
      return false;
    }

    if (RaidNode.isParityHarPartFile(srcPath)) {
      return processParityHarPartFile(srcPath, progress);
    }

    // Reconstruct parity file
    for (Codec codec : Codec.getCodecs()) {
      if (isParityFile(srcPath, codec)) {
        Decoder decoder = new Decoder(getConf(), codec);
        decoder.connectToStore(srcPath);
        return processParityFile(srcPath, 
            decoder, context);
      }
    }

    // Reconstruct source file without connecting to stripe store 
    for (Codec codec : Codec.getCodecs()) {
      ParityFilePair ppair = ParityFilePair.getParityFile(
          codec, srcStat, getConf());
      if (ppair != null) {
        Decoder decoder = new Decoder(getConf(), codec);
        decoder.connectToStore(srcPath);
        return processFile(srcPath, ppair, decoder, false, context);
      }
    }
    // Reconstruct source file through stripe store
    for (Codec codec : Codec.getCodecs()) {
      if (!codec.isDirRaid) {
        continue;
      }
      try {
        // try to fix through the stripe store.
        Decoder decoder = new Decoder(getConf(), codec);
        decoder.connectToStore(srcPath);
        if (processFile(srcPath, null, decoder, true, context)) {
          return true;
        }
      } catch (Exception ex) {
        LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0,
            codec, srcPath, -1, LOGTYPES.OFFLINE_RECONSTRUCTION_USE_STRIPE,
            fs, ex, context);
      }
    }
    
    return false;    
  }

  /**
   * Sorts source files ahead of parity files.
   */
  void sortLostFiles(List<String> files) {
    // TODO: We should first fix the files that lose more blocks
    Comparator<String> comp = new Comparator<String>() {
      public int compare(String p1, String p2) {
        Codec c1 = null;
        Codec c2 = null;
        for (Codec codec : Codec.getCodecs()) {
          if (isParityFile(p1, codec)) {
            c1 = codec;
          } else if (isParityFile(p2, codec)) {
            c2 = codec;
          }
        }
        if (c1 == null && c2 == null) {
          return 0; // both are source files
        }
        if (c1 == null && c2 != null) {
          return -1; // only p1 is a source file
        }
        if (c2 == null && c1 != null) {
          return 1; // only p2 is a source file
        }
        return c2.priority - c1.priority; // descending order
      }
    };
    Collections.sort(files, comp);
  }

  /**
   * Returns a DistributedFileSystem hosting the path supplied.
   */
  protected DistributedFileSystem getDFS(Path p) throws IOException {
    FileSystem fs = p.getFileSystem(getConf());
    DistributedFileSystem dfs = null;
    if (fs instanceof DistributedFileSystem) {
      dfs = (DistributedFileSystem) fs;
    } else if (fs instanceof FilterFileSystem) {
      FilterFileSystem ffs = (FilterFileSystem) fs;
      if (ffs.getRawFileSystem() instanceof DistributedFileSystem) {
        dfs = (DistributedFileSystem) ffs.getRawFileSystem();
      }
    }
    return dfs;
  }

  /**
   * Throw exceptions for blocks with lost checksums or stripes
   */
  void checkLostBlocks(List<Block> blocksLostChecksum,
      List<Block> blocksLostStripe, Path p, Codec codec)
      throws IOException {
    StringBuilder message = new StringBuilder();
    if (blocksLostChecksum.size() > 0) {
      message.append("Lost " + blocksLostChecksum.size() +
          " checksums in blocks:");
      for (Block blk : blocksLostChecksum) {
        message.append(" ");
        message.append(blk.toString());
      }
    }
    if (blocksLostStripe.size() > 0) {
      message.append("Lost " + blocksLostStripe.size() +
          " stripes in blocks:");
      for (Block blk : blocksLostStripe) {
        message.append(" ");
        message.append(blk.toString());
      }
    }
    if (message.length() == 0)
      return;
    message.append(" in file " + p);
    throw new IOException(message.toString());
  }
  
  private boolean abortReconstruction(Long oldCRC, Decoder decoder) {
    // If current codec is simulated file-level raid,
    // We assume we only have XOR and RS
    // it's allowed to lose checksums
    return oldCRC == null && decoder.checksumStore != null &&
        (decoder.codec.isDirRaid ||
        !decoder.codec.simulateBlockFix ||
         decoder.requiredChecksumVerification);
  }
  
  /**
   * Reads through a source file reconstructing lost blocks on the way.
   * @param srcPath Path identifying the lost file.
   * @throws IOException
   * @return true if file was reconstructed, false if no reconstruction 
   * was necessary or possible.
   */
  public boolean processFile(Path srcPath, ParityFilePair parityPair,
      Decoder decoder, Boolean fromStripeStore, Context context) 
          throws IOException,
      InterruptedException {
    LOG.info("Processing file " + srcPath);
    Progressable progress = context;
    if (progress == null) {
      progress = RaidUtils.NULL_PROGRESSABLE;
    }
    
    DistributedFileSystem srcFs = getDFS(srcPath);
    FileStatus srcStat = srcFs.getFileStatus(srcPath);
    long blockSize = srcStat.getBlockSize();
    long srcFileSize = srcStat.getLen();
    String uriPath = srcPath.toUri().getPath();

    int numBlocksReconstructed = 0;
    List<LocatedBlockWithMetaInfo> lostBlocks = lostBlocksInFile(srcFs,
        uriPath, srcStat);
    if (lostBlocks.size() == 0) {
      LOG.warn("Couldn't find any lost blocks in file " + srcPath + 
          ", ignoring...");
      return false;
    }
    List<Block> blocksLostChecksum = new ArrayList<Block>();
    List<Block> blocksLostStripe = new ArrayList<Block>();
    
    for (LocatedBlockWithMetaInfo lb: lostBlocks) {
      Block lostBlock = lb.getBlock();
      long lostBlockOffset = lb.getStartOffset();

      LOG.info("Found lost block " + lostBlock +
          ", offset " + lostBlockOffset);
      Long oldCRC = decoder.retrieveChecksum(lostBlock, 
          srcPath, lostBlockOffset, srcFs, context);
      if (abortReconstruction(oldCRC, decoder)) {
        blocksLostChecksum.add(lostBlock);
        continue;
      }
      StripeInfo si = decoder.retrieveStripe(lostBlock, 
          srcPath, lostBlockOffset, srcFs, context, false);
      if (si == null && decoder.stripeStore != null) {
        blocksLostStripe.add(lostBlock);
        continue;
      }

      final long blockContentsSize =
        Math.min(blockSize, srcFileSize - lostBlockOffset);
      File localBlockFile =
        File.createTempFile(lostBlock.getBlockName(), ".tmp");
      localBlockFile.deleteOnExit();

      try {
        CRC32 crc = null;
        
        if (fromStripeStore) {
          crc = decoder.recoverBlockToFileFromStripeInfo(srcFs, srcPath,
              lostBlock, localBlockFile, blockSize,
              lostBlockOffset, blockContentsSize, si, context);
        } else {
          crc = decoder.recoverBlockToFile(srcFs, srcStat,
                        parityPair.getFileSystem(),
                        parityPair.getPath(), blockSize,
                        lostBlockOffset, localBlockFile,
                        blockContentsSize, si, context);
        }
        LOG.info("Recovered crc: " + ((crc == null)?null: crc.getValue()) +
            " expected crc:" + oldCRC);
        if (crc != null && oldCRC != null &&
            crc.getValue() != oldCRC) {
          // checksum doesn't match, it's dangerous to send it
          IOException ioe = new IOException("Block " + lostBlock.toString() +
              " new checksum " + crc.getValue() +
              " doesn't match the old one " + oldCRC);
          LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0,
              decoder.codec, srcPath, lostBlockOffset,
              LOGTYPES.OFFLINE_RECONSTRUCTION_CHECKSUM_VERIFICATION,
              srcFs, ioe, context);
          throw ioe;
        }
        // Now that we have recovered the file block locally, send it.
        computeMetadataAndSendReconstructedBlock(localBlockFile,
            lostBlock, blockContentsSize,
            lb.getLocations(),
            lb.getDataProtocolVersion(), 
            lb.getNamespaceID(), 
            progress);
        
        numBlocksReconstructed++;

      } finally {
        localBlockFile.delete();
      }
      progress.progress();
    }
    
    LOG.info("Reconstructed " + numBlocksReconstructed + " blocks in " + srcPath);
    checkLostBlocks(blocksLostChecksum, blocksLostStripe, srcPath, decoder.codec);
    return true;
  }

  /**
   * Reads through a parity file, reconstructing lost blocks on the way.
   * This function uses the corresponding source file to regenerate parity
   * file blocks.
   * @return true if file was reconstructed, false if no reconstruction 
   * was necessary or possible.
   */
  boolean processParityFile(Path parityPath, Decoder decoder, 
      Context context)
  throws IOException, InterruptedException {
    LOG.info("Processing parity file " + parityPath);
    
    Progressable progress = context;
    if (progress == null) {
      progress = RaidUtils.NULL_PROGRESSABLE;
    }
    DistributedFileSystem parityFs = getDFS(parityPath);
    Path srcPath = RaidUtils.sourcePathFromParityPath(parityPath, parityFs);
    if (srcPath == null) {
      LOG.warn("Could not get regular file corresponding to parity file " +  
          parityPath + ", ignoring...");
      return false;
    }
    
    DistributedFileSystem srcFs = getDFS(srcPath);
    FileStatus parityStat = parityFs.getFileStatus(parityPath);
    long blockSize = parityStat.getBlockSize();
    FileStatus srcStat = srcFs.getFileStatus(srcPath);

    // Check timestamp.
    if (srcStat.getModificationTime() != parityStat.getModificationTime()) {
      LOG.warn("Mismatching timestamp for " + srcPath + " and " + parityPath + 
          ", ignoring...");
      return false;
    }

    String uriPath = parityPath.toUri().getPath();
    int numBlocksReconstructed = 0;
    List<LocatedBlockWithMetaInfo> lostBlocks = 
      lostBlocksInFile(parityFs, uriPath, parityStat);
    if (lostBlocks.size() == 0) {
      LOG.warn("Couldn't find any lost blocks in parity file " + parityPath + 
          ", ignoring...");
      return false;
    }
    List<Block> blocksLostChecksum = new ArrayList<Block>();
    List<Block> blocksLostStripe = new ArrayList<Block>();
    
    for (LocatedBlockWithMetaInfo lb: lostBlocks) {
      Block lostBlock = lb.getBlock();
      long lostBlockOffset = lb.getStartOffset();
      LOG.info("Found lost block " + lostBlock +
          ", offset " + lostBlockOffset);
      
      Long oldCRC = decoder.retrieveChecksum(lostBlock,
          parityPath, lostBlockOffset, parityFs, context);
      if (abortReconstruction(oldCRC, decoder)) {
        blocksLostChecksum.add(lostBlock);
        continue;
      }
      StripeInfo si = decoder.retrieveStripe(lostBlock,
          srcPath, lostBlockOffset, srcFs, context, false);
      if (si == null && decoder.stripeStore != null) {
        blocksLostStripe.add(lostBlock);
        continue;
      }
      
      File localBlockFile =
        File.createTempFile(lostBlock.getBlockName(), ".tmp");
      localBlockFile.deleteOnExit();

      try {
        CRC32 crc = decoder.recoverParityBlockToFile(srcFs, srcStat, parityFs,
            parityPath, blockSize, lostBlockOffset, localBlockFile, si,
            context);
        LOG.info("Recovered crc: " + ((crc == null)?null: crc.getValue()) +
            " expected crc:" + oldCRC);
        if (crc != null && oldCRC != null &&
            crc.getValue() != oldCRC) {
          // checksum doesn't match, it's dangerous to send it
          IOException ioe = new IOException("Block " + lostBlock.toString()
              + " new checksum " + crc.getValue()
              + " doesn't match the old one " + oldCRC);
          LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, 0,
              decoder.codec, parityPath, lostBlockOffset,
              LOGTYPES.OFFLINE_RECONSTRUCTION_CHECKSUM_VERIFICATION,
              parityFs, ioe, context);
          throw ioe;
        }
        // Now that we have recovered the parity file block locally, send it.
        computeMetadataAndSendReconstructedBlock(
            localBlockFile, 
            lostBlock, blockSize, lb.getLocations(),
            lb.getDataProtocolVersion(), lb.getNamespaceID(),
            progress);

        numBlocksReconstructed++;
      } finally {
        localBlockFile.delete();
      }
      progress.progress();
    }
    
    LOG.info("Reconstructed " + numBlocksReconstructed + " blocks in " + parityPath);
    checkLostBlocks(blocksLostChecksum, blocksLostStripe, parityPath, decoder.codec);
    return true;
  }

  /**
   * Reads through a parity HAR part file, reconstructing lost blocks on the way.
   * A HAR block can contain many file blocks, as long as the HAR part file
   * block size is a multiple of the file block size.
   * @return true if file was reconstructed, false if no reconstruction 
   * was necessary or possible.
   */
  boolean processParityHarPartFile(Path partFile,
      Progressable progress)
  throws IOException {
    LOG.info("Processing parity HAR file " + partFile);
    // Get some basic information.
    DistributedFileSystem dfs = getDFS(partFile);
    FileStatus partFileStat = dfs.getFileStatus(partFile);
    long partFileBlockSize = partFileStat.getBlockSize();
    LOG.info(partFile + " has block size " + partFileBlockSize);

    // Find the path to the index file.
    // Parity file HARs are only one level deep, so the index files is at the
    // same level as the part file.
    // Parses through the HAR index file.
    HarIndex harIndex = HarIndex.getHarIndex(dfs, partFile);
    String uriPath = partFile.toUri().getPath();
    int numBlocksReconstructed = 0;
    List<LocatedBlockWithMetaInfo> lostBlocks = lostBlocksInFile(dfs, uriPath, 
        partFileStat);
    if (lostBlocks.size() == 0) {
      LOG.warn("Couldn't find any lost blocks in HAR file " + partFile + 
          ", ignoring...");
      return false;
    }
    for (LocatedBlockWithMetaInfo lb: lostBlocks) {
      Block lostBlock = lb.getBlock();
      long lostBlockOffset = lb.getStartOffset();

      File localBlockFile =
        File.createTempFile(lostBlock.getBlockName(), ".tmp");
      localBlockFile.deleteOnExit();

      try {
        processParityHarPartBlock(dfs, partFile, 
            lostBlockOffset, partFileStat, harIndex,
            localBlockFile, progress);
        
        // Now that we have recovered the part file block locally, send it.
        computeMetadataAndSendReconstructedBlock(localBlockFile,
            lostBlock, 
            localBlockFile.length(),
            lb.getLocations(),
            lb.getDataProtocolVersion(), lb.getNamespaceID(),
            progress);
        
        numBlocksReconstructed++;
      } finally {
        localBlockFile.delete();
      }
      progress.progress();
    }
    
    LOG.info("Reconstructed " + numBlocksReconstructed + " blocks in " + partFile);
    return true;
  }

  /**
   * This reconstructs a single part file block by recovering in sequence each
   * parity block in the part file block.
   */
  private void processParityHarPartBlock(FileSystem dfs, Path partFile,
      long blockOffset,
      FileStatus partFileStat,
      HarIndex harIndex,
      File localBlockFile,
      Progressable progress)
  throws IOException {
    String partName = partFile.toUri().getPath(); // Temporarily.
    partName = partName.substring(1 + partName.lastIndexOf(Path.SEPARATOR));

    OutputStream out = new FileOutputStream(localBlockFile);

    try {
      // A HAR part file block could map to several parity files. We need to
      // use all of them to recover this block.
      final long blockEnd = Math.min(blockOffset + 
          partFileStat.getBlockSize(),
          partFileStat.getLen());
      for (long offset = blockOffset; offset < blockEnd; ) {
        HarIndex.IndexEntry entry = harIndex.findEntry(partName, offset);
        if (entry == null) {
          String msg = "Lost index file has no matching index entry for " +
          partName + ":" + offset;
          LOG.warn(msg);
          throw new IOException(msg);
        }
        Path parityFile = new Path(entry.fileName);
        Encoder encoder = null;
        for (Codec codec : Codec.getCodecs()) {
          if (isParityFile(parityFile, codec)) {
            encoder = new Encoder(getConf(), codec);
          }
        }
        if (encoder == null) {
          String msg = "Could not figure out codec correctly for " + parityFile;
          LOG.warn(msg);
          throw new IOException(msg);
        }
        Path srcFile = RaidUtils.sourcePathFromParityPath(parityFile, dfs);
        if (null == srcFile) {
          String msg = "Can not find the source path for parity file: " +
                          parityFile;
          LOG.warn(msg);
          throw new IOException(msg);
        }
        FileStatus srcStat = dfs.getFileStatus(srcFile);
        if (srcStat.getModificationTime() != entry.mtime) {
          String msg = "Modification times of " + parityFile + " and " +
          srcFile + " do not match.";
          LOG.warn(msg);
          throw new IOException(msg);
        }
        long lostOffsetInParity = offset - entry.startOffset;
        LOG.info(partFile + ":" + offset + " maps to " +
            parityFile + ":" + lostOffsetInParity +
            " and will be recovered from " + srcFile);
        encoder.recoverParityBlockToStream(dfs, srcStat,
            srcStat.getBlockSize(), parityFile,
            lostOffsetInParity, out, progress);
        // Finished recovery of one parity block. Since a parity block has the
        // same size as a source block, we can move offset by source block 
        // size.
        offset += srcStat.getBlockSize();
        LOG.info("Recovered " + srcStat.getBlockSize() + " part file bytes ");
        if (offset > blockEnd) {
          String msg =
            "Recovered block spills across part file blocks. Cannot continue";
          throw new IOException(msg);
        }
        progress.progress();
      }
    } finally {
      out.close();
    }
  }

  /**
   * Choose a datanode (hostname:portnumber). The datanode is chosen at
   * random from the live datanodes.
   * @param locationsToAvoid locations to avoid.
   * @return A chosen datanode.
   * @throws IOException
   */
  private DatanodeInfo chooseDatanode(DatanodeInfo[] locationsToAvoid,
                          DatanodeInfo[] live)
  throws IOException {
    LOG.info("Choosing a datanode from " + live.length +
        " live nodes while avoiding " + locationsToAvoid.length);
    Random rand = new Random();
    DatanodeInfo chosen = null;
    int maxAttempts = 1000;
    for (int i = 0; i < maxAttempts && chosen == null; i++) {
      int idx = rand.nextInt(live.length);
      chosen = live[idx];
      for (DatanodeInfo avoid: locationsToAvoid) {
        if (chosen.equals(avoid)) {
          LOG.info("Avoiding " + avoid.name);
          chosen = null;
          break;
        }
      }
    }
    if (chosen == null) {
      throw new IOException("Could not choose datanode");
    }
    LOG.info("Choosing datanode " + chosen.name);
    return chosen;
  }
  
  private DatanodeInfo chooseDatanode(DatanodeInfo[] locationsToAvoid) 
                throws IOException {
    DistributedFileSystem dfs = getDFS(new Path("/"));
    DatanodeInfo[] live =
        dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
    return chooseDatanode(locationsToAvoid, live);
  }

  /**
   * Reads data from the data stream provided and computes metadata.
   */
  DataInputStream computeMetadata(Configuration conf, InputStream dataStream)
  throws IOException {
    ByteArrayOutputStream mdOutBase = new ByteArrayOutputStream(1024*1024);
    DataOutputStream mdOut = new DataOutputStream(mdOutBase);

    // First, write out the version.
    mdOut.writeShort(FSDataset.FORMAT_VERSION_NON_INLINECHECKSUM);

    // Create a summer and write out its header.
    int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    DataChecksum sum =
      DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
          bytesPerChecksum);
    sum.writeHeader(mdOut);

    // Buffer to read in a chunk of data.
    byte[] buf = new byte[bytesPerChecksum];
    // Buffer to store the checksum bytes.
    byte[] chk = new byte[sum.getChecksumSize()];

    // Read data till we reach the end of the input stream.
    int bytesSinceFlush = 0;
    while (true) {
      // Read some bytes.
      int bytesRead = dataStream.read(buf, bytesSinceFlush, 
          bytesPerChecksum - bytesSinceFlush);
      if (bytesRead == -1) {
        if (bytesSinceFlush > 0) {
          boolean reset = true;
          sum.writeValue(chk, 0, reset); // This also resets the sum.
          // Write the checksum to the stream.
          mdOut.write(chk, 0, chk.length);
          bytesSinceFlush = 0;
        }
        break;
      }
      // Update the checksum.
      sum.update(buf, bytesSinceFlush, bytesRead);
      bytesSinceFlush += bytesRead;

      // Flush the checksum if necessary.
      if (bytesSinceFlush == bytesPerChecksum) {
        boolean reset = true;
        sum.writeValue(chk, 0, reset); // This also resets the sum.
        // Write the checksum to the stream.
        mdOut.write(chk, 0, chk.length);
        bytesSinceFlush = 0;
      }
    }

    byte[] mdBytes = mdOutBase.toByteArray();
    return new DataInputStream(new ByteArrayInputStream(mdBytes));
  }

  private void computeMetadataAndSendReconstructedBlock(
      File localBlockFile,
      Block block, long blockSize,
      DatanodeInfo[] locations,
      int dataTransferVersion,
      int namespaceId,
      Progressable progress)
  throws IOException {

    LOG.info("Computing metdata");
    FileInputStream blockContents = null;
    DataInputStream blockMetadata = null;
    try {
      blockContents = new FileInputStream(localBlockFile);
      blockMetadata = computeMetadata(getConf(), blockContents);
      blockContents.close();
      progress.progress();
      DatanodeInfo datanode = null;
      
      DistributedFileSystem dfs = getDFS(new Path("/"));
      DatanodeInfo[] live =
          dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
      
      for (int retry = 0; retry < SEND_BLOCK_MAX_RETRIES; ++retry) {
        try {
          datanode = chooseDatanode(locations, live);
          // Reopen
          blockContents = new FileInputStream(localBlockFile);
          sendReconstructedBlock(datanode.name, blockContents, blockMetadata, block, 
              blockSize, dataTransferVersion, namespaceId, progress);
          return;
        } catch (IOException ex) {
          if (retry == SEND_BLOCK_MAX_RETRIES - 1) {
            // last retry, rethrow the exception
            throw ex;
          }
          
          // log the warn and retry
          LOG.warn("Got exception when sending the reconstructed block to datanode " + 
                  datanode + ", retried: " + retry + " times.", ex);
          
          // add the bad node to the locations.
          DatanodeInfo[] newLocations = new DatanodeInfo[locations.length + 1];
          System.arraycopy(locations, 0, newLocations, 0, locations.length);
          newLocations[locations.length] = datanode;
          locations = newLocations;
        }
      }
    } finally {
      if (blockContents != null) {
        blockContents.close();
        blockContents = null;
      }
      if (blockMetadata != null) {
        blockMetadata.close();
        blockMetadata = null;
      }
    }
  }

  /**
   * Send a generated block to a datanode.
   * @param datanode Chosen datanode name in host:port form.
   * @param blockContents Stream with the block contents.
   * @param block Block object identifying the block to be sent.
   * @param blockSize size of the block.
   * @param dataTransferVersion the data transfer version
   * @param namespaceId namespace id the block belongs to
   * @throws IOException
   */
  private void sendReconstructedBlock(String datanode,
      final FileInputStream blockContents,
      final DataInputStream metadataIn,
      Block block, long blockSize,
      int dataTransferVersion, int namespaceId,
      Progressable progress) 
  throws IOException {
    InetSocketAddress target = NetUtils.createSocketAddr(datanode);
    Socket sock = SocketChannel.open().socket();

    int readTimeout =
      getConf().getInt(BlockIntegrityMonitor.BLOCKFIX_READ_TIMEOUT, 
          HdfsConstants.READ_TIMEOUT);
    NetUtils.connect(sock, target, readTimeout);
    sock.setSoTimeout(readTimeout);

    int writeTimeout = getConf().getInt(BlockIntegrityMonitor.BLOCKFIX_WRITE_TIMEOUT,
        HdfsConstants.WRITE_TIMEOUT);

    OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
    DataOutputStream out =
      new DataOutputStream(new BufferedOutputStream(baseStream, 
          FSConstants.
          SMALL_BUFFER_SIZE));

    boolean corruptChecksumOk = false;
    boolean chunkOffsetOK = false;
    boolean verifyChecksum = true;
    boolean transferToAllowed = false;

    try {
      LOG.info("Sending block " + block +
          " from " + sock.getLocalSocketAddress().toString() +
          " to " + sock.getRemoteSocketAddress().toString());
      BlockSender blockSender = 
        new BlockSender(namespaceId, block, blockSize, 0, blockSize,
            corruptChecksumOk, chunkOffsetOK, verifyChecksum,
            transferToAllowed, dataTransferVersion >= DataTransferProtocol.PACKET_INCLUDE_VERSION_VERSION,
            new BlockWithChecksumFileReader.InputStreamWithChecksumFactory() {
              @Override
              public InputStream createStream(long offset) throws IOException {
                // we are passing 0 as the offset above,
                // so we can safely ignore
                // the offset passed
                return blockContents; 
              }

              @Override
              public DataInputStream getChecksumStream() throws IOException {
                return metadataIn;
              }

            @Override
            public BlockDataFile.Reader getBlockDataFileReader()
                throws IOException {
              return BlockDataFile.getDummyDataFileFromFileChannel(
                  blockContents.getChannel()).getReader(null);
            }
          });

      WriteBlockHeader header = new WriteBlockHeader(new VersionAndOpcode(
          dataTransferVersion, DataTransferProtocol.OP_WRITE_BLOCK));
      header.set(namespaceId, block.getBlockId(), block.getGenerationStamp(),
          0, false, true, new DatanodeInfo(), 0, null, "");
      header.writeVersionAndOpCode(out);
      header.write(out);
      blockSender.sendBlock(out, baseStream, null, progress);

      LOG.info("Sent block " + block + " to " + datanode);
    } finally {
      sock.close();
      out.close();
    }
  }

  /**
   * Returns the lost blocks in a file.
   */
  abstract List<LocatedBlockWithMetaInfo> lostBlocksInFile(
      DistributedFileSystem fs,
      String uriPath, FileStatus stat)
      throws IOException;

  
  /**
   * This class implements corrupt block fixing functionality.
   */
  public static class CorruptBlockReconstructor extends BlockReconstructor {
    
    public CorruptBlockReconstructor(Configuration conf) throws IOException {
      super(conf);
    }

    
    List<LocatedBlockWithMetaInfo> lostBlocksInFile(DistributedFileSystem fs,
                                        String uriPath, 
                                        FileStatus stat)
        throws IOException {
      
      List<LocatedBlockWithMetaInfo> corrupt =
        new LinkedList<LocatedBlockWithMetaInfo>();
      VersionedLocatedBlocks locatedBlocks;
      int namespaceId = 0;
      int methodFingerprint = 0;
      if (DFSClient.isMetaInfoSuppoted(fs.getClient().namenodeProtocolProxy)) {
        LocatedBlocksWithMetaInfo lbksm = fs.getClient().namenode.
                  openAndFetchMetaInfo(uriPath, 0, stat.getLen());
        namespaceId = lbksm.getNamespaceID();
        locatedBlocks = lbksm;
        methodFingerprint = lbksm.getMethodFingerPrint();
        fs.getClient().getNewNameNodeIfNeeded(methodFingerprint);
      } else {
        locatedBlocks = fs.getClient().namenode.open(uriPath, 0, stat.getLen());
      }
      final int dataTransferVersion = locatedBlocks.getDataProtocolVersion();
      for (LocatedBlock b: locatedBlocks.getLocatedBlocks()) {
        if (b.isCorrupt() ||
            (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
          corrupt.add(new LocatedBlockWithMetaInfo(b.getBlock(),
              b.getLocations(), b.getStartOffset(),
              dataTransferVersion, namespaceId, methodFingerprint));
        }
      }
      return corrupt;
    }
  }
  
  /**
   * This class implements decommissioning block copying functionality.
   */
  public static class DecommissioningBlockReconstructor extends BlockReconstructor {

    public DecommissioningBlockReconstructor(Configuration conf) throws IOException {
      super(conf);
    }

    List<LocatedBlockWithMetaInfo> lostBlocksInFile(DistributedFileSystem fs,
                                           String uriPath,
                                           FileStatus stat)
        throws IOException {

      List<LocatedBlockWithMetaInfo> decommissioning = 
        new LinkedList<LocatedBlockWithMetaInfo>();
      VersionedLocatedBlocks locatedBlocks;
      int namespaceId = 0;
      int methodFingerprint = 0;
      if (DFSClient.isMetaInfoSuppoted(fs.getClient().namenodeProtocolProxy)) {
        LocatedBlocksWithMetaInfo lbksm = fs.getClient().namenode.
                  openAndFetchMetaInfo(uriPath, 0, stat.getLen());
        namespaceId = lbksm.getNamespaceID();
        locatedBlocks = lbksm;
        methodFingerprint = lbksm.getMethodFingerPrint();
        fs.getClient().getNewNameNodeIfNeeded(methodFingerprint);
      } else {
        locatedBlocks = fs.getClient().namenode.open(uriPath, 0, stat.getLen());
      }
      final int dataTransferVersion = locatedBlocks.getDataProtocolVersion();


      for (LocatedBlock b : locatedBlocks.getLocatedBlocks()) {
        if (b.isCorrupt() ||
            (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
          // If corrupt, this block is the responsibility of the CorruptBlockReconstructor
          continue;
        }

        // Copy this block iff all good copies are being decommissioned
        boolean allDecommissioning = true;
        for (DatanodeInfo i : b.getLocations()) {
          allDecommissioning &= i.isDecommissionInProgress();
        }
        if (allDecommissioning) {
          decommissioning.add(new LocatedBlockWithMetaInfo(b.getBlock(),
              b.getLocations(), b.getStartOffset(),
              dataTransferVersion, namespaceId, methodFingerprint));
        }
      }
      return decommissioning;
    }

  }


}
