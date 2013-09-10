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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.raid.StripeReader.LocationPair;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

public class RaidUtils {
  public static final Log LOG = LogFactory.getLog(RaidUtils.class);
  public static Progressable NULL_PROGRESSABLE = new Progressable() {
    /**
     * Do nothing.
     **/
    @Override
    public void progress() {
    }
  };

  /**
   * Removes files matching the trash/tmp file pattern.
   */
  public static void filterTrash(Configuration conf, List<String> files) {
    filterTrash(conf, files.iterator());
  }

  public static void filterTrash(Configuration conf, Iterator<String> fileIt) {
    // Remove files under Trash.
    String trashPattern = conf.get("raid.blockfixer.trash.pattern",
                                   "^/user/.*/\\.Trash.*|^/tmp/.*");
    Pattern compiledPattern = Pattern.compile(trashPattern);
    while (fileIt.hasNext()) {
      Matcher m = compiledPattern.matcher(fileIt.next());
      if (m.matches()) {
        fileIt.remove();
      }
    }
  }
  
  /**
   * holds raid type and parity file pair
   */
  public static class RaidInfo {
    public RaidInfo(final Codec codec,
                    final ParityFilePair parityPair,
                    final int parityBlocksPerStripe) {
      this.codec = codec;
      this.parityPair = parityPair;
      this.parityBlocksPerStripe = parityBlocksPerStripe;
    }
    public final Codec codec;
    public final ParityFilePair parityPair;
    public final int parityBlocksPerStripe;
  }
  
  public static RaidInfo getFileRaidInfo(final FileStatus stat,
      Configuration conf) throws IOException {
    return getFileRaidInfo(stat, conf, false);
  }

  /**
   * returns the raid for a given file
   */
  public static RaidInfo getFileRaidInfo(final FileStatus stat, 
      Configuration conf, boolean skipHarChecking)
    throws IOException {
    // now look for the parity file
    ParityFilePair ppair = null;
    for (Codec c : Codec.getCodecs()) {
      ppair = ParityFilePair.getParityFile(c, stat, conf, skipHarChecking);
      if (ppair != null) {
        return new RaidInfo(c, ppair, c.parityLength);
      }
    }
    return new RaidInfo(null, ppair, 0);
  }
  
  public static void collectFileCorruptBlocksInStripe(
      final DistributedFileSystem dfs, 
      final RaidInfo raidInfo, final FileStatus fileStatus, 
      final Map<Integer, Integer> corruptBlocksPerStripe)
          throws IOException {
    // read conf
    final int stripeBlocks = raidInfo.codec.stripeLength;

    // figure out which blocks are missing/corrupted
    final Path filePath = fileStatus.getPath();
    final long blockSize = fileStatus.getBlockSize();
    final long fileLength = fileStatus.getLen();
    final long fileLengthInBlocks = RaidNode.numBlocks(fileStatus); 
    final long fileStripes = RaidNode.numStripes(fileLengthInBlocks,
        stripeBlocks);
    final BlockLocation[] fileBlocks = 
      dfs.getFileBlockLocations(fileStatus, 0, fileLength);
    
    // figure out which stripes these corrupted blocks belong to
    for (BlockLocation fileBlock: fileBlocks) {
      int blockNo = (int) (fileBlock.getOffset() / blockSize);
      final int stripe = blockNo / stripeBlocks;
      if (isBlockCorrupt(fileBlock)) {
        incCorruptBlocksPerStripe(corruptBlocksPerStripe, stripe);
        if (LOG.isDebugEnabled()) {
          LOG.debug("file " + filePath.toString() + " corrupt in block " + 
                   blockNo + "/" + fileLengthInBlocks + ", stripe " + stripe +
                   "/" + fileStripes);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("file " + filePath.toString() + " OK in block " + blockNo +
                   "/" + fileLengthInBlocks + ", stripe " + stripe + "/" +
                   fileStripes);
        }
      }
    }
    checkParityBlocks(filePath, corruptBlocksPerStripe, blockSize, 0, fileStripes,
                      fileStripes, raidInfo);
  }
  
  public static void collectDirectoryCorruptBlocksInStripe(
      final Configuration conf,
      final DistributedFileSystem dfs, 
      final RaidInfo raidInfo, final FileStatus fileStatus, 
      Map<Integer, Integer> corruptBlocksPerStripe)
          throws IOException {
    final int stripeSize = raidInfo.codec.stripeLength;
    final Path filePath = fileStatus.getPath();
    final BlockLocation[] fileBlocks = 
      dfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    LocationPair lp = StripeReader.getBlockLocation(raidInfo.codec, dfs,
        filePath, 0, conf, raidInfo.parityPair.getListFileStatus());
    int startBlockIdx = lp.getStripeIdx() * stripeSize +
        lp.getBlockIdxInStripe();
    int startStripeIdx = lp.getStripeIdx();
    long endStripeIdx = RaidNode.numStripes((long)(startBlockIdx + fileBlocks.length),
        stripeSize);
    long blockSize = DirectoryStripeReader.getParityBlockSize(conf,
        lp.getListFileStatus());
    long numBlocks = DirectoryStripeReader.getBlockNum(lp.getListFileStatus());
    HashMap<Integer, Integer> allCorruptBlocksPerStripe = 
        new HashMap<Integer, Integer>();
    checkParityBlocks(filePath, allCorruptBlocksPerStripe, blockSize,
        startStripeIdx, endStripeIdx,
        RaidNode.numStripes(numBlocks, stripeSize), raidInfo);
    DirectoryStripeReader sReader = new DirectoryStripeReader(conf,
        raidInfo.codec, dfs, lp.getStripeIdx(), -1L, filePath.getParent(),
        lp.getListFileStatus());
    // Get the corrupt block information for all stripes related to the file
    while (sReader.getCurrentStripeIdx() < endStripeIdx) {
      int stripe = (int)sReader.getCurrentStripeIdx();
      BlockLocation[] bls = sReader.getNextStripeBlockLocations();
      for (BlockLocation bl : bls) {
        if (isBlockCorrupt(bl)) {
          incCorruptBlocksPerStripe(allCorruptBlocksPerStripe,
                                         stripe);
        }
      }
    }
    // figure out which stripes these corrupted blocks belong to
    for (BlockLocation fileBlock: fileBlocks) {
      int blockNo = startBlockIdx + (int) (fileBlock.getOffset() /
          fileStatus.getBlockSize());
      final int stripe = blockNo / stripeSize;
      if (isBlockCorrupt(fileBlock)) {
        corruptBlocksPerStripe.put(stripe, allCorruptBlocksPerStripe.get(stripe));
        if (LOG.isDebugEnabled()) {
          LOG.debug("file " + filePath.toString() + " corrupt in block " + 
                   blockNo + ", stripe " + stripe);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("file " + filePath.toString() + " OK in block " +
                   blockNo + ", stripe " + stripe);
        }
      }
    }
  }

  /**
   * gets the parity blocks corresponding to file
   * returns the parity blocks in case of DFS
   * and the part blocks containing parity blocks
   * in case of HAR FS
   */
  private static BlockLocation[] getParityBlocks(final Path filePath,
                                          final long blockSize,
                                          final long numStripes,
                                          final RaidInfo raidInfo) 
    throws IOException {
    FileSystem parityFS = raidInfo.parityPair.getFileSystem();
    
    // get parity file metadata
    FileStatus parityFileStatus = raidInfo.parityPair.getFileStatus(); 
    long parityFileLength = parityFileStatus.getLen();

    if (parityFileLength != numStripes * raidInfo.parityBlocksPerStripe *
        blockSize) {
      throw new IOException("expected parity file of length" + 
                            (numStripes * raidInfo.parityBlocksPerStripe *
                             blockSize) +
                            " but got parity file of length " + 
                            parityFileLength);
    }

    BlockLocation[] parityBlocks = 
      parityFS.getFileBlockLocations(parityFileStatus, 0L, parityFileLength);
    
    if (parityFS instanceof DistributedFileSystem ||
        parityFS instanceof DistributedRaidFileSystem) {
      long parityBlockSize = parityFileStatus.getBlockSize();
      if (parityBlockSize != blockSize) {
        throw new IOException("file block size is " + blockSize + 
                              " but parity file block size is " + 
                              parityBlockSize);
      }
    } else if (parityFS instanceof HarFileSystem) {
      LOG.debug("HAR FS found");
    } else {
      LOG.warn("parity file system is not of a supported type");
    }
    
    return parityBlocks;
  }

  
  /**
   * checks the parity blocks for a given file and modifies
   * corruptBlocksPerStripe accordingly
   */
  private static void checkParityBlocks(final Path filePath,
                                 final Map<Integer, Integer>
                                 corruptBlocksPerStripe,
                                 final long blockSize,
                                 final long startStripeIdx,
                                 final long endStripeIdx,
                                 final long numStripes,
                                 final RaidInfo raidInfo)
    throws IOException {

    // get the blocks of the parity file
    // because of har, multiple blocks may be returned as one container block
    BlockLocation[] containerBlocks = getParityBlocks(filePath, blockSize,
        numStripes, raidInfo);

    long parityStripeLength = blockSize *
      ((long) raidInfo.parityBlocksPerStripe);

    long parityBlocksFound = 0L;

    for (BlockLocation cb: containerBlocks) {
      if (cb.getLength() % blockSize != 0) {
        throw new IOException("container block size is not " +
                              "multiple of parity block size");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("found container with offset " + cb.getOffset() +
                  ", length " + cb.getLength());
      }

      for (long offset = cb.getOffset();
           offset < cb.getOffset() + cb.getLength();
           offset += blockSize) {
        long block = offset / blockSize;
        
        int stripe = (int) (offset / parityStripeLength);

        if (stripe < 0) {
          // before the beginning of the parity file
          continue;
        }
        if (stripe >= numStripes) {
          // past the end of the parity file
          break;
        }

        parityBlocksFound++;
        
        if (stripe < startStripeIdx || stripe >= endStripeIdx) {
          continue;
        }

        if (isBlockCorrupt(cb)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("parity file for " + filePath.toString() + 
                     " corrupt in block " + block +
                     ", stripe " + stripe + "/" + numStripes);
          }
          incCorruptBlocksPerStripe(corruptBlocksPerStripe, stripe);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("parity file for " + filePath.toString() + 
                     " OK in block " + block +
                     ", stripe " + stripe + "/" + numStripes);
          }
        }
      }
    }

    long parityBlocksExpected = raidInfo.parityBlocksPerStripe * numStripes;
    if (parityBlocksFound != parityBlocksExpected ) {
      throw new IOException("expected " + parityBlocksExpected + 
                            " parity blocks but got " + parityBlocksFound);
    }
  }
  
  private static void incCorruptBlocksPerStripe(Map<Integer, Integer>
  corruptBlocksPerStripe, int stripe) {
    Integer value = corruptBlocksPerStripe.get(stripe);
    if (value == null) {
      value = 0;
    } 
    corruptBlocksPerStripe.put(stripe, value + 1);
  }
  
  private static boolean isBlockCorrupt(BlockLocation fileBlock)
      throws IOException {
    if (fileBlock == null)
      // empty block
      return false;
    return fileBlock.isCorrupt() || 
        (fileBlock.getNames().length == 0 && fileBlock.getLength() > 0);
  }
  
  /**
   * returns the source file corresponding to a parity file
   * @throws IOException 
   */
  public static Path sourcePathFromParityPath(Path parityPath, FileSystem fs) 
        throws IOException {
    String parityPathStr = parityPath.toUri().getPath();
    for (Codec codec : Codec.getCodecs()) {
      String prefix = codec.getParityPrefix();
      if (parityPathStr.startsWith(prefix)) {
        // Remove the prefix to get the source file.
        String src = parityPathStr.replaceFirst(prefix, Path.SEPARATOR);
        Path srcPath = new Path(src);
        if (fs.exists(srcPath)) {
          return srcPath;
        }
      }
    }
    return null;
  }

  public static int readTillEnd(InputStream in, byte[] buf, boolean eofOK, 
      long endOffset, int toRead)
    throws IOException {
    int numRead = 0;
    while (numRead < toRead) {
      int readLen = toRead - numRead;
      if (in instanceof DFSDataInputStream) {
        int available = (int)(endOffset - ((DFSDataInputStream)in).getPos());
        if (available < readLen) {
          readLen = available;
        }
      }
      int nread = readLen > 0? in.read(buf, numRead, readLen): 0;
      if (nread < 0) {
        if (eofOK) {
          // EOF hit, fill with zeros
          Arrays.fill(buf, numRead, toRead, (byte)0);
          break;
        } else {
          // EOF hit, throw.
          throw new IOException("Premature EOF");
        }
      } else if (nread == 0) {
        // reach endOffset, fill with zero;
        Arrays.fill(buf, numRead, toRead, (byte)0);
        break;
      } else {
        numRead += nread;
      }
    }
    
    // return 0 if we read a ZeroInputStream
    if (in instanceof ZeroInputStream) {
      return 0;
    }
    return numRead;
  }

  public static void copyBytes(
    InputStream in, OutputStream out, byte[] buf, long count)
    throws IOException {
    for (long bytesRead = 0; bytesRead < count; ) {
      int toRead = Math.min(buf.length, (int)(count - bytesRead));
      IOUtils.readFully(in, buf, 0, toRead);
      bytesRead += toRead;
      out.write(buf, 0, toRead);
    }
  }

  /**
   * Parse a condensed configuration option and set key:value pairs.
   * @param conf the configuration object.
   * @param optionKey the name of condensed option. The value corresponding
   *        to this should be formatted as key:value,key:value...
   */
  public static void parseAndSetOptions(
      Configuration conf, String optionKey) {
    String optionValue = conf.get(optionKey);
    if (optionValue != null) {
      RaidNode.LOG.info("Parsing option " + optionKey);
      // Parse the option value to get key:value pairs.
      String[] keyValues = optionValue.trim().split(",");
      for (String keyValue: keyValues) {
        String[] fields = keyValue.trim().split(":");
        String key = fields[0].trim();
        String value = fields[1].trim();
        conf.set(key, value);
      }
    } else {
      RaidNode.LOG.error("Option " + optionKey + " not found");
    }
  }

  public static void closeStreams(InputStream[] streams) throws IOException {
    for (InputStream stm: streams) {
      if (stm != null) {
        stm.close();
      }
    }
  }

  public static class ZeroInputStream extends InputStream
	    implements Seekable, PositionedReadable {
    private long endOffset;
    private long pos;

    public ZeroInputStream(long endOffset) {
      this.endOffset = endOffset;
      this.pos = 0;
    }

    @Override
    public int read() throws IOException {
      if (pos < endOffset) {
        pos++;
        return 0;
      }
      return -1;
    }

    @Override
    public int available() throws IOException {
      return (int)(endOffset - pos);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public void seek(long seekOffset) throws IOException {
      if (seekOffset < endOffset) {
        pos = seekOffset;
      } else {
        throw new IOException("Illegal Offset" + pos);
      }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int count = 0;
      for (; position < endOffset && count < length; position++) {
        buffer[offset + count] = 0;
        count++;
      }
      return count;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int count = 0;
      for (; position < endOffset && count < length; position++) {
        buffer[offset + count] = 0;
        count++;
      }
      if (count < length) {
        throw new IOException("Premature EOF");
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    public List<ByteBuffer> readFullyScatterGather(long position, int length)
      throws IOException {
      throw new IOException("ScatterGather not implemeted for Raid.");
    }
  }

  private static int computeMaxMissingBlocks() {
    int max = 0;
    for (Codec codec : Codec.getCodecs()) {
      if (max < codec.stripeLength + codec.parityLength) {
        max = codec.stripeLength + codec.parityLength;
      }
    }
    return max;
  }
  
  public static String getMissingBlksHtmlTable(long numNonRaidedMissingBlocks,
      Map<String, long[]> numStrpWithMissingBlksMap) {
    int max = computeMaxMissingBlocks();
    String head = "";
    for (int i = 1; i <= max; ++i) {
      head += JspUtils.td(i + "");
    }
    head = JspUtils.tr(JspUtils.td("CODE") + head);
    String result = head;
    String row = JspUtils.td("Not Raided");
    row += JspUtils.td(StringUtils.humanReadableInt(
        numNonRaidedMissingBlocks));
    row = JspUtils.tr(row);
    result += row;
    
    for (Codec codec : Codec.getCodecs()) {
      String code = codec.id;
      row = JspUtils.td(code);
      long[] numStrps = numStrpWithMissingBlksMap.get(code);
      if (null == numStrps) {
        continue;
      }
      for (int i = 0; i < numStrps.length; ++i) {
        row += JspUtils.td(StringUtils.humanReadableInt(
            numStrps[i] * (i + 1)));
      }
      row = JspUtils.tr(row);
      result += row;
    }
    return JspUtils.table(result);
  }

}
