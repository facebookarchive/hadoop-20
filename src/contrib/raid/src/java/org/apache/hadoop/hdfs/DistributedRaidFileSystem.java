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
package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.raid.Decoder.DecoderInputStream;
import org.apache.hadoop.raid.LogUtils;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.Decoder;
import org.apache.hadoop.raid.ParityFilePair;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * This is an implementation of the Hadoop  RAID Filesystem. This FileSystem 
 * wraps an instance of the DistributedFileSystem.
 * If a file is corrupted, this FileSystem uses the parity blocks to 
 * regenerate the bad block.
 */

public class DistributedRaidFileSystem extends FilterFileSystem {
  public static final int SKIP_BUF_SIZE = 2048;
  public static final short NON_RAIDED_FILE_REPLICATION = 3;
  public static final String DIRECTORIES_IGNORE_PARITY_CHECKING_KEY = 
      "fs.raid.directories.ignore.parity.checking";
  public static final List<String> DIRECTORIES_IGNORE_PARITY_CHECKING_DEFAULT = 
        Arrays.asList("/tmp/");
  private List<String> directoriesIgnoreCheckParity = 
      DIRECTORIES_IGNORE_PARITY_CHECKING_DEFAULT;

  Configuration conf;

  DistributedRaidFileSystem() throws IOException {
  }

  DistributedRaidFileSystem(FileSystem fs) throws IOException {
    super(fs);
  }

  /* Initialize a Raid FileSystem
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    this.conf = conf;
    
    // init the codec from conf.
    Codec.initializeCodecs(conf);

    Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl",
        DistributedFileSystem.class);
    if (clazz == null) {
      throw new IOException("No FileSystem for fs.raid.underlyingfs.impl.");
    }
    
    String ignoredDirectories = conf.get(DIRECTORIES_IGNORE_PARITY_CHECKING_KEY);
    if (ignoredDirectories != null && ignoredDirectories.length() > 0) {
      String[] directories = ignoredDirectories.split(",");
      directoriesIgnoreCheckParity = new ArrayList<String>();
      for (String dir : directories) {
        if (dir.length() > 0 && dir.startsWith("/")) {
          if (!dir.endsWith("/")) {
            dir = dir + "/";
          }
          directoriesIgnoreCheckParity.add(dir);
        }
      }
    }

    this.fs = (FileSystem)ReflectionUtils.newInstance(clazz, null); 
    super.initialize(name, conf);
  }

  /*
   * Returns the underlying filesystem
   */
  public FileSystem getFileSystem() throws IOException {
    return fs;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // We want to use RAID logic only on instance of DFS.
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem underlyingDfs = (DistributedFileSystem) fs;
      LocatedBlocks lbs =
          underlyingDfs.getLocatedBlocks(f, 0L, Long.MAX_VALUE);
      if (lbs != null) {
        // Use underlying filesystem if the file is under construction.
        if (!lbs.isUnderConstruction()) {
          // Use underlying filesystem if file length is 0.
          final long fileSize = getFileSize(lbs);
          if (fileSize > 0) {
            return new ExtFSDataInputStream(conf, this, f,
              fileSize, getBlockSize(lbs), bufferSize);
          }
        }
      }
    }
    return fs.open(f, bufferSize);
  }

  // Obtain block size given 3 or more blocks
  private static long getBlockSize(LocatedBlocks lbs) throws IOException {
    List<LocatedBlock> locatedBlocks = lbs.getLocatedBlocks();
    long bs = -1;
    for (LocatedBlock lb: locatedBlocks) {
      if (lb.getBlockSize() > bs) {
        bs = lb.getBlockSize();
      }
    }
    return bs;
  }

  private static long getFileSize(LocatedBlocks lbs) throws IOException {
    List<LocatedBlock> locatedBlocks = lbs.getLocatedBlocks();
    long fileSize = 0;
    for (LocatedBlock lb: locatedBlocks) {
      fileSize += lb.getBlockSize();
    }
    if (fileSize != lbs.getFileLength()) {
      throw new IOException("lbs.getFileLength() " + lbs.getFileLength() +
          " does not match sum of block sizes " + fileSize);
    }
    return fileSize;
  }

  /**                       
   * Make an absolute path relative by stripping the leading /
   */   
  static Path makeRelative(Path path) {
    if (!path.isAbsolute()) {
      return path;
    }          
    String p = path.toUri().getPath();
    String relative = p.substring(1, p.length());
    return new Path(relative);
  }
  
  /**
   * check if the trash is enabled here.
   * @return
   * @throws IOException
   */
  private boolean isSkipTrash() throws IOException {
    Trash trashTmp = new Trash(this, getConf());
    return !trashTmp.isEnabled();
  }
  
  private boolean isIgnoreParityChecking(Path src) {
    String uriPath = src.toUri().getPath();
    for (String ignoreRoot : directoriesIgnoreCheckParity) {
      if (uriPath.startsWith(ignoreRoot)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean delete(Path f) throws IOException {
    return delete(f, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    if (isIgnoreParityChecking(f)) {
      // just delete the source file
      return fs.delete(f, recursive);
    }
    
    // not allow to delete a parity file
    Path sourcePath = RaidUtils.sourcePathFromParityPath(f, fs);
    if (sourcePath != null) {
      throw new IOException("You can not delete a parity file, srcPath: "
          + sourcePath + ", parityPath: " + f);
    }
    
    List<Path> parityPathList = new ArrayList<Path>();
    List<Codec> usedCodec = new ArrayList<Codec>();
    FileStatus stat = null;
    try {
      stat = fs.getFileStatus(f);
    } catch (FileNotFoundException ex){
      return false;
    }
    if (!stat.isDir() && stat.getReplication() >= NON_RAIDED_FILE_REPLICATION) {
      return fs.delete(f, recursive);
    }
    
    for (Codec codec : Codec.getCodecs()) {
      Path srcPath;
      if (codec.isDirRaid && !stat.isDir()) {
        srcPath = f.getParent();
      } else {
        srcPath = f;
      }
      
      Path parityPath = new Path(codec.parityDirectory, makeRelative(srcPath));
      if (fs.exists(parityPath)) {
        parityPathList.add(parityPath);
        usedCodec.add(codec);
      }
    }
    
    // check the trash
    if (!stat.isDir()) {
      for (Codec codec : usedCodec) {
        if (codec.isDirRaid && isSkipTrash()) {
          throw new IOException("File " + f + " is directory-raided using " + 
              codec + ", you can not delete it while skipping the trash.");
        }
      }
    }

    // delete the src file
    if (!fs.delete(f, recursive)) {
      return false;
    }

    // delete the parity file
    for (int i = 0; i < parityPathList.size(); i++) {
      Codec codec = usedCodec.get(i);
      if (codec.isDirRaid && !stat.isDir()) {
        // will not delete the parity for the whole directory if we 
        // only want to delete one file in the directory.
        continue;
      }
      
      Path parityPath = parityPathList.get(i);
      fs.delete(parityPath, recursive);
    }

    return true;
  }

  /**
   * undelete the parity file together with the src file.
   */
  @Override
  public boolean undelete(Path f, String userName) throws IOException {
    List<Codec> codecList = Codec.getCodecs();
    Path[] parityPathList = new Path[codecList.size()];

    for (int i=0; i<parityPathList.length; i++) {
      parityPathList[i] = new Path(codecList.get(i).parityDirectory, 
                                   makeRelative(f));
    }

    // undelete the src file.
    if (!fs.undelete(f, userName)) {
      return false;
    }

    // try to undelete the parity file
    for (Path parityPath : parityPathList) {
      fs.undelete(parityPath, userName);
    }
    return true;
  }

  /**
   * search the Har-ed parity files
   */
  private boolean searchHarDir(FileStatus stat) 
      throws IOException {
    if (!stat.isDir()) {
      return false;
    }
    String pattern = stat.getPath().toString() + "/*" + RaidNode.HAR_SUFFIX 
        + "*";
    FileStatus[] stats = globStatus(new Path(pattern));
    if (stats != null && stats.length > 0) {
      return true;
    }
      
    stats = fs.listStatus(stat.getPath());
   
    // search deeper.
    for (FileStatus status : stats) {
      if (searchHarDir(status)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    if (isIgnoreParityChecking(src)) {
      // just rename the source file
      return fs.rename(src, dst);
    }
    
    // not allow to rename a parity
    Path sourcePath = RaidUtils.sourcePathFromParityPath(src, fs);
    if (sourcePath != null) {
      throw new IOException("You can not rename a parity file, srcPath: "
          + sourcePath + ", parityPath: " + src);
    }
    
    List<Path> srcParityList = new ArrayList<Path>();
    List<Path> destParityList = new ArrayList<Path>();
    List<Codec> usedCodec = new ArrayList<Codec>();
    FileStatus srcStat = null;
    try {
      srcStat = fs.getFileStatus(src);
    } catch (FileNotFoundException e) {
      return false;
    }
    if (!srcStat.isDir() &&
        srcStat.getReplication() >= NON_RAIDED_FILE_REPLICATION) {
      return fs.rename(src, dst);
    }
    
    for (Codec codec : Codec.getCodecs()) {
      Path srcPath;
      if (codec.isDirRaid && !srcStat.isDir()) {
        srcPath = src.getParent();
      } else {
        srcPath = src;
      }
      
      Path parityPath = new Path(codec.parityDirectory, makeRelative(srcPath));
      try {
        // check the HAR for directory
        FileStatus stat = fs.getFileStatus(parityPath);
        if (stat.isDir()) {
          
          // search for the har directory.
          if (searchHarDir(stat)) {  
            // HAR Path exists
            throw new IOException("We can not rename the directory because " +
                " there exists a HAR dir in the parity path. src = " + src);
          }
        }
        
        // we will rename the parity paths as well.
        usedCodec.add(codec);
        srcParityList.add(parityPath);
        destParityList.add(new Path(codec.parityDirectory, makeRelative(dst)));
      } catch (FileNotFoundException ex) {
        // parity file does not exist
        // check the HAR for file
        ParityFilePair parityInHar = 
            ParityFilePair.getParityFile(codec, srcStat, conf);
        if (null != parityInHar) {
          // this means we have the parity file in HAR
          // will throw an exception.
          throw new IOException("We can not rename the file whose parity file" +
              " is in HAR. src = " + src);
        }
      }
    }
  
    // rename the file
    if (!fs.rename(src, dst)) {
      return false;
    }
    
    // rename the parity file
    for (int i=0; i<srcParityList.size(); i++) {
      Codec codec = usedCodec.get(i);
      if (codec.isDirRaid && !srcStat.isDir()) {
        // if we just rename one file in a dir-raided directory, 
        // and we also have other files in the same directory,
        // we will keep the parity file.
        Path parent = src.getParent();
        if (fs.listStatus(parent).length > 1) {
          continue;
        }
      }
      
      Path srcParityPath = srcParityList.get(i);
      Path destParityPath = destParityList.get(i);
      fs.mkdirs(destParityPath.getParent());
      fs.rename(srcParityPath, destParityPath);
    }
    return true;
  }

  public void close() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch(IOException ie) {
        //this might already be closed, ignore
      }
    }
    super.close();
  }

  /**
   * Layered filesystem input stream. This input stream tries reading
   * from alternate locations if it encoumters read errors in the primary location.
   */
  private static class ExtFSDataInputStream extends FSDataInputStream {

    private static class UnderlyingBlock {
      // File that holds this block. Need not be the same as outer file.
      public Path path;
      // Offset within path where this block starts.
      public long actualFileOffset;
      // Length of the block (length <= blk sz of outer file).
      public long length;
      public UnderlyingBlock(Path path, long actualFileOffset,
          long length) {
        this.path = path;
        this.actualFileOffset = actualFileOffset;
        this.length = length;
      }
    }

    /**
     * Create an input stream that wraps all the reads/positions/seeking.
     */
    private static class ExtFsInputStream extends FSInputStream {

      // Extents of "good" underlying data that can be read.
      private UnderlyingBlock[] underlyingBlocks;
      private long currentOffset;
      private FSDataInputStream currentStream;
      private DecoderInputStream recoveryStream;
      private boolean useRecoveryStream;
      private boolean ifLastReadUseRecovery;
      private UnderlyingBlock currentBlock;
      private byte[] oneBytebuff = new byte[1];
      private byte[] skipbuf = new byte[SKIP_BUF_SIZE];
      private int nextLocation;
      private DistributedRaidFileSystem lfs;
      private Path path;
      private final long fileSize;
      private final long blockSize;
      private final int buffersize;
      private final Configuration conf;
      private Configuration innerConf;
      private Map<String, ParityFilePair> parityFilePairs;

      ExtFsInputStream(Configuration conf, DistributedRaidFileSystem lfs,
          Path path, long fileSize, long blockSize, int buffersize)
        throws IOException {
        this.path = path;
        this.nextLocation = 0;
        // Construct array of blocks in file.
        this.blockSize = blockSize;
        this.fileSize = fileSize;
        long numBlocks = (this.fileSize % this.blockSize == 0) ?
            this.fileSize / this.blockSize :
              1 + this.fileSize / this.blockSize;
        this.underlyingBlocks = new UnderlyingBlock[(int)numBlocks];
        for (int i = 0; i < numBlocks; i++) {
          long actualFileOffset = i * blockSize;
          long length = Math.min(
              blockSize, fileSize - actualFileOffset);
          this.underlyingBlocks[i] = new UnderlyingBlock(
              path, actualFileOffset, length);
        }
        this.currentOffset = 0;
        this.currentBlock = null;
        this.buffersize = buffersize;
        this.conf = conf;
        this.lfs = lfs;
        this.ifLastReadUseRecovery = false;
        
        // Initialize the "inner" conf, and cache this for all future uses.
        //Make sure we use DFS and not DistributedRaidFileSystem for unRaid.
        this.innerConf = new Configuration(conf);
        Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl",
            DistributedFileSystem.class);
        this.innerConf.set("fs.hdfs.impl", clazz.getName());
        // Disable caching so that a previously cached RaidDfs is not used.
        this.innerConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        
        this.parityFilePairs = new HashMap<String, ParityFilePair>();
        
        // Open a stream to the first block.
        openCurrentStream();
      }
      
      private void closeCurrentStream() throws IOException {
        if (currentStream != null) {
          currentStream.close();
          currentStream = null;
        }
      }
      
      private void closeRecoveryStream() throws IOException {
        if (null != recoveryStream) {
          recoveryStream.close();
          recoveryStream = null;
        }
      }

      /**
       * Open a stream to the file containing the current block
       * and seek to the appropriate offset
       */
      private void openCurrentStream() throws IOException {
        openCurrentStream(true);
      }
      
      private void openCurrentStream(boolean seek) throws IOException {  
        if (recoveryStream != null && 
            recoveryStream.getCurrentOffset() == currentOffset &&
            recoveryStream.getAvailable() > 0) {
          useRecoveryStream = true;
          closeCurrentStream();
          return;
        } else {
          useRecoveryStream = false;
          closeRecoveryStream();
        }

        //if seek to the filelen + 1, block should be the last block
        int blockIdx = (currentOffset < fileSize)?
            (int)(currentOffset/blockSize):
              underlyingBlocks.length - 1;
        UnderlyingBlock block = underlyingBlocks[blockIdx];
        
        // If the current path is the same as we want.
        if (currentBlock == block ||
            currentBlock != null && currentBlock.path == block.path) {
          // If we have a valid stream, nothing to do.
          if (currentStream != null) {
            currentBlock = block;
            if (seek) {
              currentStream.seek(currentOffset);
            }
            return;
          }
        } else {
          closeCurrentStream();
        }

        currentBlock = block;
        currentStream = lfs.fs.open(currentBlock.path, buffersize);
        
        // we will not seek in the pread
        if (seek) {
          currentStream.seek(currentOffset);
        }
      }

      /**
       * Returns the number of bytes available in the current block.
       */
      private int blockAvailable() {
        return (int) (currentBlock.length -
            (currentOffset - currentBlock.actualFileOffset));
      }

      @Override
      public synchronized int available() throws IOException {
        // Application should not assume that any bytes are buffered here.
        nextLocation = 0;
        return Math.min(blockAvailable(), currentStream.available());
      }

      @Override
      public synchronized  void close() throws IOException {
        closeCurrentStream();
        closeRecoveryStream();
        super.close();
      }

      @Override
      public boolean markSupported() { return false; }

      @Override
      public void mark(int readLimit) {
        // Mark and reset are not supported.
        nextLocation = 0;
      }

      @Override
      public void reset() throws IOException {
        // Mark and reset are not supported.
        nextLocation = 0;
      }

      @Override
      public synchronized int read() throws IOException {
        int value = read(oneBytebuff, 0, 1);
        if (value < 0) {
          return value;
        } else {
          return 0xFF & oneBytebuff[0];
        }
      }

      @Override
      public synchronized int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
      }

      @Override
      public synchronized int read(byte[] b, int offset, int len) 
          throws IOException {
        ifLastReadUseRecovery = false;
        if (currentOffset >= fileSize) {
          return -1;
        }
        openCurrentStream();
        int limit = Math.min(blockAvailable(), len);
        int value;
        if (useRecoveryStream) {
          ifLastReadUseRecovery = true;
          value = recoveryStream.read(b, offset, limit);
        } else {
          try{
            value = currentStream.read(b, offset, limit);
          } catch (BlockMissingException e) {
            value = readViaCodec(b, offset, limit, blockAvailable(), e);
          } catch (ChecksumException e) {
            value = readViaCodec(b, offset, limit, blockAvailable(), e);
          }
        }

        currentOffset += value;
        nextLocation = 0;
        return value;
      }
      
      @Override
      public synchronized int read(long position, byte[] b, int offset, int len) 
          throws IOException {
        ifLastReadUseRecovery = false;
        long oldPos = currentOffset;
        currentOffset = position;
        nextLocation = 0;
        try {
          if (currentOffset >= fileSize) {
            return -1;
          }
          
          openCurrentStream(false);
          int limit = Math.min(blockAvailable(), len);
          int value;
          if (useRecoveryStream) {
            ifLastReadUseRecovery = true;
            value = recoveryStream.read(b, offset, limit);
          } else {
            try{
              value = currentStream.read(position, b, offset, limit);
            } catch (BlockMissingException e) {
              value = readViaCodec(b, offset, limit, limit, e);
            } catch (ChecksumException e) {
              value = readViaCodec(b, offset, limit, limit, e);
            }            
          }
          currentOffset += value;
          nextLocation = 0;
          return value;
        } finally {
          seek(oldPos);
        }
      }
      
      private int readViaCodec(byte[] b, int offset, int len,
          int streamLimit, IOException e) 
          throws IOException{
        
        // generate the DecoderInputStream
        try {
          if (null == recoveryStream || 
              recoveryStream.getCurrentOffset() != currentOffset) {
            closeRecoveryStream();
            recoveryStream = getAlternateInputStream(e, currentOffset, 
                streamLimit);
          }
        } catch (IOException ex) {
          LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, len, null,
              path, currentOffset, LOGTYPES.READ, lfs, ex, null);
          throw e;
        }
        if (null == recoveryStream) {
          LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, len, null, 
              path, currentOffset, LOGTYPES.READ, lfs, e, null);
          throw e;
        }
        
        try {
          int value = recoveryStream.read(b, offset, len);
          closeCurrentStream();
          return value;
        } catch (Throwable ex) {
          LogUtils.logRaidReconstructionMetrics(LOGRESULTS.FAILURE, len, 
              recoveryStream.getCodec(), path, currentOffset, LOGTYPES.READ, lfs, ex, null);
          throw e;
        }
      }

      @Override
      public synchronized long skip(long n) throws IOException {
        long skipped = 0;
        long startPos = getPos();
        while (skipped < n) {
          int toSkip = (int)Math.min(SKIP_BUF_SIZE, n - skipped);
          int val = read(skipbuf, 0, toSkip);
          if (val < 0) {
            break;
          }
          skipped += val;
        }
        nextLocation = 0;
        long newPos = getPos();
        if (newPos - startPos > n) {
          throw new IOException(
              "skip(" + n + ") went from " + startPos + " to " + newPos);
        }
        if (skipped != newPos - startPos) {
          throw new IOException(
              "skip(" + n + ") went from " + startPos + " to " + newPos +
              " but skipped=" + skipped);
        }
        return skipped;
      }

      @Override
      public synchronized long getPos() throws IOException {
        nextLocation = 0;
        return currentOffset;
      }

      @Override
      public synchronized void seek(long pos) throws IOException {
        if (pos > fileSize) {
          throw new EOFException("Cannot seek to " + pos + ", file length is " + fileSize);
        }
        if (pos != currentOffset) {
          currentOffset = pos;
          openCurrentStream();
        }
        nextLocation = 0;
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        seek(targetPos);
        boolean value = currentStream.seekToNewSource(currentStream.getPos());
        nextLocation = 0;
        return value;
      }

      /**
       * position readable again.
       */
      @Override
      public void readFully(final long pos, byte[] b, int offset, int length) 
          throws IOException {
        long oldPos = currentOffset;
        boolean ifRecovery = false;
        try {
          while (true) {
            // This loop retries reading until successful. Unrecoverable errors
            // cause exceptions.
            // currentOffset is changed by read().
            long curPos = pos;
            while (length > 0) {
              int n = read(curPos, b, offset, length);
              // There is data race here so value ifRecovery is best efforts.
              ifRecovery |= this.ifLastReadUseRecovery;
              if (n < 0) {
                throw new IOException("Premature EOF");
              }
              offset += n;
              length -= n;
              curPos += n;
            }
            nextLocation = 0;

            if (ifRecovery) {
              ifLastReadUseRecovery = true;
            }
            return;
          }
        } finally {
          seek(oldPos);
        }
      }

      @Override
      public void readFully(long pos, byte[] b) throws IOException {
        readFully(pos, b, 0, b.length);
        nextLocation = 0;
      }
      
      public boolean ifLastReadUseRecovery() {
        return ifLastReadUseRecovery;
      }

      /**
       * Extract good data from RAID
       * 
       * @throws IOException if all alternate locations are exhausted
       */
      private DecoderInputStream getAlternateInputStream(IOException curexp, 
            long offset, 
            final long readLimit) 
            throws IOException{
        
        // Start offset of block.
        long corruptOffset = (offset / blockSize) * blockSize;
        
        FileStatus srcStat = this.lfs.getFileStatus(path);
        long fileLen = srcStat.getLen();
        long limit = Math.min(readLimit, 
            blockSize - (offset - corruptOffset));
        limit = Math.min(limit, fileLen);
        long blockIdx = corruptOffset / blockSize;
        
        DistributedFileSystem dfs = (DistributedFileSystem) this.lfs.fs;
        LocatedBlocks locatedBlocks = dfs.getClient().getLocatedBlocks(
            path.toUri().getPath(), blockIdx * blockSize, blockSize);
        LocatedBlock lostBlock = locatedBlocks.get(0);
        
        int oldNextLocation = nextLocation;
        // First search for parity without stripe store
        while (nextLocation < Codec.getCodecs().size()) {
          try {
            int idx = nextLocation++;
            Codec codec = Codec.getCodecs().get(idx);
            if (!parityFilePairs.containsKey(codec.id)) {
              parityFilePairs.put(codec.id, ParityFilePair.getParityFile(codec, 
                  srcStat, this.innerConf));
            }
            
            DecoderInputStream recoveryStream = 
                RaidNode.unRaidCorruptInputStream(innerConf, path, 
                codec, parityFilePairs.get(codec.id), lostBlock.getBlock(),
                blockSize, offset, limit, false);
            
            if (null != recoveryStream) {
              return recoveryStream;
            }
            
          } catch (Exception e) {
            LOG.info("Ignoring error in using alternate path " + path, e);
          }
        }
        // Second look up in the stripe store for dir-raid
        nextLocation = oldNextLocation;
        while (nextLocation < Codec.getCodecs().size()) {
          try {
            int idx = nextLocation++;
            Codec codec = Codec.getCodecs().get(idx);
            if (!codec.isDirRaid) 
              continue;
            DecoderInputStream recoveryStream = 
                RaidNode.unRaidCorruptInputStream(innerConf, path, 
                codec, parityFilePairs.get(idx), lostBlock.getBlock(),
                blockSize, offset, limit, true);
            
            if (null != recoveryStream) {
              return recoveryStream;
            }
            
          } catch (Exception e) {
            LOG.info("Ignoring error in using alternate path " + path, e);
          }
        }
        LOG.warn("Could not reconstruct block " + path + ":" + offset);
        throw curexp;
      }

      /**
       * The name of the file system that is immediately below the
       * DistributedRaidFileSystem. This is specified by the
       * configuration parameter called fs.raid.underlyingfs.impl.
       * If this parameter is not specified in the configuration, then
       * the default class DistributedFileSystem is returned.
       * @param conf the configuration object
       * @return the filesystem object immediately below DistributedRaidFileSystem
       * @throws IOException if all alternate locations are exhausted
       */
      private FileSystem getUnderlyingFileSystem(Configuration conf) {
        Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl", 
                                       DistributedFileSystem.class);
        FileSystem fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
        return fs;
      }
    }

    /**
     * constructor for ext input stream.
     * @param fs the underlying filesystem
     * @param p the path in the underlying file system
     * @param buffersize the size of IO
     * @throws IOException
     */
    public ExtFSDataInputStream(Configuration conf, DistributedRaidFileSystem lfs,
      Path p, long fileSize, long blockSize, int buffersize) throws IOException {
        super(new ExtFsInputStream(conf, lfs, p, fileSize, blockSize,
            buffersize));
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter)
          throws FileNotFoundException, IOException {
    return fs.listLocatedStatus(f, filter);
  }

}
