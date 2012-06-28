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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.raid.Decoder;
import org.apache.hadoop.raid.Decoder.DecoderInputStream;
import org.apache.hadoop.raid.ParityFilePair;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * This is an implementation of the Hadoop  RAID Filesystem. This FileSystem 
 * wraps an instance of the DistributedFileSystem.
 * If a file is corrupted, this FileSystem uses the parity blocks to 
 * regenerate the bad block.
 */

public class DistributedRaidFileSystem extends FilterFileSystem {
  public static final int SKIP_BUF_SIZE = 2048;

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

  @Override
  public boolean delete(Path f) throws IOException {
    return delete(f, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    List<Path> parityPathList = new ArrayList<Path>() ;

    for (Codec codec : Codec.getCodecs()) {
      Path parityPath = new Path(codec.parityDirectory, makeRelative(f));
      if (fs.exists(parityPath)) {
        parityPathList.add(parityPath);
      }
    }

    // delete the src file
    if (!fs.delete(f, recursive)) {
      return false;
    }

    // delete the parity file
    for (Path parityPath : parityPathList) {
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

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    boolean renameParityFile = false;
    Path srcParityPath = null;
    Path destParityPath = null;
    
    for (Codec codec: Codec.getCodecs()) {
      Path parityPath = new Path(codec.parityDirectory, makeRelative(src));
      if (fs.exists(parityPath)) {
        renameParityFile = true;
        srcParityPath = parityPath;
        destParityPath = new Path(codec.parityDirectory, makeRelative(dst));
        break;
      }
    }
  
    // rename the file
    if (!fs.rename(src, dst)) {
      return false;
    }
    
    // rename the parity file
    if (renameParityFile) {
      if (!fs.exists(destParityPath.getParent())) {
        fs.mkdirs(destParityPath.getParent());
      }
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
      // Offset within the outer file where this block starts.
      public long originalFileOffset;
      // Length of the block (length <= blk sz of outer file).
      public long length;
      public UnderlyingBlock(Path path, long actualFileOffset,
          long originalFileOffset, long length) {
        this.path = path;
        this.actualFileOffset = actualFileOffset;
        this.originalFileOffset = originalFileOffset;
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
      private List<ParityFilePair> parityFilePairs;

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
          long originalFileOffset = i * blockSize;
          long length = Math.min(
              blockSize, fileSize - originalFileOffset);
          this.underlyingBlocks[i] = new UnderlyingBlock(
              path, actualFileOffset, originalFileOffset, length);
        }
        this.currentOffset = 0;
        this.currentBlock = null;
        this.buffersize = buffersize;
        this.conf = conf;
        this.lfs = lfs;
        
        // Initialize the "inner" conf, and cache this for all future uses.
        //Make sure we use DFS and not DistributedRaidFileSystem for unRaid.
        this.innerConf = new Configuration(conf);
        Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl",
            DistributedFileSystem.class);
        this.innerConf.set("fs.hdfs.impl", clazz.getName());
        // Disable caching so that a previously cached RaidDfs is not used.
        this.innerConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        
        // load the parity files
        this.parityFilePairs = new ArrayList<ParityFilePair>();
        for (Codec codec : Codec.getCodecs()) {
          ParityFilePair ppair = ParityFilePair.getParityFile(codec, 
              this.path, this.innerConf);
          this.parityFilePairs.add(ppair);
        }
        
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
            return;
          }
        } else {
          closeCurrentStream();
        }

        currentBlock = block;
        currentStream = lfs.fs.open(currentBlock.path, buffersize);
        long offset = block.actualFileOffset +
            (currentOffset - block.originalFileOffset);
        currentStream.seek(offset);
      }

      /**
       * Returns the number of bytes available in the current block.
       */
      private int blockAvailable() {
        return (int) (currentBlock.length -
            (currentOffset - currentBlock.originalFileOffset));
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
        if (currentOffset >= fileSize) {
          return -1;
        }
        openCurrentStream();
        int limit = Math.min(blockAvailable(), len);
        int value;
        if (useRecoveryStream) {
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
        long oldPos = currentOffset;
        seek(position);
        try {
          if (currentOffset >= fileSize) {
            return -1;
          }
          
          openCurrentStream();
          int limit = Math.min(blockAvailable(), len);
          int value;
          if (useRecoveryStream) {
            value = recoveryStream.read(b, offset, limit);
          } else {
            try{
              value = currentStream.read(b, offset, limit);
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
        if (null == recoveryStream || 
            recoveryStream.getCurrentOffset() != currentOffset) {
          recoveryStream = getAlternateInputStream(e, currentOffset, 
              streamLimit);
        }
        if (null == recoveryStream) {
          throw e;
        }
        
        try {
          int value = recoveryStream.read(b, offset, len);
          closeCurrentStream();
          return value;
        } catch (IOException ex) {
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
          closeCurrentStream();
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
        try {
          while (true) {
            // This loop retries reading until successful. Unrecoverable errors
            // cause exceptions.
            // currentOffset is changed by read().
            long curPos = pos;
            while (length > 0) {
              int n = read(curPos, b, offset, length);
              if (n < 0) {
                throw new IOException("Premature EOF");
              }
              offset += n;
              length -= n;
              curPos += n;
            }
            nextLocation = 0;
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
        
       
        long fileLen = this.lfs.getFileStatus(path).getLen();
        long limit = Math.min(readLimit, 
            blockSize - (offset - corruptOffset));
        limit = Math.min(limit, fileLen);
        
        while (nextLocation < Codec.getCodecs().size()) {
          
          try {
            int idx = nextLocation++;
            Codec codec = Codec.getCodecs().get(idx);
            
            DecoderInputStream recoveryStream = 
                RaidNode.unRaidCorruptInputStream(innerConf, path, 
                codec, parityFilePairs.get(idx), blockSize, offset, limit);
            
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
