
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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.hdfs.util.InjectionEvent;

/**
 * The class to maintain all data I/O activity to a block file.
 */
public class BlockDataFile {
  File file;         // block file
  final FSVolume volume;       // volume where the block belongs
  protected Reader readerForCurrentChannel = null; 

  public BlockDataFile (File file, FSVolume volume) {
    this.file = file;
    this.volume = volume;
  }

  public FSVolume getVolume() {
    return volume;
  }

  synchronized File getFile() {
    return file;
  }

  public synchronized void setFile(File file) {
    this.file = file;
    closeFileChannel();
  }
  
  public synchronized void closeFileChannel() {
    if (readerForCurrentChannel != null) {
      try {
        readerForCurrentChannel.close();
      } catch (IOException ie) {
        // Ignore.
      }
      readerForCurrentChannel = null;
    }
  }

  
  public File getTmpFile(int namespaceId, Block b) throws IOException {
    return getVolume().getTmpFile(namespaceId, b);
  }
  
  public synchronized Reader getReader(DataNode datanode) throws IOException {
    File f = getFile();
    if (f == null) {
      throw new IOException("Can not open file, null object.");
    }
    if (!f.exists()) {
      // Sometimes the file is deleted or removed but the file channel is still open.
      // In this case, we would fail the request. Ideally, it can be done smarter.
      // For example, we can just go ahead and read the data and reply on full
      // block report to finally clean the block. But to keep the behavior consistent
      // with previous behavior, we leave this check here.
      closeFileChannel();
      // if file is not null, but doesn't exist - possibly disk failed
      if (datanode != null) {
        datanode.checkDiskError();
      }
      throw new IOException("File is already deleted or renamed");
    }

    if (readerForCurrentChannel == null) {
      RandomAccessFile blockInFile = new RandomAccessFile(f, "r");
      FileChannel fileChannel = blockInFile.getChannel();
      FileDescriptor fileDescriptor = blockInFile.getFD();
      datanode.myMetrics.cachedFileHandlerCount.inc();
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("Create file channel of " + f);
      }
      readerForCurrentChannel = getReader(fileChannel, fileDescriptor);
    }
    return readerForCurrentChannel;
  } 
  
  protected Reader getReader(FileChannel fileChannel,
      FileDescriptor fileDescriptor) {
    return new Reader(fileChannel, fileDescriptor);
  }
  
  static public BlockDataFile getDummyDataFileFromFileChannel(
      final FileChannel myFileChannel) {
    return new BlockDataFile(null, null) {
      @Override
      public synchronized Reader getReader(DataNode datanode) throws IOException {
        return getReader(myFileChannel, null);
      }
    };
  }
  
  public synchronized Writer getWriter(int bufferSize)
      throws FileNotFoundException, IOException {
    if (file == null) {
      throw new IOException("file is NULL");
    }
    FileOutputStream fout = new FileOutputStream(
        new RandomAccessFile(file, "rw").getFD());
    FileChannel channel = fout.getChannel();
    FileDescriptor fd = fout.getFD();
    OutputStream dataOut;
    if (bufferSize > 0) {
      dataOut = new BufferedOutputStream(fout, bufferSize);
    } else {
      dataOut = fout;
    }
    return new Writer(dataOut, channel, fd);
  }
  
  public synchronized RandomAccessor getRandomAccessor()
      throws FileNotFoundException, IOException {
    if (file == null) {
      throw new IOException("file is NULL");
    }
    return new RandomAccessor(new RandomAccessFile(file, "rw"));
  }

  public static abstract class NativeOperation implements Runnable {
    protected final FileDescriptor fd;
    protected final long offset;
    protected final long len;
    protected final int flags;
    protected final IOStream stream;

    public NativeOperation(FileDescriptor fd, long offset, long len, int flags,
        IOStream stream) {
      this.fd = fd;
      this.offset = offset;
      this.len = len;
      this.flags = flags;
      this.stream = stream;
    }

    protected abstract void invoke() throws IOException;

    @Override
    public void run() {
      stream.lock();
      try {
        if (!stream.isOpen()) {
          DataNode.LOG.warn("Stream for " + stream.getFile()
              + " has been closed, skipping operation");
          InjectionHandler.processEvent(
              InjectionEvent.DATANODE_SKIP_NATIVE_OPERATION, flags, this);
          return;
        }
        invoke();
      } catch (IOException ie) {
        DataNode.LOG.warn(this.getClass() + " failed", ie);
      } finally {
        stream.unlock();
      }
    }
  }

  public static class SyncFileRangeRunnable extends NativeOperation {

    public SyncFileRangeRunnable(FileDescriptor fd, long offset, long len,
        int flags, IOStream stream) {
      super(fd, offset, len, flags, stream);
    }

    @Override
    protected void invoke() throws IOException {
      NativeIO.syncFileRangeIfPossible(fd, offset, len, flags);
    }
  }

  public static class PosixFadviseRunnable extends NativeOperation {

    public PosixFadviseRunnable(FileDescriptor fd, long offset, long len,
        int flags, IOStream stream) {
      super(fd, offset, len, flags, stream);
    }

    @Override
    protected void invoke() throws IOException {
      NativeIO.posixFadviseIfPossible(fd, offset, len, flags);
    }
  }

  /**
   * The object for reading data of a block.
   * For inline checksum, this stream is the only stream from disk.
   * Fro non-inline checksum, this stream is the data stream.
   */
  public class Reader extends IOStream {
    private FileChannel fileChannel;
    
    private Reader (FileChannel fileChannel, FileDescriptor fileDescriptor) {
      super(fileDescriptor);
      this.fileChannel = fileChannel;
    }
    
    protected void closeInternal() throws IOException {
      if (fileChannel != null) {
        fileChannel.close();
        fileChannel = null;
      }
    }

    /**
     * Read fully from the block data stream
     * @param buf
     * @param off
     * @param len
     * @param offset
     * @param throwOnEof
     * @return
     * @throws IOException
     */
    public int readFully(byte buf[], int off, int len, long offset, boolean throwOnEof)
        throws IOException {
      return IOUtils.readFileChannelFully(fileChannel, buf, off, len, offset,
          throwOnEof);
    }
    
    /**
     * @return size of the data stream of the block
     * @throws IOException
     */
    public long size() throws IOException {
      return fileChannel.size();
    }
    
     /**
      * Transfer data of the block stream fully to a socket.
      * @param sockOut
      * @param offset
      * @param len
      * @throws IOException
      */
    public void transferToSocketFully(SocketOutputStream sockOut, long offset,
        int len) throws IOException {
      sockOut.transferToFully(fileChannel, offset, len);
    }
  }

  public abstract class IOStream implements java.io.Closeable {
    protected Lock lock = new ReentrantLock();
    private volatile boolean isOpen = true;
    private final FileDescriptor fd;

    public IOStream(FileDescriptor fd) {
      this.fd = fd;
    }
    
    public File getFile() {
      return file;
    }

    protected abstract void closeInternal() throws IOException;

    @Override
    public void close() throws IOException {
      lock();
      try {
        markClosed();
        closeInternal();
      } finally {
        unlock();
      }
    }

    void lock() {
      lock.lock();
    }

    void unlock() {
      lock.unlock();
    }

    private void markClosed() {
      this.isOpen = false;
    }

    public boolean isOpen() {
      return this.isOpen;
    }

    public void posixFadviseIfPossible(long offset, long len, int flags)
        throws NativeIOException {
        posixFadviseIfPossible(offset, len, flags, false);
    }

    public void posixFadviseIfPossible(long offset, long len, int flags,
        boolean sync) throws NativeIOException {
      if (fd != null) {
        if (sync) {
          NativeIO.posixFadviseIfPossible(fd, offset, len, flags);
        } else {
          volume.submitNativeIOTask(new PosixFadviseRunnable(fd, offset, len,
                flags, this));
        }
      } else if (file != null) {
        DataNode.LOG.warn("Failed to fadvise as file " + file
            + " is missing file descriptor");
      }
    }

    public void syncFileRangeIfPossible(long offset,
        long len, int flags) throws IOException {

      Future<?> f = volume.submitNativeIOTask(new SyncFileRangeRunnable(fd,
          offset, len, flags, this));
      if (((flags & NativeIO.SYNC_FILE_RANGE_WAIT_AFTER) != 0)
          || ((flags & NativeIO.SYNC_FILE_RANGE_WAIT_BEFORE) != 0)) {
        // These are synchronous requests and we need to block until they
        // complete.
        try {
          f.get();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
  }

  /**
   * The object for writing to a block on disk. For inline checksum this writer
   * writes the only data stream. For non-inline checksum, the stream is only
   * for the data.
   */
  public class Writer extends IOStream {
    final protected OutputStream dataOut; // to block file at local disk
    final private FileChannel channel;
    final private FileDescriptor fd;

    public Writer(OutputStream dataOut, FileChannel channel, FileDescriptor fd) {
      super(fd);
      this.dataOut = dataOut;
      this.channel = channel;
      this.fd = fd;
    }

    public void write(byte b[]) throws IOException {
      dataOut.write(b);
    }
    
    public void write(byte b[], int off, int len) throws IOException {
      dataOut.write(b, off, len);
    }
    
    public void flush() throws IOException {
      dataOut.flush();
    }
    
    public long getChannelPosition() throws IOException {
      return channel.position();
    }
    
    public long getChannelSize() throws IOException {
      return channel.size();
    }
    
    public void position(long offset) throws IOException {
      channel.position(offset);
    }
    
    public void force(boolean metaData) throws IOException {
      if (channel != null) {
        channel.force(metaData);
      }
    }
    
    public boolean hasChannel() {
      return (channel != null) && (fd != null);
    }

    protected void closeInternal() throws IOException {
      dataOut.close();
    }

  }
  
  /**
   * The wrapper to wrap a RandomAccessFile to local block file.
   */
  public class RandomAccessor implements Closeable {
    final private RandomAccessFile randomAcessFile;
    
    private RandomAccessor(RandomAccessFile randomAcessFile) {
      this.randomAcessFile = randomAcessFile;
    }
    
    public void seek(long pos) throws IOException {
      randomAcessFile.seek(pos);
    }
    
    public void readFully(byte b[], int off, int len) throws IOException {
      randomAcessFile.readFully(b, off, len);
    }
    
    public void write(byte b[], int off, int len) throws IOException {
      randomAcessFile.write(b, off, len);
    }
    
    public void setLength(long newLength) throws IOException {
      randomAcessFile.setLength(newLength);
    }

    @Override
    public void close() throws IOException {
      randomAcessFile.close();
    }
  }  
}
