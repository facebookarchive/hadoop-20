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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.metrics.APITrace;
import org.apache.hadoop.metrics.APITrace.StreamTracer;
import org.apache.hadoop.metrics.APITrace.TraceableStream;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;


/**
 *  Tracing class for recording client-side API calls and RPC calls.
 *
 *  We override the methods in FilterFileSystem which are interesting to trace.  In general, we
 *  make an identical unmodified call to the underlying file system, log the method call and
 *  results, and return the value that the underlying file system returned unmodified.  The
 *  only exceptions are the calls which return streams (e.g., open or create).  For these calls,
 *  we want to record I/O that is performed on streams, so we return special trace wrappers
 #  around the streams returned returned by the underlying file system.
*/
public class APITraceFileSystem extends FilterFileSystem {
  private static final String[] traceConfOpts = {"mapred.job.id",
                                                 "mapred.task.id",
                                                 "fs.default.name"};

  APITraceFileSystem() throws IOException {
    this.fs = null;
  }

  APITraceFileSystem(FileSystem fs) throws IOException {
    super(fs);
  }


  /*
   * Initialize an API-Trace File System
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    // if an underlying fs has not already been specified,
    // create and initialize one
    if (this.fs == null) {
      Class<?> clazz = conf.getClass("fs.apitrace.underlyingfs.impl",
                                     DistributedFileSystem.class);
      if (clazz == null) {
        throw new IOException("No FileSystem for fs.apitrace.underlyingfs.impl.");
      }

      this.fs = (FileSystem)ReflectionUtils.newInstance(clazz, null);
      super.initialize(name, conf);
    }

    // trace conf options
    for (String opt : traceConfOpts) {
      APITrace.CallEvent ce = new APITrace.CallEvent();
      ce.logCall(APITrace.COMMENT_msg, null,
                 new Object[] {opt, conf.get(opt, "")});
    }
  }

  public FileSystem getRawFileSystem() {
    // TODO: I'm not sure if this is the correct behavior.

    if (fs instanceof FilterFileSystem) {
      return ((FilterFileSystem)fs).getRawFileSystem();
    }
    return fs;
  }


  // start auto(wrap:FileSystem)
  public String getName() {
    APITrace.CallEvent ce;
    String rv;
    ce = new APITrace.CallEvent();

    rv = super.getName();

    ce.logCall(APITrace.CALL_getName,
               rv,
               null);
    return rv;
  }


  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, 
                                               long len) throws IOException {
    APITrace.CallEvent ce;
    BlockLocation[] rv;
    ce = new APITrace.CallEvent();

    rv = super.getFileBlockLocations(file, start, len);

    ce.logCall(APITrace.CALL_getFileBlockLocations,
               null,
               new Object[] {
                 start,
                 len});
    return rv;
  }


  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    APITrace.CallEvent ce;
    FSDataInputStream rv;
    ce = new APITrace.CallEvent();

    rv = super.open(f, bufferSize);
    rv = new TraceFSDataInputStream(rv, new StreamTracer());

    ce.logCall(APITrace.CALL_open,
               rv,
               new Object[] {
                 f,
                 bufferSize});
    return rv;
  }


  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
    throws IOException {

    APITrace.CallEvent ce;
    FSDataOutputStream rv;
    ce = new APITrace.CallEvent();

    rv = super.append(f, bufferSize, progress);
    rv = new TraceFSDataOutputStream(rv, new StreamTracer());

    ce.logCall(APITrace.CALL_append,
               rv,
               new Object[] {
                 f,
                 bufferSize});
    return rv;
  }


  public FSDataOutputStream create(Path f, FsPermission permission, 
                                   boolean overwrite, int bufferSize, 
                                   short replication, long blockSize, 
                                   Progressable progress) throws IOException {
    APITrace.CallEvent ce;
    FSDataOutputStream rv;
    ce = new APITrace.CallEvent();

    rv = super.create(f, permission, overwrite, bufferSize, replication, 
                      blockSize, progress);
    rv = new TraceFSDataOutputStream(rv, new StreamTracer());

    ce.logCall(APITrace.CALL_create,
               rv,
               new Object[] {
                 f,
                 overwrite,
                 bufferSize,
                 replication,
                 blockSize});
    return rv;
  }


  public FSDataOutputStream create(Path f, FsPermission permission, 
                                   boolean overwrite, int bufferSize, 
                                   short replication, long blockSize, 
                                   int bytesPerChecksum, Progressable progress, 
                                   boolean forceSync) throws IOException {
    APITrace.CallEvent ce;
    FSDataOutputStream rv;
    ce = new APITrace.CallEvent();

    rv = super.create(f, permission, overwrite, bufferSize, replication, 
                      blockSize, bytesPerChecksum, progress, forceSync);
    rv = new TraceFSDataOutputStream(rv, new StreamTracer());

    ce.logCall(APITrace.CALL_create1,
               rv,
               new Object[] {
                 f,
                 overwrite,
                 bufferSize,
                 replication,
                 blockSize,
                 bytesPerChecksum,
                 forceSync});
    return rv;
  }


  @Deprecated public FSDataOutputStream createNonRecursive(Path f, 
                                                           boolean overwrite, 
                                                           int bufferSize, 
                                                           short replication, 
                                                           long blockSize, 
                                                           Progressable progress, 
                                                           boolean forceSync)
    throws IOException {

    APITrace.CallEvent ce;
    FSDataOutputStream rv;
    ce = new APITrace.CallEvent();

    rv = super.createNonRecursive(f, overwrite, bufferSize, replication, 
                                  blockSize, progress, forceSync);
    rv = new TraceFSDataOutputStream(rv, new StreamTracer());

    ce.logCall(APITrace.CALL_createNonRecursive,
               rv,
               new Object[] {
                 f,
                 overwrite,
                 bufferSize,
                 replication,
                 blockSize,
                 forceSync});
    return rv;
  }


  @Deprecated public FSDataOutputStream createNonRecursive(Path f, 
                                                           FsPermission permission, 
                                                           boolean overwrite, 
                                                           int bufferSize, 
                                                           short replication, 
                                                           long blockSize, 
                                                           Progressable progress, 
                                                           boolean forceSync, 
                                                           boolean doParallelWrite)
    throws IOException {

    APITrace.CallEvent ce;
    FSDataOutputStream rv;
    ce = new APITrace.CallEvent();

    rv = super.createNonRecursive(f, permission, overwrite, bufferSize, 
                                  replication, blockSize, progress, 
                                  forceSync, doParallelWrite);
    rv = new TraceFSDataOutputStream(rv, new StreamTracer());

    ce.logCall(APITrace.CALL_createNonRecursive1,
               rv,
               new Object[] {
                 f,
                 overwrite,
                 bufferSize,
                 replication,
                 blockSize,
                 forceSync,
                 doParallelWrite});
    return rv;
  }


  public boolean setReplication(Path src, short replication) throws IOException {
    APITrace.CallEvent ce;
    boolean rv;
    ce = new APITrace.CallEvent();

    rv = super.setReplication(src, replication);

    ce.logCall(APITrace.CALL_setReplication,
               rv,
               new Object[] {
                 src,
                 replication});
    return rv;
  }


  public boolean hardLink(Path src, Path dst) throws IOException {
    APITrace.CallEvent ce;
    boolean rv;
    ce = new APITrace.CallEvent();

    rv = super.hardLink(src, dst);

    ce.logCall(APITrace.CALL_hardLink,
               rv,
               new Object[] {
                 src,
                 dst});
    return rv;
  }


  public boolean rename(Path src, Path dst) throws IOException {
    APITrace.CallEvent ce;
    boolean rv;
    ce = new APITrace.CallEvent();

    rv = super.rename(src, dst);

    ce.logCall(APITrace.CALL_rename,
               rv,
               new Object[] {
                 src,
                 dst});
    return rv;
  }


  public boolean delete(Path f) throws IOException {
    APITrace.CallEvent ce;
    boolean rv;
    ce = new APITrace.CallEvent();

    rv = super.delete(f);

    ce.logCall(APITrace.CALL_delete,
               rv,
               new Object[] {
                 f});
    return rv;
  }


  public boolean delete(Path f, boolean recursive) throws IOException {
    APITrace.CallEvent ce;
    boolean rv;
    ce = new APITrace.CallEvent();

    rv = super.delete(f, recursive);

    ce.logCall(APITrace.CALL_delete1,
               rv,
               new Object[] {
                 f,
                 recursive});
    return rv;
  }


  public FileStatus[] listStatus(Path f) throws IOException {
    APITrace.CallEvent ce;
    FileStatus[] rv;
    ce = new APITrace.CallEvent();

    rv = super.listStatus(f);

    ce.logCall(APITrace.CALL_listStatus,
               rv,
               new Object[] {
                 f});
    return rv;
  }


  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    APITrace.CallEvent ce;
    boolean rv;
    ce = new APITrace.CallEvent();

    rv = super.mkdirs(f, permission);

    ce.logCall(APITrace.CALL_mkdirs,
               rv,
               new Object[] {
                 f});
    return rv;
  }


  public OpenFileInfo[] iterativeGetOpenFiles(Path prefix, int millis, 
                                              String start) throws IOException {
    APITrace.CallEvent ce;
    OpenFileInfo[] rv;
    ce = new APITrace.CallEvent();

    rv = super.iterativeGetOpenFiles(prefix, millis, start);

    ce.logCall(APITrace.CALL_iterativeGetOpenFiles,
               null,
               new Object[] {
                 prefix,
                 millis,
                 start});
    return rv;
  }


  public long getUsed() throws IOException{
    APITrace.CallEvent ce;
    long rv;
    ce = new APITrace.CallEvent();

    rv = super.getUsed();

    ce.logCall(APITrace.CALL_getUsed,
               rv,
               null);
    return rv;
  }


  public long getDefaultBlockSize() {
    APITrace.CallEvent ce;
    long rv;
    ce = new APITrace.CallEvent();

    rv = super.getDefaultBlockSize();

    ce.logCall(APITrace.CALL_getDefaultBlockSize,
               rv,
               null);
    return rv;
  }


  public short getDefaultReplication() {
    APITrace.CallEvent ce;
    short rv;
    ce = new APITrace.CallEvent();

    rv = super.getDefaultReplication();

    ce.logCall(APITrace.CALL_getDefaultReplication,
               rv,
               null);
    return rv;
  }


  public ContentSummary getContentSummary(Path f) throws IOException {
    APITrace.CallEvent ce;
    ContentSummary rv;
    ce = new APITrace.CallEvent();

    rv = super.getContentSummary(f);

    ce.logCall(APITrace.CALL_getContentSummary,
               null,
               new Object[] {
                 f});
    return rv;
  }


  public FileStatus getFileStatus(Path f) throws IOException {
    APITrace.CallEvent ce;
    FileStatus rv;
    ce = new APITrace.CallEvent();

    rv = super.getFileStatus(f);

    ce.logCall(APITrace.CALL_getFileStatus,
               rv,
               new Object[] {
                 f});
    return rv;
  }


  public FileChecksum getFileChecksum(Path f) throws IOException {
    APITrace.CallEvent ce;
    FileChecksum rv;
    ce = new APITrace.CallEvent();

    rv = super.getFileChecksum(f);

    ce.logCall(APITrace.CALL_getFileChecksum,
               null,
               new Object[] {
                 f});
    return rv;
  }


  public void setVerifyChecksum(boolean verifyChecksum) {
    APITrace.CallEvent ce;
    ce = new APITrace.CallEvent();

    super.setVerifyChecksum(verifyChecksum);

    ce.logCall(APITrace.CALL_setVerifyChecksum,
               null,
               new Object[] {
                 verifyChecksum});
  }


  public void setOwner(Path p, String username, String groupname)
    throws IOException {

    APITrace.CallEvent ce;
    ce = new APITrace.CallEvent();

    super.setOwner(p, username, groupname);

    ce.logCall(APITrace.CALL_setOwner,
               null,
               new Object[] {
                 p,
                 username,
                 groupname});
  }


  public void setTimes(Path p, long mtime, long atime) throws IOException {
    APITrace.CallEvent ce;
    ce = new APITrace.CallEvent();

    super.setTimes(p, mtime, atime);

    ce.logCall(APITrace.CALL_setTimes,
               null,
               new Object[] {
                 p,
                 mtime,
                 atime});
  }


  public void setPermission(Path p, FsPermission permission) throws IOException {
    APITrace.CallEvent ce;
    ce = new APITrace.CallEvent();

    super.setPermission(p, permission);

    ce.logCall(APITrace.CALL_setPermission,
               null,
               new Object[] {
                 p});
  }


  // end auto


  /*
   *  Tracer for output streams returned by FileSystem.create().
   * 
   *  The underlying file system will return an FSDataOutputStream object upon create.
   *  Rather than adding log statements to the logic of every FSDataOutputStream implementation we
   *  might want to trace, we simply return a wrapper around the stream that the underlying FS
   *  returned.  Our design is guided by three desirable properties for the wrapper:
   *
   *  (1) The wrapper must implement the FSDataOutputStream interface (requirement).
   *  (2) The wrapper should have a relatively simple tracing API (desirable).
   *  (3) The wrapper should never silently perform I/O without logging,
   *      even if parent classes support new methods in the future (desirable).
   *
   *  In order to satisfy all three constraints, we need to provide two levels of wrappers
   *  over the object returned by the underlying FS.  In order to satisfy constraint
   *  (1), we return our own implementation of FSDataOutputStream as the higher-level wrapper.
   *
   *  Unfortunately, this wrapper alone is not sufficient as it does not meet requirement (2):
   *  FSDataOutputStream inherits a broad API from java.io.DataOutputStream (e.g., we would need
   *  to trace writeBoolean, writeFloat, writeUTF, etc if we traced at this level).  Also,
   *  simply tracing at the FSDataOutputStream level would violate (3), as DataOutputStreams
   *  automatically wrap an inner stream, so any new methods added to java.io.DataOutputStream
   *  would be inherited by our implementation, meaning that I/O might occur without tracing.
   *  
   *  In order to satisfy (2) and (3), we use an implementation of java.io.OutputStream as our
   *  lower-level wrapper.  This level performs all the necessary logging calls.
   */
  private static class TraceFSDataOutputStream extends FSDataOutputStream
    implements TraceableStream {

    private StreamTracer streamTracer;

    private static class TraceFSOutputStream extends OutputStream
      implements TraceableStream, Syncable {

      private FSDataOutputStream out;
      private StreamTracer streamTracer;

      TraceFSOutputStream(FSDataOutputStream out, StreamTracer streamTracer) {
        this.out = out;
        this.streamTracer = streamTracer;
      }

      public StreamTracer getStreamTracer() {
        return streamTracer;
      }

      // start auto(wrap:FSOutputStream)
      public void sync() throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        out.sync();

        streamTracer.logCall(ce, APITrace.CALL_sync,
                             null,
                             null);
      }


      public void write(int b) throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        out.write(b);

        streamTracer.logIOCall(ce, 1);
      }


      public void write(byte[] b) throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        out.write(b);

        streamTracer.logIOCall(ce, b.length);
      }


      public void write(byte[] b, int off, int len) throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        out.write(b, off, len);

        streamTracer.logIOCall(ce, len);
      }


      public void flush() throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        out.flush();

        streamTracer.logCall(ce, APITrace.CALL_flush,
                             null,
                             null);
      }


      public void close() throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        out.close();

        streamTracer.logCall(ce, APITrace.CALL_close2,
                             null,
                             null);
      }


      // end auto
    }

    public TraceFSDataOutputStream(FSDataOutputStream out,
                                   StreamTracer streamTracer) throws IOException {
      super(new TraceFSOutputStream(out, streamTracer),
            null, out.getPos());
      this.streamTracer = streamTracer;
    }

    public StreamTracer getStreamTracer() {
      return streamTracer;
    }
  }



  /*
   *  Tracer for input streams returned by FileSystem.open().
   * 
   *  The underlying file system will return an FSDataInputStream object upon open.
   *  Rather than adding log statements to the logic of every FSDataInputStream implementation we
   *  might want to trace, we simply return a wrapper around the stream that the underlying FS
   *  returned.  Our design is guided by three desirable properties for the wrapper:
   *
   *  (1) The wrapper must implement the FSDataInputStream interface (requirement).
   *  (2) The wrapper should have a relatively simple tracing API (desirable).
   *  (3) The wrapper should never silently perform I/O without logging,
   *      even if parent classes support new methods in the future (desirable).
   *
   *  In order to satisfy all three constraints, we need to provide two levels of wrappers
   *  over the object returned by the underlying FS.  In order to satisfy constraint
   *  (1), we return our own implementation of FSDataInputStream as the higher-level wrapper.
   *
   *  Unfortunately, this wrapper alone is not sufficient as it does not meet requirement (2):
   *  FSDataInputStream inherits a broad API from java.io.DataInputStream (e.g., we would need
   *  to trace readBoolean, readFloat, readUTF, etc if we traced at this level).  Also,
   *  simply tracing at the FSDataInputStream level would violate (3), as DataInputStreams
   *  automatically wrap an inner stream, so any new methods added to java.io.DataInputStream
   *  would be inherited by our implementation, meaning that I/O might occur without tracing.
   *  
   *  In order to satisfy (2) and (3), we use an implementation of hadoop.fs.FSInputStream as
   *  our lower-level wrapper.  This level performs all the necessary logging calls.
   */
  private static class TraceFSDataInputStream extends FSDataInputStream
    implements TraceableStream {

    private StreamTracer streamTracer = null;

    private static class TraceFSInputStream extends FSInputStream implements TraceableStream {
      private FSDataInputStream in;
      private StreamTracer streamTracer = null;

      TraceFSInputStream(FSDataInputStream in, StreamTracer streamTracer) {
        this.in = in;
        this.streamTracer = streamTracer;
      }

      public StreamTracer getStreamTracer() {
        return streamTracer;
      }

      // start auto(wrap:FSInputStream)
      public void seek(long pos) throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        in.seek(pos);

        streamTracer.logCall(ce, APITrace.CALL_seek,
                             null,
                             new Object[] {
                               pos});
      }


      public long getPos() throws IOException {
        return in.getPos();
      }


      public boolean seekToNewSource(long targetPos) throws IOException {
        APITrace.CallEvent ce;
        boolean rv;
        ce = new APITrace.CallEvent();

        rv = in.seekToNewSource(targetPos);

        streamTracer.logCall(ce, APITrace.CALL_seekToNewSource,
                             rv,
                             new Object[] {
                               targetPos});
        return rv;
      }


      public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {

        APITrace.CallEvent ce;
        int rv;
        ce = new APITrace.CallEvent();

        rv = in.read(position, buffer, offset, length);

        streamTracer.logCall(ce, APITrace.CALL_read,
                             rv,
                             new Object[] {
                               position,
                               offset,
                               length});
        return rv;
      }


      public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {

        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        in.readFully(position, buffer, offset, length);

        streamTracer.logCall(ce, APITrace.CALL_readFully,
                             null,
                             new Object[] {
                               position,
                               offset,
                               length});
      }


      public void readFully(long position, byte[] buffer) throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        in.readFully(position, buffer);

        streamTracer.logCall(ce, APITrace.CALL_readFully1,
                             null,
                             new Object[] {
                               position});
      }


      public int read() throws IOException {
        APITrace.CallEvent ce;
        int rv;
        ce = new APITrace.CallEvent();

        rv = in.read();

        if (rv >= 0) {
          streamTracer.logIOCall(ce, 1);
        } else {
          streamTracer.logCall(ce, APITrace.CALL_read1,
                               rv,
                               null);
        }
        return rv;
      }


      public int read(byte[] b) throws IOException {
        APITrace.CallEvent ce;
        int rv;
        ce = new APITrace.CallEvent();

        rv = in.read(b);

        if (rv >= 0) {
          streamTracer.logIOCall(ce, rv);
        } else {
          streamTracer.logCall(ce, APITrace.CALL_read2,
                               rv,
                               null);
        }
        return rv;
      }


      public int read(byte[] b, int off, int len) throws IOException {
        APITrace.CallEvent ce;
        int rv;
        ce = new APITrace.CallEvent();

        rv = in.read(b, off, len);

        if (rv >= 0) {
          streamTracer.logIOCall(ce, rv);
        } else {
          streamTracer.logCall(ce, APITrace.CALL_read3,
                               rv,
                               new Object[] {
                                 off,
                                 len});
        }
        return rv;
      }


      public long skip(long n) throws IOException {
        APITrace.CallEvent ce;
        long rv;
        ce = new APITrace.CallEvent();

        rv = in.skip(n);

        streamTracer.logCall(ce, APITrace.CALL_skip,
                             rv,
                             new Object[] {
                               n});
        return rv;
      }


      public int available() throws IOException {
        APITrace.CallEvent ce;
        int rv;
        ce = new APITrace.CallEvent();

        rv = in.available();

        streamTracer.logCall(ce, APITrace.CALL_available,
                             rv,
                             null);
        return rv;
      }


      public void close() throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        in.close();

        streamTracer.logCall(ce, APITrace.CALL_close1,
                             null,
                             null);
      }


      public void mark(int readlimit) {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        in.mark(readlimit);

        streamTracer.logCall(ce, APITrace.CALL_mark,
                             null,
                             new Object[] {
                               readlimit});
      }


      public void reset() throws IOException {
        APITrace.CallEvent ce;
        ce = new APITrace.CallEvent();

        in.reset();

        streamTracer.logCall(ce, APITrace.CALL_reset,
                             null,
                             null);
      }


      // end auto
    }

    public TraceFSDataInputStream(FSDataInputStream in,
                                  StreamTracer streamTracer) throws IOException {
      super(new TraceFSInputStream(in, streamTracer));
      this.streamTracer = streamTracer;
    }

    public StreamTracer getStreamTracer() {
      return streamTracer;
    }
  }
}
