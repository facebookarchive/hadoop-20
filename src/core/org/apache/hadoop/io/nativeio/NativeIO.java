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
package org.apache.hadoop.io.nativeio;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.util.InjectionEventCore;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * JNI wrappers for various native IO-related calls not available in Java.
 * These functions should generally be used alongside a fallback to another
 * more portable mechanism.
 */
public class NativeIO {
  // Flags for open() call from bits/fcntl.h
  public static final int O_RDONLY   =    00;
  public static final int O_WRONLY   =    01;
  public static final int O_RDWR     =    02;
  public static final int O_CREAT    =  0100;
  public static final int O_EXCL     =  0200;
  public static final int O_NOCTTY   =  0400;
  public static final int O_TRUNC    = 01000;
  public static final int O_APPEND   = 02000;
  public static final int O_NONBLOCK = 04000;
  public static final int O_SYNC   =  010000;
  public static final int O_ASYNC  =  020000;
  public static final int O_FSYNC = O_SYNC;
  public static final int O_NDELAY = O_NONBLOCK;

  // Flags for posix_fadvise() from bits/fcntl.h
  /* No further special treatment.  */
  public static final int POSIX_FADV_NORMAL = 0; 
  /* Expect random page references.  */
  public static final int POSIX_FADV_RANDOM = 1; 
  /* Expect sequential page references.  */
  public static final int POSIX_FADV_SEQUENTIAL = 2; 
  /* Will need these pages.  */
  public static final int POSIX_FADV_WILLNEED = 3; 
  /* Don't need these pages.  */
  public static final int POSIX_FADV_DONTNEED = 4; 
  /* Data will be accessed once.  */
  public static final int POSIX_FADV_NOREUSE = 5;

  // Flags for clock_gettime from time.h
  public static final int CLOCK_REALTIME           = 0;
  public static final int CLOCK_MONOTONIC          = 1;
  public static final int CLOCK_PROCESS_CPUTIME_ID = 2;
  public static final int CLOCK_THREAD_CPUTIME_ID  = 3;
  public static final int CLOCK_MONOTONIC_RAW      = 4;
  public static final int CLOCK_REALTIME_COARSE    = 5;
  public static final int CLOCK_MONOTONIC_COARSE   = 6;


  /* Wait upon writeout of all pages
     in the range before performing the
     write.  */
  public static final int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
  /* Initiate writeout of all those
     dirty pages in the range which are
     not presently under writeback.  */
  public static final int SYNC_FILE_RANGE_WRITE = 2;

  /* Wait upon writeout of all pages in
     the range after performing the
     write.  */
  public static final int SYNC_FILE_RANGE_WAIT_AFTER = 4;

  private static final Log LOG = LogFactory.getLog(NativeIO.class);

  private static boolean nativeLoaded = false;
  private static boolean workaroundNonThreadSafePasswdCalls = false;
  private static boolean fadvisePossible = true;
  private static boolean ioprioPossible = true;
  private static boolean syncFileRangePossible = true;

  static final String WORKAROUND_NON_THREADSAFE_CALLS_KEY =
    "hadoop.workaround.non.threadsafe.getpwuid";
  static final boolean WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT = false;

  // Copied from ioprio.h
  public static final int IOPRIO_CLASS_NONE = 0;
  public static final int IOPRIO_CLASS_RT = 1;
  public static final int IOPRIO_CLASS_BE = 2;
  public static final int IOPRIO_CLASS_IDLE = 3;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        Configuration conf = new Configuration();
        workaroundNonThreadSafePasswdCalls = conf.getBoolean(
          WORKAROUND_NON_THREADSAFE_CALLS_KEY,
          WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT);

        initNative();
        nativeLoaded = true;
      } catch (Throwable t) {
        // This can happen if the user has an older version of libhadoop.so
        // installed - in this case we can continue without native IO
        // after warning
        LOG.error("Unable to initialize NativeIO libraries", t);
      }
    }
  }

  public static boolean isfadvisePossible() {
    return fadvisePossible;
  }

  public static boolean isIoprioPossible() {
    return ioprioPossible;
  }

  /**
   * Return true if the JNI-based native IO extensions are available.
   */
  public static boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
  }

  /** Wrapper around open(2) */
  public static native FileDescriptor open(String path, int flags, int mode) throws IOException;
  /** Wrapper around fstat(2) */
  public static native Stat fstat(FileDescriptor fd) throws IOException;
  /** Wrapper around stat(2) */
  public static native Stat stat(String path) throws IOException;
  /** Wrapper around link(2) */
  public static native void link(String src, String dst) throws IOException;
  /** Wrapper around chmod(2) */
  public static native void chmod(String path, int mode) throws IOException;
  /** Wrapper around fsync(2) (Java does not support fsync on directory) */
  public static native void fsync(String path) throws IOException;

  /** Wrapper around posix_fadvise(2) */
  public static native void posix_fadvise(
    FileDescriptor fd, long offset, long len, int flags) throws NativeIOException;

  /** Wrapper around sync_file_range(2) */
  static native void sync_file_range(
    FileDescriptor fd, long offset, long nbytes, int flags) throws NativeIOException;

  /** Initialize the JNI method ID and class ID cache */
  private static native void initNative();

  /**
   * Wrapper around ioprio_set, we always do this for the current thread so
   * we omit 'which' and 'who'.
   */
  static native void ioprio_set(int classOfService, int priority) throws IOException;

  /**
   * Wrapper around ioprio_set, we always do this for the current thread so
   * we omit 'which' and 'who'. This is different from ioprio_set(class,
   * priority) in the sense that this is the value that is returned by
   * ioprio_get and we can directly pass this value in to reset the
   * priority of a thread.
   */
  static native void ioprio_set(int ioprio_prio_value) throws IOException;

  /**
   * Wrapper around ioprio_get, we always do this for the current thread so
   * we omit 'which' and 'who'.
   */
  static native int ioprio_get() throws IOException;

  /**
   * Wrapper around native stat()
   */
  public static Stat stat(File file) throws IOException {
    if (file == null) {
      throw new IllegalArgumentException("Null parameter passed");
    }
    return stat(file.getAbsolutePath());
  }

  /**
   * Wrapper around native link()
   */
  public static void link(File src, File dst) throws IOException {
    if (src == null || dst == null) {
      throw new IllegalArgumentException("Null parameter passed");
    }
    if (isAvailable()) {
      link(src.getAbsolutePath(), dst.getAbsolutePath());
    } else {
      HardLink.createHardLink(src, dst);
    }
  }

  /**
   * Wrapper around native clock_gettime()
   */
  public static native void clock_gettime(int which_clock,
                                          TimeSpec tp) throws IOException;

  public static void validateIoprioSet(int classOfService, int data) {
    if (classOfService < 0 || classOfService > 3) {
      throw new IllegalArgumentException("Invalid class of service : "
          + classOfService + " (0-3) supported");
    }
    if (data < 0 || data > 7) {
      throw new IllegalArgumentException("Invalid class of service : "
          + classOfService + " (0-7) supported");
    }
  }
  public static void validatePosixFadvise(int advise) {
    if (advise < NativeIO.POSIX_FADV_NORMAL
        || advise > NativeIO.POSIX_FADV_NOREUSE) {
      throw new IllegalArgumentException("Invalid posix fadvise : " + advise);
    }
  }

  /**
   * Call ioprio_get for this thread.
   *
   * @throws NativeIOException
   *           if there is an error with the syscall
   * @return -1 on failure, ioprio value on success.
   */
  public static int ioprioGetIfPossible() throws IOException {
    if (nativeLoaded && ioprioPossible) {
      try {
        return ioprio_get();
      } catch (UnsupportedOperationException uoe) {
        LOG.warn("ioprioGetIfPossible() failed", uoe);
        ioprioPossible = false;
      } catch (UnsatisfiedLinkError ule) {
        LOG.warn("ioprioGetIfPossible() failed", ule);
        ioprioPossible = false;
      } catch (NativeIOException nie) {
        LOG.warn("ioprioGetIfPossible() failed", nie);
        throw nie;
      }
    }
    return -1;
  }

  /**
   * Call ioprio_set(ioprio_value) for this thread.
   *
   * @throws NativeIOException
   *           if there is an error with the syscall
   */
  public static void ioprioSetIfPossible(int ioprio_value) throws IOException {
    if (nativeLoaded && ioprioPossible) {
      try {
        ioprio_set(ioprio_value);
      } catch (UnsupportedOperationException uoe) {
        LOG.warn("ioprioSetIfPossible() failed", uoe);
        ioprioPossible = false;
      } catch (UnsatisfiedLinkError ule) {
        LOG.warn("ioprioSetIfPossible() failed", ule);
        ioprioPossible = false;
      } catch (NativeIOException nie) {
        LOG.warn("ioprioSetIfPossible() failed", nie);
        throw nie;
      }
    }
  }

  /**
   * Call ioprio_set(class, data) for this thread.
   *
   * @throws NativeIOException
   *           if there is an error with the syscall
   */
  public static void ioprioSetIfPossible(int classOfService, int data)
      throws IOException {
    if (nativeLoaded && ioprioPossible) {
      if (classOfService == IOPRIO_CLASS_NONE) {
        // ioprio is disabled.
        return;
      }
      try {
        ioprio_set(classOfService, data);
      } catch (UnsupportedOperationException uoe) {
        LOG.warn("ioprioSetIfPossible() failed", uoe);
        ioprioPossible = false;
      } catch (UnsatisfiedLinkError ule) {
        LOG.warn("ioprioSetIfPossible() failed", ule);
        ioprioPossible = false;
      } catch (NativeIOException nie) {
        LOG.warn("ioprioSetIfPossible() failed", nie);
        throw nie;
      }
    }
  }

  /**
   * Calls fsync on the given file/dir path.
   */
  public static void fsyncIfPossible(String path) throws IOException {
    if (nativeLoaded) {
      fsync(path);
    } else {
      LOG.warn("Cannot fsync : " + path +
          " since native libraries are not available");
    }
  }

  /**
   * Call posix_fadvise on the given file descriptor. See the manpage
   * for this syscall for more information. On systems where this
   * call is not available, does nothing.
   *
   * @throws NativeIOException if there is an error with the syscall
   */
  public static void posixFadviseIfPossible(
      FileDescriptor fd, long offset, long len, int flags)
      throws NativeIOException {
    if (nativeLoaded && fadvisePossible) {
      try {
        posix_fadvise(fd, offset, len, flags);
        InjectionHandler.processEvent(
            InjectionEventCore.NATIVEIO_POSIX_FADVISE, flags);
      } catch (UnsupportedOperationException uoe) {
        LOG.warn("posixFadviseIfPossible() failed", uoe);
        fadvisePossible = false;
      } catch (UnsatisfiedLinkError ule) {
        LOG.warn("posixFadviseIfPossible() failed", ule);
        fadvisePossible = false;
      } catch (NativeIOException nie) {
        LOG.warn("posixFadviseIfPossible() failed", nie);
        throw nie;
      }
    }
  }

  /**
   * Call sync_file_range on the given file descriptor. See the manpage
   * for this syscall for more information. On systems where this
   * call is not available, does nothing.
   *
   * @throws NativeIOException if there is an error with the syscall
   */
  public static void syncFileRangeIfPossible(
      FileDescriptor fd, long offset, long nbytes, int flags)
      throws NativeIOException {
    InjectionHandler.processEvent(InjectionEventCore.NATIVEIO_SYNC_FILE_RANGE,
        flags);
    if (nativeLoaded && syncFileRangePossible) {
      try {
        sync_file_range(fd, offset, nbytes, flags);
      } catch (UnsupportedOperationException uoe) {
        LOG.warn("syncFileRangeIfPossible() failed", uoe);
        syncFileRangePossible = false;
      } catch (UnsatisfiedLinkError ule) {
        LOG.warn("syncFileRangeIfPossible() failed", ule);
        syncFileRangePossible = false;
      } catch (NativeIOException nie) {
        LOG.warn("syncFileRangeIfPossible() failed: fd " + fd + " offset "
            + offset + " nbytes " + nbytes + " flags " + flags, nie);
        throw nie;
      }
    }
  }

  public static void clockGetTimeIfPossible(int which_clock,
                                            TimeSpec tp) throws IOException {
    if (nativeLoaded) {
      clock_gettime(which_clock, tp);
    } else {
      throw new IOException("Native not loaded.");
    }
  }



  /**
   * Result type of the fstat call
   */
  public static class Stat {
    private final String owner, group;
    private final int mode;
    // The number of hardlinks for this file.
    private final int hardlinks;
    // The inode number for this file.
    private final long inode;

    // Mode constants
    public static final int S_IFMT = 0170000;      /* type of file */
    public static final int   S_IFIFO  = 0010000;  /* named pipe (fifo) */
    public static final int   S_IFCHR  = 0020000;  /* character special */
    public static final int   S_IFDIR  = 0040000;  /* directory */
    public static final int   S_IFBLK  = 0060000;  /* block special */
    public static final int   S_IFREG  = 0100000;  /* regular */
    public static final int   S_IFLNK  = 0120000;  /* symbolic link */
    public static final int   S_IFSOCK = 0140000;  /* socket */
    public static final int   S_IFWHT  = 0160000;  /* whiteout */
    public static final int S_ISUID = 0004000;  /* set user id on execution */
    public static final int S_ISGID = 0002000;  /* set group id on execution */
    public static final int S_ISVTX = 0001000;  /* save swapped text even after use */
    public static final int S_IRUSR = 0000400;  /* read permission, owner */
    public static final int S_IWUSR = 0000200;  /* write permission, owner */
    public static final int S_IXUSR = 0000100;  /* execute/search permission, owner */

    Stat(String owner, String group, int mode, int hardlinks, long inode) {
      this.owner = owner;
      this.group = group;
      this.mode = mode;
      this.hardlinks = hardlinks;
      this.inode = inode;
    }

    public String toString() {
      return "Stat(owner='" + owner + "', group='" + group + "'" +
        ", mode=" + mode + ", hardlinks=" + hardlinks +
        ", inode=" + inode + ")";
    }

    public String getOwner() {
      return owner;
    }
    public String getGroup() {
      return group;
    }
    public int getMode() {
      return mode;
    }
    public long getInode() {
      return this.inode;
    }
    public int getHardLinks() {
      return this.hardlinks;
    }
  }

  /**
   * Result type of the clock_gettime call
   */
  public static class TimeSpec {
    public long tv_sec = 0;
    public long tv_nsec = 0;

    public String toString() {
      return "{tv_sec=" + tv_sec + "," + "tv_nsec=" + tv_nsec + "}";
    }
  }
}
