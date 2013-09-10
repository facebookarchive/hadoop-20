package org.apache.hadoop.raid;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

public class ParityFilePair {

  final private Path path;
  final private FileStatus stat;
  final private FileSystem fs;
  // Only used for dir raid
  final private List<FileStatus> lfs;

  private ParityFilePair(Path path, FileStatus stat, FileSystem fs,
      List<FileStatus> lfs) {
    this.path = path;
    this.stat = stat;
    this.fs = fs;
    this.lfs = lfs;
  }

  public Path getPath() {
    return this.path;
  }

  public FileStatus getFileStatus() {
    return this.stat;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }
  
  public List<FileStatus> getListFileStatus() {
    return this.lfs;
  }

  /**
   * Return whether if parity file of the source file exists or not
   * 
   * @param src     The FileStatus of the source file.
   * @param codec   The Codec of the parity to check
   * @param conf
   * @return
   * @throws IOException
   */
  public static boolean parityExists(FileStatus src, Codec codec,
      Configuration conf) throws IOException {
    return ParityFilePair.getParityFile(codec, src, conf) != null;
  }
  
  public static ParityFilePair getParityFile(Codec codec, FileStatus srcStat,
      Configuration conf) throws IOException {
    return getParityFile(codec, srcStat, conf, false);
  }

  /**
   * Returns the Path to the parity file of a given file / directory
   *
   * @param codec The Codec of the parity
   * @param srcStat FileStatus of the original source file
   * @param skip the Har file checking or not
   * @return ParityFilePair representing the parity file of the source
   * @throws IOException
   */
  public static ParityFilePair getParityFile(Codec codec, FileStatus srcStat,
      Configuration conf, boolean skipHarChecking) throws IOException {

    if (srcStat == null) {
      return null;
    }
    Path srcPath = srcStat.getPath();
    FileSystem fsSrc = srcPath.getFileSystem(conf);

    if (codec.isDirRaid) {
      if (!srcStat.isDir()) {
        // directory raid needs a directory to get parity file
        srcPath = srcPath.getParent();
        try {
          srcStat = fsSrc.getFileStatus(srcPath);
        } catch (FileNotFoundException e) {
          return null;
        }
      }
    } 
    Path srcParent = srcPath.getParent();
    //We assume that parity file and source file live in the same cluster
    FileSystem fsDest = fsSrc; 
    Path destPathPrefix = fsDest.makeQualified(new Path(
        codec.parityDirectory));

    //CASE 1: CHECK HAR - Must be checked first because har is created after
    // parity file and returning the parity file could result in error while
    // reading it.
    Path outPath =  RaidNode.getOriginalParityFile(destPathPrefix, srcPath);
    if (!codec.isDirRaid && !skipHarChecking) {
      Path outDir = RaidNode.getOriginalParityFile(destPathPrefix, srcParent);
      String harDirName = srcParent.getName() + RaidNode.HAR_SUFFIX;
      Path HarPath = new Path(outDir, harDirName);
      if (fsDest.exists(HarPath)) {
        URI HarPathUri = HarPath.toUri();
        Path inHarPath = new Path("har://",HarPathUri.getPath()+"/"+outPath.toUri().getPath());
        FileSystem fsHar = new HarFileSystem(fsDest);
        fsHar.initialize(inHarPath.toUri(), conf);
        FileStatus inHar = FileStatusCache.get(fsHar, inHarPath);
        if (inHar != null) {
          ParityFilePair pfp = verifyParity(srcStat, inHar, codec, conf, fsHar);
          if (pfp != null) {
            return pfp;
          }
        }
      }
    }

    //CASE 2: CHECK PARITY
    try {
      FileStatus outHar = fsDest.getFileStatus(outPath);
      ParityFilePair pfp = verifyParity(srcStat, outHar, codec, conf, fsDest);
      if (pfp != null) {
        return pfp;
      }
    } catch (java.io.FileNotFoundException e) {
    }

    return null; // NULL if no parity file
  }

  /*
   * verify and return ParityFilePair if succeed, or return null
   */
  static ParityFilePair verifyParity(FileStatus src, FileStatus parity,
      Codec codec, Configuration conf, FileSystem destFs) throws IOException {
    if (parity.getModificationTime() != src.getModificationTime()) {
      return null;
    }
    int stripeLength = codec.stripeLength;
    int parityLegnth = codec.parityLength;
    long expectedSize = 0;
    List<FileStatus> lfs = null;
    if (codec.isDirRaid) {
      FileSystem srcFs = src.getPath().getFileSystem(conf);
      lfs = RaidNode.listDirectoryRaidFileStatus(conf, srcFs, 
          src.getPath());
      if (lfs == null) {
        return null;
      }

      long blockNum = DirectoryStripeReader.getBlockNum(lfs);
      long parityBlockSize = DirectoryStripeReader.getParityBlockSize(conf,
          lfs); 
      int parityBlocks = (int)Math.ceil(
          ((double)blockNum) / stripeLength) * parityLegnth;
      expectedSize = parityBlocks * parityBlockSize;
    } else {
      double sourceBlocks = Math.ceil(
          ((double)src.getLen()) / src.getBlockSize());
      int parityBlocks = (int)Math.ceil(
          sourceBlocks / stripeLength) * parityLegnth;
      expectedSize = parityBlocks * src.getBlockSize();
    }
    if (parity.getLen() != expectedSize) {
      RaidNode.LOG.error("Bad parity file:" + parity.getPath() +
          " File size doen't match. parity:" + parity.getLen() +
          " expected:" + expectedSize);
      return null;
    }
    return new ParityFilePair(parity.getPath(), parity, destFs, lfs);
  }

  /**
   * Caches the listStatus result in a thread local cache. This greatly
   * increases the speed of repeatedly calling getParityFile() from
   * the same thread.
   */
  static class FileStatusCache {

    private static final long CACHE_STALE_PERIOD = 60 * 1000L;

    // The threadlocal stores a separate cache for each thread
    private static ThreadLocal<Map<Path, FileStatusWithTime>> tLocalCaches =
        new ThreadLocal<Map<Path, FileStatusWithTime>>();

    /**
     * Do getFileStatus with caching
     * @param fs FileSystem
     * @param file The path to do getFileStatus
     * @return FileStatus of the file
     * @throws IOException
     */
    static FileStatus get(FileSystem fs, Path file) throws IOException {

      long now = RaidNode.now();
      Map<Path, FileStatusWithTime> cache = tLocalCaches.get();

      // If the current thread do not have a cache, create one.
      if (cache == null || !useCache) {
        cache = new HashMap<Path, FileStatusWithTime>();
        tLocalCaches.set(cache);
      }
      FileStatusWithTime fileStatusWithTime = cache.get(file);
      if (fileStatusWithTime != null) {
        if (now - fileStatusWithTime.time > CACHE_STALE_PERIOD) {
          cache.remove(file);
        } else {
          return fileStatusWithTime.fileStatus;
        }
      }
      Path parent = file.getParent();
      // We cache the FileStatus in one directory. When move to a different
      // directory, the cache is cleared.
      cache.clear();
      FileStatus[] files = null;
      try {
        files = fs.listStatus(parent);
      } catch (FileNotFoundException e) {
      }
      if (files == null) {
        return null;
      }
      for (FileStatus status : files) {
        cache.put(status.getPath(), new FileStatusWithTime(status, now));
      }
      if (cache.containsKey(file)) {
        return cache.get(file).fileStatus;
      } else {
        return null;
      }
    }

  }

  private static class FileStatusWithTime {
    final FileStatus fileStatus;
    final long time;
    FileStatusWithTime(FileStatus fileStatus, long time) {
      this.fileStatus = fileStatus;
      this.time = time;
    }
  }

  private static boolean useCache = true;

  /**
   * Disable the cache of FileStatus
   * SHOULD NOT BE USED BY ANY PRODUCTION CODE
   */
  public static void disableCacheUsedInTestOnly() {
    useCache = false;
  }

}
