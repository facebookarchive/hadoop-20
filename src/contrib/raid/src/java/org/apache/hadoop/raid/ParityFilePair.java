package org.apache.hadoop.raid;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
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

  private ParityFilePair(Path path, FileStatus stat, FileSystem fs) {
    this.path = path;
    this.stat = stat;
    this.fs = fs;
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

  /**
   * Returns the Path to the parity file of a given file
   *
   * @param code The ErasureCodeType of the parity
   * @param srcPath Path to the original source file
   * @return ParityFilePair representing the parity file of the source
   * @throws IOException
   */
  public static ParityFilePair getParityFile(ErasureCodeType code, Path srcPath,
      Configuration conf) throws IOException {

    Path destPathPrefix = RaidNode.getDestinationPath(code, conf);
    Path srcParent = srcPath.getParent();

    FileSystem fsDest = destPathPrefix.getFileSystem(conf);
    FileSystem fsSrc = srcPath.getFileSystem(conf);

    FileStatus srcStatus = null;
    try {
      srcStatus = fsSrc.getFileStatus(srcPath);
    } catch (java.io.FileNotFoundException e) {
      return null;
    }

    Path outDir = destPathPrefix;
    if (srcParent != null) {
      if (srcParent.getParent() == null) {
        outDir = destPathPrefix;
      } else {
        outDir = new Path(destPathPrefix, RaidNode.makeRelative(srcParent));
      }
    }

    //CASE 1: CHECK HAR - Must be checked first because har is created after
    // parity file and returning the parity file could result in error while
    // reading it.
    Path outPath =  RaidNode.getOriginalParityFile(destPathPrefix, srcPath);
    String harDirName = srcParent.getName() + RaidNode.HAR_SUFFIX;
    Path HarPath = new Path(outDir,harDirName);
    if (fsDest.exists(HarPath)) {
      URI HarPathUri = HarPath.toUri();
      Path inHarPath = new Path("har://",HarPathUri.getPath()+"/"+outPath.toUri().getPath());
      FileSystem fsHar = new HarFileSystem(fsDest);
      fsHar.initialize(inHarPath.toUri(), conf);
      FileStatus inHar = FileStatusCache.get(fsHar, inHarPath);
      if (inHar != null) {
        if (verifyParity(srcStatus, inHar, code, conf)) {
          return new ParityFilePair(inHarPath, inHar, fsHar);
        }
      }
    }

    //CASE 2: CHECK PARITY
    try {
      FileStatus outHar = fsDest.getFileStatus(outPath);
      if (verifyParity(srcStatus, outHar, code, conf)) {
        return new ParityFilePair(outPath, outHar, fsDest);
      }
    } catch (java.io.FileNotFoundException e) {
    }

    return null; // NULL if no parity file
  }

  static boolean verifyParity(FileStatus src, FileStatus parity,
      ErasureCodeType code, Configuration conf) {
    if (parity.getModificationTime() != src.getModificationTime()) {
      return false;
    }
    int stripeLength = RaidNode.getStripeLength(conf);
    int parityLegnth = ErasureCodeType.XOR == code ? 1 :
        RaidNode.rsParityLength(conf);
    double sourceBlocks = Math.ceil(
        ((double)src.getLen()) / src.getBlockSize());
    int parityBlocks = (int)Math.ceil(
        sourceBlocks / stripeLength) * parityLegnth;
    long expectedSize = parityBlocks * src.getBlockSize();
    if (parity.getLen() != expectedSize) {
      RaidNode.LOG.error("Bad parity file:" + parity.getPath() +
          " File size doen't match. parity:" + parity.getLen() +
          " expected:" + expectedSize);
      return false;
    }
    return true;
  }

  /**
   * Caches the listStatus result in a thread local cache. This greatly
   * increases the speed of repeatedly calling getParityFile() from
   * the same thread.
   */
  private static class FileStatusCache {

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
      for (FileStatus status : fs.listStatus(parent)) {
        cache.put(status.getPath(), new FileStatusWithTime(status, now));
      }
      return cache.get(file).fileStatus;
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
