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
package org.apache.hadoop.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.DataTransferThrottler;
import org.apache.hadoop.util.IOThrottler;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.StringUtils;

/** Provide command line access to a FileSystem. */
public class FsShell extends Configured implements Tool {

  protected FileSystem fs;
  private Trash trash;
  public static final SimpleDateFormat dateForm =
    new SimpleDateFormat("yyyy-MM-dd HH:mm");
  protected static final SimpleDateFormat modifFmt =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static final int BORDER = 2;
  static {
    modifFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
  }
  static final String SETREP_SHORT_USAGE="-setrep [-R] [-w] <rep> <path/file ...>";
  static final int SETREP_MAX_PATHS = 1024;
  static final String GET_SHORT_USAGE = "-get [-rate <bandwidth>] [-ignoreCrc] [-crc] [-gencrc] [-start offset] <src> <localdst>";
  static final String COPYTOLOCAL_SHORT_USAGE = GET_SHORT_USAGE.replace(
      "-get", "-copyToLocal");
  static final String HEAD_USAGE="-head <file>";
  static final String TAIL_USAGE="-tail [-f] <file>";

  /**
   */
  public FsShell() {
    this(null);
  }

  public FsShell(Configuration conf) {
    super(conf);
    fs = null;
    trash = null;
  }

  protected FileSystem getFS() throws IOException {
    if (this.fs != null) {
      return this.fs;
    }
    synchronized(this) {
      if (this.fs == null) {
        this.fs = FileSystem.get(getConf());
      }
    }
    return this.fs;
  }
  
  protected Trash getTrash() throws IOException{
    if (this.trash != null) {
      return this.trash;
    }
    synchronized(this) {
      if (this.trash == null) {
        this.trash = new Trash(getConf());
      }
    }
    return this.trash;
  }
  
  public void init() throws IOException {
    getConf().setQuietMode(true);
  }

  public enum LsOption {
    Recursive,
    WithBlockSize,
    DirectoryOnly
  };

  /**
   * Copies from stdin to the indicated file.
   */
  private void copyFromStdin(Path dst, FileSystem dstFs, IOThrottler throttler) throws IOException {
    if (dstFs.isDirectory(dst)) {
      throw new IOException("When source is stdin, destination must be a file.");
    }
    if (dstFs.exists(dst)) {
      throw new IOException("Target " + dst.toString() + " already exists.");
    }
    FSDataOutputStream out = dstFs.create(dst);
    try {
      IOUtils.copyBytes(System.in, out,
          getConf().getInt("io.file.buffer.size", 4096), false, throttler);
    }
    finally {
      out.close();
    }
  }

  /**
   * Print from src to stdout 
   */
  private void printToStdout(InputStream in) throws IOException {
    try {
      IOUtils.copyBytes(in, System.out, getConf(), false);
    } finally {
      in.close();
    }
  }
  /**
   * Print from src to stdout
   * And print the checksum to stderr
   */
  private void printToStdoutAndCRCToStderr(InputStream in, Path file) throws IOException {
    try {
      long checksum = IOUtils.copyBytesAndGenerateCRC(in, System.out, getConf(), false);
      FileUtil.printChecksumToStderr(checksum, file);
    } finally {
      in.close();
    }
  }


  /**
   * Add local files to the indicated FileSystem name. src is kept.
   */
  void copyFromLocal(Path[] srcs, String dstf, boolean validate, IOThrottler throttler)
  throws IOException {
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    if (srcs.length == 1 && srcs[0].toString().equals("-"))
      copyFromStdin(dstPath, dstFs, throttler);
    else {
      FileUtil.copy(
          FileSystem.getLocal(getConf()), srcs, dstFs, dstPath,
          false, false, validate, getConf(), throttler);
    }
  }

  /**
   * Add local files to the indicated FileSystem name. src is removed.
   */
  void moveFromLocal(Path[] srcs, String dstf) throws IOException {
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    dstFs.moveFromLocalFile(srcs, dstPath);
  }

  /**
   * Add a local file to the indicated FileSystem name. src is removed.
   */
  void moveFromLocal(Path src, String dstf) throws IOException {
    moveFromLocal((new Path[]{src}), dstf);
  }

  /**
   * Obtain the indicated files that match the file pattern <i>srcf</i>
   * and copy them to the local name. srcf is kept.
   * When copying multiple files, the destination must be a directory.
   * Otherwise, IOException is thrown.
   * @param argv: arguments
   * @param pos: Ignore everything before argv[pos]
   * @param throttler: IOThrottler for copy
   * @exception: IOException
   * @see org.apache.hadoop.fs.FileSystem.globStatus
   */
  void copyToLocal(String[]argv, int pos, IOThrottler throttler) throws IOException {
    CommandFormat cf = new CommandFormat("copyToLocal", 2,3,"crc","ignoreCrc", 
        "gencrc", "validate", "start");

    String srcstr = null;
    String dststr = null;
    long offset = 0L;
    try {
      List<String> parameters = cf.parse(argv, pos);
      int curPos = 0;
      if (parameters.size() == 3) {
        offset = Long.parseLong(parameters.get(curPos++));
      }
      srcstr = parameters.get(curPos++);
      dststr = parameters.get(curPos++);
    }
    catch(IllegalArgumentException iae) {
      System.err.println("Usage: java FsShell " + GET_SHORT_USAGE);
      throw iae;
    }
    boolean copyCrc = cf.getOpt("crc");
    final boolean genCrc = cf.getOpt("gencrc");
    final boolean verifyChecksum = !cf.getOpt("ignoreCrc");
    final boolean validate = cf.getOpt("validate");

    if (dststr.equals("-")) {
      if (copyCrc) {
        System.err.println("-crc option is not valid when destination is stdout.");
      }
      cat(srcstr, verifyChecksum, genCrc, offset);
    } else {
      File dst = new File(dststr);
      Path srcpath = new Path(srcstr);
      FileSystem srcFS = getSrcFileSystem(srcpath, verifyChecksum);
      if (copyCrc && !(srcFS instanceof ChecksumFileSystem)) {
        System.err.println("-crc option is not valid when source file system " +
            "does not have crc files. Automatically turn the option off.");
        copyCrc = false;
      }
      FileStatus[] srcs = srcFS.globStatus(srcpath);
      boolean dstIsDir = dst.isDirectory();
      if (srcs.length > 1 && !dstIsDir) {
        throw new IOException("When copying multiple files, "
                              + "destination should be a directory.");
      }
      for (FileStatus status : srcs) {
        File f = dstIsDir? new File(dst, status.getPath().getName()): dst;
        copyToLocal(srcFS, status, f, copyCrc, genCrc, validate, offset, throttler);
      }
    }
  }

  /**
   * Return the {@link FileSystem} specified by src and the conf.
   * It the {@link FileSystem} supports checksum, set verifyChecksum.
   */
  private FileSystem getSrcFileSystem(Path src, boolean verifyChecksum
      ) throws IOException {
    FileSystem srcFs = src.getFileSystem(getConf());
    srcFs.setVerifyChecksum(verifyChecksum);
    return srcFs;
  }

  /**
   * The prefix for the tmp file used in copyToLocal.
   * It must be at least three characters long, required by
   * {@link java.io.File#createTempFile(String, String, File)}.
   */
  static final String COPYTOLOCAL_PREFIX = "_copyToLocal_";

  /**
   * Copy a source file from a given file system to local destination.
   * @param srcFS source file system
   * @param srcStatus File status of source path
   * @param dst destination
   * @param copyCrc copy CRC files?
   * @param generate CRC or not?
   * @param validate validate copied destination against source
   * @param offset start offset of the copying
   * @exception IOException If some IO failed
   */
  private void copyToLocal(final FileSystem srcFS, final FileStatus srcStatus,
                           final File dst, final boolean copyCrc,
                           final boolean genCrc, final boolean validate,
                           long offset, IOThrottler throttler)
    throws IOException {
    /* Keep the structure similar to ChecksumFileSystem.copyToLocal().
     * Ideal these two should just invoke FileUtil.copy() and not repeat
     * recursion here. Of course, copy() should support two more options :
     * copyCrc and useTmpFile (may be useTmpFile need not be an option).
     */
    Path src = srcStatus.getPath();
    if (!srcStatus.isDir()) {
      if (dst.exists()) {
        // match the error message in FileUtil.checkDest():
        throw new IOException("Target " + dst + " already exists");
      }

      // use absolute name so that tmp file is always created under dest dir
      File tmp = FileUtil.createLocalTempFile(dst.getAbsoluteFile(),
                                              COPYTOLOCAL_PREFIX, true);
      if (!FileUtil.copy(srcFS, src, tmp, false, validate, srcFS.getConf(),
          genCrc, offset, throttler)) {
        throw new IOException("Failed to copy " + src + " to " + dst);
      }

      if (!tmp.renameTo(dst)) {
        throw new IOException("Failed to rename tmp file " + tmp +
                              " to local destination \"" + dst + "\".");
      }
      
      if (copyCrc) {
        if (!(srcFS instanceof ChecksumFileSystem)) {
          throw new IOException("Source file system does not have crc files");
        }

        ChecksumFileSystem csfs = (ChecksumFileSystem) srcFS;
        File dstcs = FileSystem.getLocal(srcFS.getConf())
          .pathToFile(csfs.getChecksumFile(new Path(dst.getCanonicalPath())));
        copyToLocal(csfs.getRawFileSystem(), csfs.getFileStatus(
            csfs.getChecksumFile(src)), dstcs, false, genCrc, validate, offset, throttler);
      }
    } else {
      // once FileUtil.copy() supports tmp file, we don't need to mkdirs().
      dst.mkdirs();
      for(FileStatus path : srcFS.listStatus(src)) {
        copyToLocal(srcFS, path,
                    new File(dst, path.getPath().getName()), copyCrc,
                    genCrc, validate, offset, throttler);
      }
    }
  }

  /**
   * Get all the files in the directories that match the source file
   * pattern and merge and sort them to only one file on local fs
   * srcf is kept.
   * @param srcf: a file pattern specifying source files
   * @param dstf: a destination local file/directory
   * @exception: IOException
   * @see org.apache.hadoop.fs.FileSystem.globStatus
   */
  void copyMergeToLocal(String srcf, Path dst) throws IOException {
    copyMergeToLocal(srcf, dst, false);
  }


  /**
   * Get all the files in the directories that match the source file pattern
   * and merge and sort them to only one file on local fs
   * srcf is kept.
   *
   * Also adds a string between the files (useful for adding \n
   * to a text file)
   * @param srcf: a file pattern specifying source files
   * @param dstf: a destination local file/directory
   * @param endline: if an end of line character is added to a text file
   * @exception: IOException
   * @see org.apache.hadoop.fs.FileSystem.globStatus
   */
  void copyMergeToLocal(String srcf, Path dst, boolean endline) throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path [] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath),
                                       srcPath);
    for(int i=0; i<srcs.length; i++) {
      if (endline) {
        FileUtil.copyMerge(srcFs, srcs[i],
                           FileSystem.getLocal(getConf()), dst, false, getConf(), "\n");
      } else {
        FileUtil.copyMerge(srcFs, srcs[i],
                           FileSystem.getLocal(getConf()), dst, false, getConf(), null);
      }
    }
  }

  /**
   * Obtain the indicated file and copy to the local name.
   * srcf is removed.
   */
  void moveToLocal(String srcf, Path dst) throws IOException {
    System.err.println("Option '-moveToLocal' is not implemented yet.");
  }

  /**
   * Fetch all files that match the file pattern <i>srcf</i> and display
   * their content on stdout.
   * @param srcf: a file pattern specifying source files
   * @exception: IOException
   * @see org.apache.hadoop.fs.FileSystem.globStatus
   */
  void cat(String src, boolean verifyChecksum, final boolean genCrc,
      final long offset) throws IOException {
    //cat behavior in Linux
    //  [~/1207]$ ls ?.txt
    //  x.txt  z.txt
    //  [~/1207]$ cat x.txt y.txt z.txt
    //  xxx
    //  cat: y.txt: No such file or directory
    //  zzz

    Path srcPattern = new Path(src);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        if (srcFs.getFileStatus(p).isDir()) {
          throw new IOException("Source must be a file.");
        }
        FSDataInputStream is = srcFs.open(p);
        long newOffset = offset;
        if (newOffset < 0) {
          if (is.isUnderConstruction()) {
            throw new IOException("Couldn't copy under-construction file with " +
                "negative offset " + newOffset);
          }
          newOffset += srcFs.getFileStatus(p).getLen();
        }
        is.seek(newOffset);
        if (genCrc) {
          printToStdoutAndCRCToStderr(is, p);
        } else {
          printToStdout(is);
        }
      }
    }.globAndProcess(srcPattern, getSrcFileSystem(srcPattern, verifyChecksum));
  }

  private class TextRecordInputStream extends InputStream {
    SequenceFile.Reader r;
    WritableComparable key;
    Writable val;

    DataInputBuffer inbuf;
    DataOutputBuffer outbuf;

    public TextRecordInputStream(FileSystem fs, FileStatus f) throws IOException {
      r = new SequenceFile.Reader(fs, f.getPath(), getConf());
      key = ReflectionUtils.newInstance(r.getKeyClass().asSubclass(WritableComparable.class),
                                        getConf());
      val = ReflectionUtils.newInstance(r.getValueClass().asSubclass(Writable.class),
                                        getConf());
      inbuf = new DataInputBuffer();
      outbuf = new DataOutputBuffer();
    }

    public int read() throws IOException {
      int ret;
      if (null == inbuf || -1 == (ret = inbuf.read())) {
        if (!r.next(key, val)) {
          return -1;
        }
        byte[] tmp = key.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\t');
        tmp = val.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\n');
        inbuf.reset(outbuf.getData(), outbuf.getLength());
        outbuf.reset();
        ret = inbuf.read();
      }
      return ret;
    }
  }

  private InputStream forMagic(Path p, FileSystem srcFs) throws IOException {
    FSDataInputStream i = srcFs.open(p);
    switch(i.readShort()) {
      case 0x1f8b: // RFC 1952
        i.seek(0);
        return new GZIPInputStream(i);
      case 0x5345: // 'S' 'E'
        if (i.readByte() == 'Q') {
          i.close();
          return new TextRecordInputStream(srcFs, srcFs.getFileStatus(p));
        }
        break;
    }
    i.seek(0);
    return i;
  }

  void text(String srcf) throws IOException {
    Path srcPattern = new Path(srcf);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        if (srcFs.isDirectory(p)) {
          throw new IOException("Source must be a file.");
        }
        printToStdout(forMagic(p, srcFs));
      }
    }.globAndProcess(srcPattern, srcPattern.getFileSystem(getConf()));
  }

  private InputStream decompress(Path p, FileSystem srcFs) throws IOException {
    CompressionCodecFactory factory = new CompressionCodecFactory(getConf());
    CompressionCodec codec = factory.getCodec(p);
    InputStream in = srcFs.open(p);
    if (codec == null) {
      throw new IOException("Cannot find codec for " + p);
    }
    return codec.createInputStream(in);
  }

  void decompress(String srcf) throws IOException {
    Path srcPattern = new Path(srcf);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        if (srcFs.isDirectory(p)) {
          throw new IOException("Source must be a file.");
        }
        printToStdout(decompress(p, srcFs));
      }
    }.globAndProcess(srcPattern, srcPattern.getFileSystem(getConf()));
  }
  
  void getFileCrc(String src) throws IOException {
    Path f = new Path(src);
    FileSystem srcFs = f.getFileSystem(getConf());
    System.out.println(srcFs.getFileCrc(f));
  }


  /**
   * Parse the incoming command string
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException
   */
  private void setReplication(String[] cmd, int pos) throws IOException {
    final int minArgs = 2;  // We need the replication and at least one path.
    CommandFormat c =
      new CommandFormat("setrep", minArgs, SETREP_MAX_PATHS, "R", "w");
    short rep = 0;
    List<String> dsts = null;
    try {
      List<String> parameters = c.parse(cmd, pos);
      rep = Short.parseShort(parameters.get(0));
      dsts = parameters.subList(1, parameters.size());
    } catch (NumberFormatException nfe) {
      System.err.println("Illegal replication, a positive integer expected");
      throw nfe;
    }
    catch(IllegalArgumentException iae) {
      System.err.println("Usage: java FsShell " + SETREP_SHORT_USAGE);
      throw iae;
    }

    if (rep < 1) {
      System.err.println("Cannot set replication to: " + rep);
      throw new IllegalArgumentException("replication must be >= 1");
    }

    List<Path> waitList = c.getOpt("w")? new ArrayList<Path>(): null;
    for (String dst: dsts) {
      setReplication(rep, dst, c.getOpt("R"), waitList);
    }

    if (waitList != null) {
      waitForReplication(waitList, rep);
    }
  }

  /**
   * Wait for all files in waitList to have replication number equal to rep.
   * @param waitList The files are waited for.
   * @param rep The new replication number.
   * @throws IOException IOException
   */
  void waitForReplication(List<Path> waitList, int rep) throws IOException {
    for(Path f : waitList) {
      System.out.print("Waiting for " + f + " ...");
      System.out.flush();

      boolean printWarning = false;
      FileSystem fs = f.getFileSystem(getConf());
      FileStatus status = fs.getFileStatus(f);
      long len = status.getLen();

      for(boolean done = false; !done; ) {
        BlockLocation[] locations = fs.getFileBlockLocations(status, 0, len);
        int i = 0;
        for(; i < locations.length &&
          locations[i].getHosts().length == rep; i++)
          if (!printWarning && locations[i].getHosts().length > rep) {
            System.out.println("\nWARNING: the waiting time may be long for "
                + "DECREASING the number of replication.");
            printWarning = true;
          }
        done = i == locations.length;

        if (!done) {
          System.out.print(".");
          System.out.flush();
          try {Thread.sleep(10000);} catch (InterruptedException e) {}
        }
      }

      System.out.println(" done");
    }
  }

  /**
   * Set the replication for files that match file pattern <i>srcf</i>
   * if it's a directory and recursive is true,
   * set replication for all the subdirs and those files too.
   * @param newRep new replication factor
   * @param srcf a file pattern specifying source files
   * @param recursive if need to set replication factor for files in subdirs
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void setReplication(short newRep, String srcf, boolean recursive,
                      List<Path> waitingList)
    throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath),
                                      srcPath);
    for(int i=0; i<srcs.length; i++) {
      setReplication(newRep, srcFs, srcs[i], recursive, waitingList);
    }
  }

  private void setReplication(short newRep, FileSystem srcFs,
                              Path src, boolean recursive,
                              List<Path> waitingList)
    throws IOException {
    if (!srcFs.getFileStatus(src).isDir()) {
      setFileReplication(src, srcFs, newRep, waitingList);
      return;
    }
    FileStatus items[] = srcFs.listStatus(src);
    if (items == null) {
      throw new IOException("Could not get listing for " + src);
    } else {

      for (int i = 0; i < items.length; i++) {
        if (!items[i].isDir()) {
          setFileReplication(items[i].getPath(), srcFs, newRep, waitingList);
        } else if (recursive) {
          setReplication(newRep, srcFs, items[i].getPath(), recursive,
                         waitingList);
        }
      }
    }
  }

  /**
   * Actually set the replication for this file
   * If it fails either throw IOException or print an error msg
   * @param file: a file/directory
   * @param newRep: new replication factor
   * @throws IOException
   */
  private void setFileReplication(Path file, FileSystem srcFs, short newRep, List<Path> waitList)
    throws IOException {
    if (srcFs.setReplication(file, newRep)) {
      if (waitList != null) {
        waitList.add(file);
      }
      System.out.println("Replication " + newRep + " set: " + file);
    } else {
      System.err.println("Could not set replication for: " + file);
    }
  }


  /**
   * Get a listing of all files in that match the file pattern <i>srcf</i>.
   * @param srcf a file pattern specifying source files
   * @param flags LS options.
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  private int ls(String srcf, EnumSet<LsOption> flags) throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(this.getConf());
    FileStatus[] srcs = srcFs.globStatus(srcPath);
    if (srcs==null || srcs.length==0) {
      throw new FileNotFoundException("Cannot access " + srcf +
          ": No such file or directory.");
    }

    boolean printHeader = (srcs.length == 1) ? true: false;
    int numOfErrors = 0;
    for(int i=0; i<srcs.length; i++) {
      numOfErrors += ls(srcs[i], srcFs, flags, printHeader);
    }
    return numOfErrors == 0 ? 0 : -1;
  }

  /* list all files under the directory <i>src</i>
   * ideally we should provide "-l" option, that lists like "ls -l".
   */
  private int ls(FileStatus src, FileSystem srcFs, EnumSet<LsOption> flags,
      boolean printHeader) throws IOException {
    final boolean recursive = flags.contains(LsOption.Recursive);
    final boolean withBlockSize = flags.contains(LsOption.WithBlockSize);
    final boolean directoryOnly = flags.contains(LsOption.DirectoryOnly);
    String cmd = "ls";
    if (recursive) {
      cmd = "lsr";
    }
    if (directoryOnly) {
      cmd = "ls -d";
    }
    final FileStatus[] items = shellListStatus(cmd, srcFs, src, directoryOnly);
    if (items == null) {
      return 1;
    } else {
      int numOfErrors = 0;
      if (!recursive && printHeader) {
        if (items.length != 0) {
          System.out.println("Found " + items.length + " items");
        }
      }

      int maxReplication = 3, maxLen = 10, maxOwner = 0,maxGroup = 0;

      for(int i = 0; i < items.length; i++) {
        FileStatus stat = items[i];
        int replication = String.valueOf(stat.getReplication()).length();
        int len = String.valueOf(stat.getLen()).length();
        int owner = String.valueOf(stat.getOwner()).length();
        int group = String.valueOf(stat.getGroup()).length();

        if (replication > maxReplication) maxReplication = replication;
        if (len > maxLen) maxLen = len;
        if (owner > maxOwner)  maxOwner = owner;
        if (group > maxGroup)  maxGroup = group;
      }

      for (int i = 0; i < items.length; i++) {
        FileStatus stat = items[i];
        Path cur = stat.getPath();
        String mdate = dateForm.format(new Date(stat.getModificationTime()));

        System.out.print((stat.isDir() ? "d" : "-") +
          stat.getPermission() + " ");
        System.out.printf("%"+ maxReplication +
          "s ", (!stat.isDir() ? stat.getReplication() : "-"));
        if (maxOwner > 0)
          System.out.printf("%-"+ maxOwner + "s ", stat.getOwner());
        if (maxGroup > 0)
          System.out.printf("%-"+ maxGroup + "s ", stat.getGroup());
        System.out.printf("%"+ maxLen + "d ", stat.getLen());
        if (withBlockSize)
          System.out.printf("%"+ maxLen + "d ", stat.getBlockSize());
        System.out.print(mdate + " ");
        System.out.println(cur.toUri().getPath());
        if (recursive && stat.isDir()) {
          numOfErrors += ls(stat,srcFs, flags, printHeader);
        }
      }
      return numOfErrors;
    }
  }

  /**
   * Show the size of all files that match the file pattern <i>src</i>
   * @param src a file pattern specifying source files
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void du(String src) throws IOException {
    Path srcPath = new Path(src);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path[] pathItems = FileUtil.stat2Paths(srcFs.globStatus(srcPath),
                                           srcPath);
    FileStatus items[] = srcFs.listStatus(pathItems);
    if ((items == null) || ((items.length == 0) &&
        (!srcFs.exists(srcPath)))){
      throw new FileNotFoundException("Cannot access " + src
            + ": No such file or directory.");
    } else {
      System.out.println("Found " + items.length + " items");
      int maxLength = 10;

      long length[] = new long[items.length];
      for (int i = 0; i < items.length; i++) {
        length[i] = items[i].isDir() ?
          srcFs.getContentSummary(items[i].getPath()).getLength() :
          items[i].getLen();
        int len = String.valueOf(length[i]).length();
        if (len > maxLength) maxLength = len;
      }
      for(int i = 0; i < items.length; i++) {
        System.out.printf("%-"+ (maxLength + BORDER) +"d", length[i]);
        System.out.println(items[i].getPath());
      }
    }
  }

  /**
   * Show the summary disk usage of each dir/file
   * that matches the file pattern <i>src</i>
   * @param src a file pattern specifying source files
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void dus(String src) throws IOException {
    Path srcPath = new Path(src);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    FileStatus status[] = srcFs.globStatus(new Path(src));
    if (status==null || status.length==0) {
      throw new FileNotFoundException("Cannot access " + src +
          ": No such file or directory.");
    }
    for(int i=0; i<status.length; i++) {
      long totalSize = srcFs.getContentSummary(status[i].getPath()).getLength();
      String pathStr = status[i].getPath().toString();
      System.out.println(("".equals(pathStr)?".":pathStr) + "\t" + totalSize);
    }
  }

  /*
   * Remove the given dir pattern if it is empty
   */
  void rmdir(String src, final boolean ignoreFailOnEmpty,
              final boolean skipTrash) throws IOException {
    Path fPattern = new Path(src);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        DeleteUtils.delete(getConf(), p, srcFs, false, true, ignoreFailOnEmpty,
            false);
      }
    }.globAndProcess(fPattern, fPattern.getFileSystem(getConf()));
  }

  /**
   * Create the given dir
   */
  void mkdir(String src) throws IOException {
    Path f = new Path(src);
    FileSystem srcFs = f.getFileSystem(getConf());
    FileStatus fstatus = null;
    try {
      fstatus = srcFs.getFileStatus(f);
      if (fstatus.isDir()) {
        throw new IOException("cannot create directory "
            + src + ": File exists");
      }
      else {
        throw new IOException(src + " exists but " +
            "is not a directory");
      }
    } catch(FileNotFoundException e) {
        if (!srcFs.mkdirs(f)) {
          throw new IOException("failed to create " + src);
        }
    }
  }

  /**
   * (Re)create zero-length file at the specified path.
   * This will be replaced by a more UNIX-like touch when files may be
   * modified.
   */
  void touchz(String src) throws IOException {
    Path f = new Path(src);
    FileSystem srcFs = f.getFileSystem(getConf());
    FileStatus st;
    if (srcFs.exists(f)) {
      st = srcFs.getFileStatus(f);
      if (st.isDir()) {
        // TODO: handle this
        throw new IOException(src + " is a directory");
      } else if (st.getLen() != 0)
        throw new IOException(src + " must be a zero-length file");
    }
    FSDataOutputStream out = srcFs.create(f);
    out.close();
  }

  /**
   * Check file types.
   */
  int test(String argv[], int i) throws IOException {
    if (!argv[i].startsWith("-") || argv[i].length() > 2)
      throw new IOException("Not a flag: " + argv[i]);
    char flag = argv[i].toCharArray()[1];
    Path f = new Path(argv[++i]);
    FileSystem srcFs = f.getFileSystem(getConf());
    switch(flag) {
      case 'e':
        return srcFs.exists(f) ? 0 : 1;
      case 'z':
        return srcFs.getFileStatus(f).getLen() == 0 ? 0 : 1;
      case 'd':
        return srcFs.getFileStatus(f).isDir() ? 0 : 1;
      default:
        throw new IOException("Unknown flag: " + flag);
    }
  }

  /**
   * Print statistics about path in specified format.
   * Format sequences:
   *   %b: Size of file in blocks
   *   %n: Filename
   *   %o: Block size
   *   %r: replication
   *   %y: UTC date as &quot;yyyy-MM-dd HH:mm:ss&quot;
   *   %Y: Milliseconds since January 1, 1970 UTC
   */
  void stat(char[] fmt, String src) throws IOException {
    Path srcPath = new Path(src);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    FileStatus glob[] = srcFs.globStatus(srcPath);
    if (null == glob)
      throw new IOException("cannot stat `" + src + "': No such file or directory");
    for (FileStatus f : glob) {
      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < fmt.length; ++i) {
        if (fmt[i] != '%') {
          buf.append(fmt[i]);
        } else {
          if (i + 1 == fmt.length) break;
          switch(fmt[++i]) {
            case 'b':
              buf.append(f.getLen());
              break;
            case 'F':
              buf.append(f.isDir() ? "directory" : "regular file");
              break;
            case 'n':
              buf.append(f.getPath().getName());
              break;
            case 'o':
              buf.append(f.getBlockSize());
              break;
            case 'r':
              buf.append(f.getReplication());
              break;
            case 'y':
              buf.append(modifFmt.format(new Date(f.getModificationTime())));
              break;
            case 'Y':
              buf.append(f.getModificationTime());
              break;
            default:
              buf.append(fmt[i]);
              break;
          }
        }
      }
      System.out.println(buf.toString());
    }
  }

  /**
   * Move files that match the file pattern <i>srcf</i>
   * to a destination file.
   * When moving mutiple files, the destination must be a directory.
   * Otherwise, IOException is thrown.
   * @param srcf a file pattern specifying source files
   * @param dstf a destination local file/directory
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void rename(String srcf, String dstf) throws IOException {
    Path srcPath = new Path(srcf);
    Path dstPath = new Path(dstf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    URI srcURI = srcFs.getUri();
    URI dstURI = dstFs.getUri();
    if (srcURI.compareTo(dstURI) != 0) {
      throw new IOException("src and destination filesystems do not match.");
    }
    Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
    Path dst = new Path(dstf);
    if (srcs.length > 1 && !srcFs.isDirectory(dst)) {
      throw new IOException("When moving multiple files, "
                            + "destination should be a directory.");
    }
    for(int i=0; i<srcs.length; i++) {
      if (!srcFs.rename(srcs[i], dst)) {
        FileStatus srcFstatus = null;
        FileStatus dstFstatus = null;
        try {
          srcFstatus = srcFs.getFileStatus(srcs[i]);
        } catch(FileNotFoundException e) {
          throw new FileNotFoundException(srcs[i] +
          ": No such file or directory");
        }
        try {
          dstFstatus = dstFs.getFileStatus(dst);
        } catch(IOException e) {
        }
        if((srcFstatus!= null) && (dstFstatus!= null)) {
          if (srcFstatus.isDir()  && !dstFstatus.isDir()) {
            throw new IOException("cannot overwrite non directory "
                + dst + " with directory " + srcs[i]);
          }
        }
        throw new IOException("Failed to rename " + srcs[i] + " to " + dst);
      }
    }
  }

  /**
   * Move/rename file(s) to a destination file. Multiple source
   * files can be specified. The destination is the last element of
   * the argvp[] array.
   * If multiple source files are specified, then the destination
   * must be a directory. Otherwise, IOException is thrown.
   * @exception: IOException
   */
  private int rename(String argv[], Configuration conf) throws IOException {
    int i = 0;
    int exitCode = 0;
    String cmd = argv[i++];
    String dest = argv[argv.length-1];
    //
    // If the user has specified multiple source files, then
    // the destination has to be a directory
    //
    if (argv.length > 3) {
      Path dst = new Path(dest);
      FileSystem dstFs = dst.getFileSystem(getConf());
      if (!dstFs.isDirectory(dst)) {
        throw new IOException("When moving multiple files, "
                              + "destination " + dest + " should be a directory.");
      }
    }
    //
    // for each source file, issue the rename
    //
    for (; i < argv.length - 1; i++) {
      try {
        //
        // issue the rename to the fs
        //
        rename(argv[i], dest);
      } catch (RemoteException e) {
        //
        // This is a error returned by hadoop server. Print
        // out the first line of the error mesage.
        //
        exitCode = -1;
        try {
          String[] content;
          content = e.getLocalizedMessage().split("\n");
          System.err.println(cmd.substring(1) + ": " + content[0]);
        } catch (Exception ex) {
          System.err.println(cmd.substring(1) + ": " +
                             ex.getLocalizedMessage());
        }
      } catch (IOException e) {
        //
        // IO exception encountered locally.
        //
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": " +
                           e.getLocalizedMessage());
      }
    }
    return exitCode;
  }

  /**
   * Copy files that match the file pattern <i>srcf</i>
   * to a destination file.
   * When copying mutiple files, the destination must be a directory.
   * Otherwise, IOException is thrown.
   * @param srcf a file pattern specifying source files
   * @param dstf a destination local file/directory
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void copy(String srcf, String dstf, Configuration conf) throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    Path [] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
    if (srcs.length > 1 && !dstFs.isDirectory(dstPath)) {
      throw new IOException("When copying multiple files, "
                            + "destination should be a directory.");
    }
    for(int i=0; i<srcs.length; i++) {
      FileUtil.copy(srcFs, srcs[i], dstFs, dstPath, false, conf);
    }
  }

  private int hardlink(String argv[]) throws IOException {
    if (argv.length != 3) {
      throw new IllegalArgumentException(
          "Must specify exactly two files to hardlink");
    }
    if (argv[1] == null || argv[2] == null) {
      throw new IllegalArgumentException("One of the arguments is null");
    }
    Path src = new Path(argv[1]);
    Path dst = new Path(argv[2]);
    FileSystem srcFs = src.getFileSystem(getConf());
    FileSystem dstFs = dst.getFileSystem(getConf());
    if (!srcFs.getUri().equals(dstFs.getUri())) {
      throw new IllegalArgumentException("Source and Destination files are" +
        " on different filesystems");
    }
    return (srcFs.hardLink(src, dst)) ? 0 : -1;
  }

  /**
   * Copy file(s) to a destination file. Multiple source
   * files can be specified. The destination is the last element of
   * the argvp[] array.
   * If multiple source files are specified, then the destination
   * must be a directory. Otherwise, IOException is thrown.
   * @exception: IOException
   */
  private int copy(String argv[], Configuration conf) throws IOException {
    int i = 0;
    int exitCode = 0;
    String cmd = argv[i++];
    String dest = argv[argv.length-1];
    //
    // If the user has specified multiple source files, then
    // the destination has to be a directory
    //
    if (argv.length > 3) {
      Path dst = new Path(dest);
      if (!dst.getFileSystem(conf).isDirectory(dst)) {
        throw new IOException("When copying multiple files, " 
                              + "destination " + dest + " should be a directory.");
      }
    }
    //
    // for each source file, issue the copy
    //
    for (; i < argv.length - 1; i++) {
      try {
        //
        // issue the copy to the fs
        //
        copy(argv[i], dest, conf);
      } catch (RemoteException e) {
        //
        // This is a error returned by hadoop server. Print
        // out the first line of the error mesage.
        //
        exitCode = -1;
        try {
          String[] content;
          content = e.getLocalizedMessage().split("\n");
          System.err.println(cmd.substring(1) + ": " +
                             content[0]);
        } catch (Exception ex) {
          System.err.println(cmd.substring(1) + ": " +
                             ex.getLocalizedMessage());
        }
      } catch (IOException e) {
        //
        // IO exception encountered locally.
        //
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": " +
                           e.getLocalizedMessage());
      }
    }
    return exitCode;
  }

  /**
   * Compress a file.
   */
  private int compress(String argv[], Configuration conf) throws IOException {
    int i = 0;
    String cmd = argv[i++];
    String srcf = argv[i++];
    String dstf = argv[i++];

    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());

    // Create codec
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(dstPath);
    if (codec == null) {
      System.err.println(cmd.substring(1) + ": cannot find compression codec for "
          + dstf);
      return 1;
    }

    // open input stream
    InputStream in = srcFs.open(srcPath);

    // Create compression stream
    OutputStream out = dstFs.create(dstPath);
    out = codec.createOutputStream(out);

    IOUtils.copyBytes(in, out, conf, true);

    return 0;
  }

  /**
   * Delete all files that match the file pattern <i>srcf</i>.
   * @param srcf a file pattern specifying source files
   * @param recursive if need to delete subdirs
   * @param skipTrash Should we skip the trash, if it's enabled?
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void delete(String srcf, final boolean recursive, final boolean skipTrash)
                                                            throws IOException {
    //rm behavior in Linux
    //  [~/1207]$ ls ?.txt
    //  x.txt  z.txt
    //  [~/1207]$ rm x.txt y.txt z.txt
    //  rm: cannot remove `y.txt': No such file or directory

    Path srcPattern = new Path(srcf);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        DeleteUtils.delete(getConf(), p, srcFs, recursive, skipTrash, false,
            true);
      }
    }.globAndProcess(srcPattern, srcPattern.getFileSystem(getConf()));
  }


  /** Undelete a file
   * @param src path to the file in the trash
   * @param srcFs FileSystem instance
   * @param userName name of the user whose trash will be searched, or
   * null for current user
   * @return true if the file was deleted
   * @throws IOException
   */
  public boolean undelete(String src, FileSystem srcFs, String userName)
    throws IOException {
    boolean result = srcFs.undelete(new Path(src), userName);
    if (result)
      System.err.println("Moved from trash: " + src);
    return result;
  }

  private void expunge() throws IOException {
    getTrash().expunge();
    getTrash().checkpoint();
  }

  /**
   * Returns the Trash object associated with this shell.
   */
  public Path getCurrentTrashDir() throws IOException{
    return getTrash().getCurrentTrashDir();
  }

  /**
   * Parse the incoming command string
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException
   */
  private void tail(String[] cmd, int pos) throws IOException {
    CommandFormat c = new CommandFormat("tail", 1, 1, "f");
    String src = null;
    Path path = null;

    try {
      List<String> parameters = c.parse(cmd, pos);
      src = parameters.get(0);
    } catch(IllegalArgumentException iae) {
      System.err.println("Usage: java FsShell " + TAIL_USAGE);
      throw iae;
    }
    boolean foption = c.getOpt("f") ? true: false;
    path = new Path(src);
    FileSystem srcFs = path.getFileSystem(getConf());
    if (srcFs.isDirectory(path)) {
      throw new IOException("Source must be a file.");
    }

    long fileSize = srcFs.getFileStatus(path).getLen();
    long offset = (fileSize > 1024) ? fileSize - 1024: 0;

    while (true) {
      FSDataInputStream in = srcFs.open(path);
      in.seek(offset);
      IOUtils.copyBytes(in, System.out, 1024, false);
      offset = in.getPos();
      in.close();
      if (!foption) {
        break;
      }
      fileSize = srcFs.getFileStatus(path).getLen();
      offset = (fileSize > offset) ? offset: fileSize;
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  /**
   * Parse the incoming command string
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException
   */
  private void head(String[] cmd, int pos) throws IOException {
    CommandFormat c = new CommandFormat("head", 1, 1);
    String src = null;
    Path path = null;

    try {
      List<String> parameters = c.parse(cmd, pos);
      src = parameters.get(0);
    } catch(IllegalArgumentException iae) {
      System.err.println("Usage: java FsShell " + HEAD_USAGE);
      throw iae;
    }

    path = new Path(src);
    FileSystem srcFs = path.getFileSystem(getConf());
    if (srcFs.isDirectory(path)) {
      throw new IOException("Source must be a file.");
    }

    long fileSize = srcFs.getFileStatus(path).getLen();
    int len = (fileSize > 1024) ? 1024 : (int) fileSize;

    FSDataInputStream in = srcFs.open(path);
    byte buf[] = new byte[len];
    IOUtils.readFully(in, buf, 0, len);
    System.out.write(buf);
    System.out.write('\n');
    in.close();
  }

  /**
   * This class runs a command on a given FileStatus. This can be used for
   * running various commands like chmod, chown etc.
   */
  static abstract class CmdHandler {

    protected int errorCode = 0;
    protected boolean okToContinue = true;
    protected String cmdName;

    int getErrorCode() { return errorCode; }
    boolean okToContinue() { return okToContinue; }
    String getName() { return cmdName; }

    protected CmdHandler(String cmdName) {
      this.cmdName = cmdName;
    }

    public abstract void run(FileStatus file, FileSystem fs) throws IOException;
  }

  /** helper returns listStatus() */
  private static FileStatus[] shellListStatus(String cmd,
                                                   FileSystem srcFs,
                                                   FileStatus src,
                                                   boolean directoryOnly) {
    if (!src.isDir() || (src.isDir() && directoryOnly)) {
      FileStatus[] files = { src };
      return files;
    }
    Path path = src.getPath();
    try {
      FileStatus[] files = srcFs.listStatus(path);
      if ( files == null ) {
        System.err.println(cmd +
                           ": could not get listing for '" + path + "'");
      }
      return files;
    } catch (IOException e) {
      System.err.println(cmd +
                         ": could not get get listing for '" + path + "' : " +
                         e.getMessage().split("\n")[0]);
    }
    return null;
  }


  /**
   * Runs the command on a given file with the command handler.
   * If recursive is set, command is run recursively.
   */
  private static int runCmdHandler(CmdHandler handler, FileStatus stat,
                                   FileSystem srcFs,
                                   boolean recursive) throws IOException {
    int errors = 0;
    handler.run(stat, srcFs);
    if (recursive && stat.isDir() && handler.okToContinue()) {
      FileStatus[] files = shellListStatus(handler.getName(), srcFs, stat, false);
      if (files == null) {
        return 1;
      }
      for(FileStatus file : files ) {
        errors += runCmdHandler(handler, file, srcFs, recursive);
      }
    }
    return errors;
  }

  ///top level runCmdHandler
  int runCmdHandler(CmdHandler handler, String[] args,
                                   int startIndex, boolean recursive)
                                   throws IOException {
    int errors = 0;

    for (int i=startIndex; i<args.length; i++) {
      Path srcPath = new Path(args[i]);
      FileSystem srcFs = srcPath.getFileSystem(getConf());
      Path[] paths = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
      for(Path path : paths) {
        try {
          FileStatus file = srcFs.getFileStatus(path);
          if (file == null) {
            System.err.println(handler.getName() +
                               ": could not get status for '" + path + "'");
            errors++;
          } else {
            errors += runCmdHandler(handler, file, srcFs, recursive);
          }
        } catch (IOException e) {
          String msg = (e.getMessage() != null ? e.getLocalizedMessage() :
            (e.getCause().getMessage() != null ?
                e.getCause().getLocalizedMessage() : "null"));
          System.err.println(handler.getName() + ": could not get status for '"
                                        + path + "': " + msg.split("\n")[0]);
        }
      }
    }

    return (errors > 0 || handler.getErrorCode() != 0) ? 1 : 0;
  }

  /**
   * Return an abbreviated English-language desc of the byte length
   * @deprecated Consider using {@link org.apache.hadoop.util.StringUtils#byteDesc} instead.
   */
  @Deprecated
  public static String byteDesc(long len) {
    return StringUtils.byteDesc(len);
  }

  /**
   * @deprecated Consider using {@link org.apache.hadoop.util.StringUtils#limitDecimalTo2} instead.
   */
  @Deprecated
  public static synchronized String limitDecimalTo2(double d) {
    return StringUtils.limitDecimalTo2(d);
  }

  private void printHelp(String cmd) {
    String summary = "hadoop fs is the command to execute fs commands. " +
      "The full syntax is: \n\n" +
      "hadoop fs [-fs <local | file system URI>] [-conf <configuration file>]\n\t" +
      "[-D <property=value>] [-ls <path>] [-lsr <path>] [-lsrx <path>] [-du <path>]\n\t" +
      "[-dus <path>] [-mv <src> <dst>] [-cp <src> <dst>] [-rm [-skipTrash] <src>]\n\t" +
      "[-rmr [-skipTrash] <src>] [-put <localsrc> ... <dst>]\n\t" +
      "[-rmdir [-ignore-fail-on-non-empty] <src>]\n\t" + 
      "[-copyFromLocal <localsrc> ... <dst>]\n\t" +
      "[-moveFromLocal <localsrc> ... <dst>] [" +
      GET_SHORT_USAGE + "\n\t" +
      "[-getmerge <src> <localdst> [addnl]] [-cat <src>]\n\t" +
      "[" + COPYTOLOCAL_SHORT_USAGE + "] [-moveToLocal <src> <localdst>]\n\t" +
      "[-mkdir <path>] [-report] [" + SETREP_SHORT_USAGE + "]\n\t" +
      "[-touchz <path>] [" + FsShellTouch.TOUCH_USAGE + "]\n\t" +
      "[-test -[ezd] <path>] [-stat [format] <path>]\n\t" +
      "[-head <path>] [-tail [-f] <path>] [-text <path>]\n\t" +
      "[-decompress <path>] [-compress <src> <tgt>]\n\t" +
      "[-undelete [-u <username>] <path>]\n\t" +
      "[-filecrc <path>]\n\t" +
      "[" + FsShellPermissions.CHMOD_USAGE + "]\n\t" +
      "[" + FsShellPermissions.CHOWN_USAGE + "]\n\t" +
      "[" + FsShellPermissions.CHGRP_USAGE + "]\n\t" +
      "[" + Count.USAGE + "]\n\t" +
      "[-help [cmd]]\n";

    String conf ="-conf <configuration file>:  Specify an application configuration file.";

    String D = "-D <property=value>:  Use value for given property.";

    String fs = "-fs [local | <file system URI>]: \tSpecify the file system to use.\n" +
      "\t\tIf not specified, the current configuration is used, \n" +
      "\t\ttaken from the following, in increasing precedence: \n" +
      "\t\t\tcore-default.xml inside the hadoop jar file \n" +
      "\t\t\tcore-site.xml in $HADOOP_CONF_DIR \n" +
      "\t\t'local' means use the local file system as your DFS. \n" +
      "\t\t<file system URI> specifies a particular file system to \n" +
      "\t\tcontact. This argument is optional but if used must appear\n" +
      "\t\tappear first on the command line.  Exactly one additional\n" +
      "\t\targument must be specified. \n";


    String ls = "-ls [-d] <path>: \tList the contents that match the specified file pattern. If\n" +
      "\t\tpath is not specified, the contents of /user/<currentUser>\n" +
      "\t\twill be listed. Directory entries are of the form \n" +
      "\t\t\tdirName (full path) <dir> \n" +
      "\t\tand file entries are of the form \n" +
      "\t\t\tfileName(full path) <r n> size \n" +
      "\t\twhere n is the number of replicas specified for the file \n" +
      "\t\tand size is the size of the file, in bytes.\n" +
      "\t\t-d \tList directory entries instead of contents.\n";

    String lsr = "-lsr <path>: \tRecursively list the contents that match the specified\n" +
      "\t\tfile pattern.  Behaves very similarly to hadoop fs -ls,\n" +
      "\t\texcept that the data is shown for all the entries in the\n" +
      "\t\tsubtree.\n";

    String lsrx = "-lsrx <path>: \tRecursively list the contents that match the specified\n" +
      "\t\tfile pattern.  Behaves very similarly to hadoop fs -lsr,\n" +
      "\t\texcept that block size of files is also shown.\n";

    String du = "-du <path>: \tShow the amount of space, in bytes, used by the files that \n" +
      "\t\tmatch the specified file pattern.  Equivalent to the unix\n" +
      "\t\tcommand \"du -sb <path>/*\" in case of a directory, \n" +
      "\t\tand to \"du -b <path>\" in case of a file.\n" +
      "\t\tThe output is in the form \n" +
      "\t\t\tname(full path) size (in bytes)\n";

    String dus = "-dus <path>: \tShow the amount of space, in bytes, used by the files that \n" +
      "\t\tmatch the specified file pattern.  Equivalent to the unix\n" +
      "\t\tcommand \"du -sb\"  The output is in the form \n" +
      "\t\t\tname(full path) size (in bytes)\n";

    String mv = "-mv <src> <dst>:   Move files that match the specified file pattern <src>\n" +
      "\t\tto a destination <dst>.  When moving multiple files, the \n" +
      "\t\tdestination must be a directory. \n";

    String cp = "-cp <src> <dst>:   Copy files that match the file pattern <src> to a \n" +
      "\t\tdestination.  When copying multiple files, the destination\n" +
      "\t\tmust be a directory. \n";

    String hardlink = "-hardlink <src> <dst> : Hardlink a source file to a "
        + "destination file. The destination file must not exist";

    String showlinks = "-showlinks <srcs...> : Displays the files hardlinked "
        + "to each of the files specified on the command line";

    String rm = "-rm [-skipTrash] <src>: \tDelete all files that match the specified file pattern.\n" +
      "\t\tEquivalent to the Unix command \"rm <src>\"\n" +
      "\t\t-skipTrash option bypasses trash, if enabled, and immediately\n" +
      "deletes <src>";

    String rmr = "-rmr [-skipTrash] <src>: \tRemove all directories which match the specified file \n" +
      "\t\tpattern. Equivalent to the Unix command \"rm -rf <src>\"\n" +
      "\t\t-skipTrash option bypasses trash, if enabled, and immediately\n" +
      "deletes <src>";

    String rmdir = "-rmdir [-ignore-fail-on-non-empty] <src>: \tRemoves the directory entry \n" +
      "\t\tspecified by the directory argument, provided it is empty. \n";

    String put = "-put <localsrc> ... <dst>: \tCopy files " +
    "from the local file system \n\t\tinto fs. \n" +
    "\t\twhen -rate is given, the upload bandwidth will not go beyond" +
    " the sepcified <bandwidth> MB/s\n";

    String copyFromLocal = "-copyFromLocal <localsrc> ... <dst>:" +
    " Identical to the -put command.\n";

    String moveFromLocal = "-moveFromLocal <localsrc> ... <dst>:" +
    " Same as -put, except that the source is\n\t\tdeleted after it's copied.\n";

    String get = GET_SHORT_USAGE
      + ":  Copy files that match the file pattern <src> \n" +
      "\t\tto the local name.  <src> is kept.  When copying mutiple, \n" +
      "\t\tfiles, the destination must be a directory. " +
      "\t\twhen -rate is given, the download bandwidth will not go beyond" +
      " the sepcified <bandwidth> MB/s\n" + 
      "\t\twhen -gencrc is given, it will print out CRC32 checksum of the file in stderr\n" + 
      "\t\twhen -start offset is given, it will first seek to given offset\n" + 
      "\t\tand start reading from there; offset could be negative, for example, -1 means\n" +
      "\t\tstarting from fileLen - 1\n";

    String getmerge = "-getmerge <src> <localdst>:  Get all the files in the directories that \n" +
      "\t\tmatch the source file pattern and merge and sort them to only\n" +
      "\t\tone file on local fs. <src> is kept.\n";

    String cat = "-cat <src>: \tFetch all files that match the file pattern <src> \n" +
      "\t\tand display their content on stdout.\n";


    String text = "-text <src>: \tTakes a source file and outputs the file in text format.\n" +
    "\t\tThe allowed formats are zip and TextRecordInputStream.\n";

    String decompress = "-decompress <src>: \tTakes a source file and decompress the file based on file name extension.\n" +
    "\t\tThe allowed formats are any registered compressed file formats.\n";

    String compress = "-compress <src> <tgt>: \tTakes a source file and compress the file to target.\n" +
    "\t\tThe compression codec is determined by the target file name extension.";

    String filecrc = "-filecrc <src>: \treturn CRC32 value of the file.\n";
    
    String undelete = "-undelete [-u <username>] <src>: \tUndelete a file from the trash folder\n" +
      "\t\tIf -u <username> is supplied, attempt to undelete from given users\n" +
      "\t\ttrash, otherwise use current users trash as default\n";

    String copyToLocal = COPYTOLOCAL_SHORT_USAGE
                         + ":  Identical to the -get command.\n";

    String moveToLocal = "-moveToLocal <src> <localdst>:  Not implemented yet \n";

    String mkdir = "-mkdir <path>: \tCreate a directory in specified location. \n";

    String setrep = SETREP_SHORT_USAGE
      + ":  Set the replication level of a file. \n"
      + "\t\tThe -R flag requests a recursive change of replication level \n"
      + "\t\tfor an entire tree.\n";

    String touchz = "-touchz <path>: Write a timestamp in yyyy-MM-dd HH:mm:ss format\n" +
      "\t\tin a file at <path>. An error is returned if the file exists with non-zero length\n";

    String touch = FsShellTouch.TOUCH_USAGE + "\n" +
      "\t\tUpdate the access and modification times of each PATH to the current time.\n" +
      "\t\tIf PATH does not exist, create empty file.\n\n" +
      "\t-a\tChange only access time\n" +
      "\t-c, --no-create\n" +
      "\t\tDo not create any files\n" +
      "\t-d, --date=\"yyyy-MM-dd HH:mm:ss\"\n" +
      "\t\tUse specified date instead of current time\n" +
      "\t-m\tChange only modification time\n" + 
      "\t-u timestamp\n" + 
      "\t\tUse specified timestamp instead of date";

    String test = "-test -[ezd] <path>: If file { exists, has zero length, is a directory\n" +
      "\t\tthen return 0, else return 1.\n";

    String stat = "-stat [format] <path>: Print statistics about the file/directory at <path>\n" +
      "\t\tin the specified format. Format accepts filesize in blocks (%b), filename (%n),\n" +
      "\t\tblock size (%o), replication (%r), modification date (%y, %Y)\n";

    String tail = TAIL_USAGE
      + ":  Show the last 1KB of the file. \n"
      + "\t\tThe -f option shows apended data as the file grows. \n";

    String head = HEAD_USAGE
      + ":  Show the first 1KB of the file. \n";

    String chmod = FsShellPermissions.CHMOD_USAGE + "\n" +
      "\t\tChanges permissions of a file.\n" +
      "\t\tThis works similar to shell's chmod with a few exceptions.\n\n" +
      "\t-R\tmodifies the files recursively. This is the only option\n" +
      "\t\tcurrently supported.\n\n" +
      "\tMODE\tMode is same as mode used for chmod shell command.\n" +
      "\t\tOnly letters recognized are 'rwxX'. E.g. a+r,g-w,+rwx,o=r\n\n" +
      "\tOCTALMODE Mode specifed in 3 digits. Unlike shell command,\n" +
      "\t\tthis requires all three digits.\n" +
      "\t\tE.g. 754 is same as u=rwx,g=rx,o=r\n\n" +
      "\t\tIf none of 'augo' is specified, 'a' is assumed and unlike\n" +
      "\t\tshell command, no umask is applied.\n";

    String chown = FsShellPermissions.CHOWN_USAGE + "\n" +
      "\t\tChanges owner and group of a file.\n" +
      "\t\tThis is similar to shell's chown with a few exceptions.\n\n" +
      "\t-R\tmodifies the files recursively. This is the only option\n" +
      "\t\tcurrently supported.\n\n" +
      "\t\tIf only owner or group is specified then only owner or\n" +
      "\t\tgroup is modified.\n\n" +
      "\t\tThe owner and group names may only cosists of digits, alphabet,\n"+
      "\t\tand any of '-_.@/' i.e. [-_.@/a-zA-Z0-9]. The names are case\n" +
      "\t\tsensitive.\n\n" +
      "\t\tWARNING: Avoid using '.' to separate user name and group though\n" +
      "\t\tLinux allows it. If user names have dots in them and you are\n" +
      "\t\tusing local file system, you might see surprising results since\n" +
      "\t\tshell command 'chown' is used for local files.\n";

    String chgrp = FsShellPermissions.CHGRP_USAGE + "\n" +
      "\t\tThis is equivalent to -chown ... :GROUP ...\n";

    String help = "-help [cmd]: \tDisplays help for given command or all commands if none\n" +
      "\t\tis specified.\n";

    if ("fs".equals(cmd)) {
      System.out.println(fs);
    } else if ("conf".equals(cmd)) {
      System.out.println(conf);
    } else if ("D".equals(cmd)) {
      System.out.println(D);
    } else if ("ls".equals(cmd)) {
      System.out.println(ls);
    } else if ("lsr".equals(cmd)) {
      System.out.println(lsr);
    } else if ("lsrx".equals(cmd)) {
      System.out.println(lsrx);
    } else if ("du".equals(cmd)) {
      System.out.println(du);
    } else if ("dus".equals(cmd)) {
      System.out.println(dus);
    } else if ("rm".equals(cmd)) {
      System.out.println(rm);
    } else if ("hardlink".equals(cmd)) {
      System.out.println(hardlink);
    } else if ("showlinks".equals(cmd)) {
      System.out.println(showlinks);
    } else if ("rmr".equals(cmd)) {
      System.out.println(rmr);
    } else if ("rmdir".equals(cmd)) {
      System.out.println(rmdir);
    } else if ("mkdir".equals(cmd)) {
      System.out.println(mkdir);
    } else if ("mv".equals(cmd)) {
      System.out.println(mv);
    } else if ("cp".equals(cmd)) {
      System.out.println(cp);
    } else if ("put".equals(cmd)) {
      System.out.println(put);
    } else if ("copyFromLocal".equals(cmd)) {
      System.out.println(copyFromLocal);
    } else if ("moveFromLocal".equals(cmd)) {
      System.out.println(moveFromLocal);
    } else if ("get".equals(cmd)) {
      System.out.println(get);
    } else if ("getmerge".equals(cmd)) {
      System.out.println(getmerge);
    } else if ("copyToLocal".equals(cmd)) {
      System.out.println(copyToLocal);
    } else if ("moveToLocal".equals(cmd)) {
      System.out.println(moveToLocal);
    } else if ("cat".equals(cmd)) {
      System.out.println(cat);
    } else if ("get".equals(cmd)) {
      System.out.println(get);
    } else if ("setrep".equals(cmd)) {
      System.out.println(setrep);
    } else if ("touchz".equals(cmd)) {
      System.out.println(touchz);
    } else if ("touch".equals(cmd)) {
      System.out.println(touch);
    } else if ("test".equals(cmd)) {
      System.out.println(test);
    } else if ("text".equals(cmd)) {
      System.out.println(text);
    } else if ("decompress".equals(cmd)) {
      System.out.println(decompress);
    } else if ("compress".equals(cmd)) {
      System.out.println(compress);
    } else if ("stat".equals(cmd)) {
      System.out.println(stat);
    } else if ("tail".equals(cmd)) {
      System.out.println(tail);
    } else if ("head".equals(cmd)) {
      System.out.println(head);
    } else if ("chmod".equals(cmd)) {
      System.out.println(chmod);
    } else if ("chown".equals(cmd)) {
      System.out.println(chown);
    } else if ("chgrp".equals(cmd)) {
      System.out.println(chgrp);
    } else if ("filecrc".equals(cmd)) {
      System.out.println(filecrc);
    } else if ("undelete".equals(cmd)) {
      System.out.println(undelete);
    } else if ("hardlink".equals(cmd)) {
      System.out.println(undelete);
    } else if (Count.matches(cmd)) {
      System.out.println(Count.DESCRIPTION);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(fs);
      System.out.println(ls);
      System.out.println(lsr);
      System.out.println(lsrx);
      System.out.println(du);
      System.out.println(dus);
      System.out.println(mv);
      System.out.println(cp);
      System.out.println(rm);
      System.out.println(rmr);
      System.out.println(rmdir);
      System.out.println(put);
      System.out.println(copyFromLocal);
      System.out.println(moveFromLocal);
      System.out.println(get);
      System.out.println(getmerge);
      System.out.println(cat);
      System.out.println(copyToLocal);
      System.out.println(moveToLocal);
      System.out.println(mkdir);
      System.out.println(setrep);
      System.out.println(head);
      System.out.println(tail);
      System.out.println(touchz);
      System.out.println(touch);
      System.out.println(test);
      System.out.println(text);
      System.out.println(decompress);
      System.out.println(filecrc);
      System.out.println(undelete);
      System.out.println(compress);
      System.out.println(stat);
      System.out.println(chmod);
      System.out.println(chown);
      System.out.println(chgrp);
      System.out.println(Count.DESCRIPTION);
      System.out.println(help);
    }


  }

  private void showlinks(String src) throws IOException {
    Path srcPath = new Path(src);
    String[] files = srcPath.getFileSystem(getConf()).getHardLinkedFiles(
        srcPath);
    System.out.println("The following files (" + files.length
        + ") are hardlinked to " + src + " : ");
    for (String file : files) {
      System.out.println(file);
    }
  }

  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int doall(String cmd, String argv[], int startindex) {
    int exitCode = 0;
    int i = startindex;
    boolean rmSkipTrash = false;
    boolean rmdirIgnoreFail = false;
    boolean lsDirOnly = false;

    // Check for -skipTrash option in rm/rmr
    if(("-rm".equals(cmd) || "-rmr".equals(cmd))
        && "-skipTrash".equals(argv[i])) {
      rmSkipTrash = true;
      i++;
    }

    if ("-rmdir".equals(cmd) && "-ignore-fail-on-non-empty".equals(argv[i])) {
      rmdirIgnoreFail = true;
      i++;  
    }

    // fs -ls -d
    // having d in its option list
    if ("-ls".equals(cmd) &&
        argv[i].startsWith("-") &&
        argv[i].contains("d")) {
      lsDirOnly = true;
      i++;  
    }

    //
    // for each source file, issue the command
    //
    for (; i < argv.length; i++) {
      try {
        //
        // issue the command to the fs
        //
        if ("-cat".equals(cmd)) {
          cat(argv[i], true, false, 0L);
        } else if ("-mkdir".equals(cmd)) {
          mkdir(argv[i]);
        } else if ("-rm".equals(cmd)) {
          delete(argv[i], false, rmSkipTrash);
        } else if ("-showlinks".equals(cmd)) {
          showlinks(argv[i]);
        } else if ("-rmr".equals(cmd)) {
          delete(argv[i], true, rmSkipTrash);
        } else if ("-rmdir".equals(cmd)) {
          rmdir(argv[i], rmdirIgnoreFail, rmSkipTrash);
        } else if ("-du".equals(cmd)) {
          du(argv[i]);
        } else if ("-dus".equals(cmd)) {
          dus(argv[i]);
        } else if (Count.matches(cmd)) {
          new Count(argv, i, getConf()).runAll();
        } else if ("-ls".equals(cmd)) {
          if (lsDirOnly) {
            exitCode = ls(argv[i],
                EnumSet.of(LsOption.DirectoryOnly));
          } else {
            exitCode = ls(argv[i], EnumSet.noneOf(LsOption.class));
          }
        } else if ("-lsr".equals(cmd)) {
          exitCode = ls(argv[i], EnumSet.of(LsOption.Recursive));
        } else if ("-lsrx".equals(cmd)) {
          exitCode = ls(argv[i],
                        EnumSet.of(LsOption.Recursive, LsOption.WithBlockSize));
        } else if ("-touchz".equals(cmd)) {
          touchz(argv[i]);
        } else if ("-text".equals(cmd)) {
          text(argv[i]);
        } else if ("-decompress".equals(cmd)) {
          decompress(argv[i]);
        } else if ("-filecrc".equals(cmd)) {
          getFileCrc(argv[i]);
        } else if ("-undelete".equals(cmd)) {
          String userName = null;
          if ("-u".equals(argv[i])) {
            // make sure there's at least one file path specified
            if (argv.length < i+1) {
              printUsage(cmd);
              return -1;
            }
            i++;
            userName = argv[i++];
          }
          exitCode = undelete(argv[i],
              new Path(argv[i]).getFileSystem(getConf()), userName) ? 0 : -1;
        }
      } catch (RemoteException e) {
        //
        // This is a error returned by hadoop server. Print
        // out the first line of the error message.
        //
        exitCode = -1;
        try {
          String[] content;
          content = e.getLocalizedMessage().split("\n");
          System.err.println(cmd.substring(1) + ": " +
                             content[0]);
        } catch (Exception ex) {
          System.err.println(cmd.substring(1) + ": " +
                             ex.getLocalizedMessage());
        }
      } catch (IOException e) {
        //
        // IO exception encountered locally.
        //
        exitCode = -1;
        String content = e.getLocalizedMessage();
        if (content != null) {
          content = content.split("\n")[0];
        }
        System.err.println(cmd.substring(1) + ": " +
                          content);
      }
    }
    return exitCode;
  }

  /**
   * Displays format of commands.
   *
   */
  private static void printUsage(String cmd) {
    String prefix = "Usage: java " + FsShell.class.getSimpleName();
    if ("-fs".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [-fs <local | file system URI>]");
    } else if ("-conf".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [-conf <configuration file>]");
    } else if ("-D".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [-D <[property=value>]");
    } else if ("-ls".equals(cmd) || "-lsr".equals(cmd) || "-lsrx".equals(cmd) ||
               "-du".equals(cmd) || "-dus".equals(cmd) ||
               "-touchz".equals(cmd) || "-mkdir".equals(cmd) ||
               "-text".equals(cmd) || "-decompress".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [" + cmd + " <path>]");
    } else if (Count.matches(cmd)) {
      System.err.println(prefix + " [" + Count.USAGE + "]");
    } else if ("-rm".equals(cmd) || "-rmr".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + cmd +
                           " [-skipTrash] <src>]");
    } else if ("-rmdir".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + cmd + 
                           " [-ignore-fail-on-non-empty] <src>]");
    } else if ("-mv".equals(cmd) || "-cp".equals(cmd)
        || "-compress".equals(cmd) || "-hardlink".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [" + cmd + " <src> <dst>]");
    } else if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd) ||
               "-moveFromLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [" + cmd + " <localsrc> ... <dst>]");
    } else if ("-get".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + GET_SHORT_USAGE + "]");
    } else if ("-copyToLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + COPYTOLOCAL_SHORT_USAGE+ "]");
    } else if ("-moveToLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [" + cmd + " [-crc] <src> <localdst>]");
    } else if ("-cat".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [" + cmd + " <src>]");
    } else if ("-showlinks".equals(cmd)) {
      System.err.println("Usage: java FsShell" + " [" + cmd + " <srcs...>]");
    } else if ("-setrep".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + SETREP_SHORT_USAGE + "]");
    } else if ("-test".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [-test -[ezd] <path>]");
    } else if ("-stat".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [-stat [format] <path>]");
    } else if ("-head".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + HEAD_USAGE + "]");
    } else if ("-tail".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + TAIL_USAGE + "]");
    } else if ("-touch".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + FsShellTouch.TOUCH_USAGE + "]");
    } else {
      System.err.println("Usage: java FsShell");
      System.err.println("           [-ls <path>]");
      System.err.println("           [-lsr <path>]");
      System.err.println("           [-lsrx <path>]");
      System.err.println("           [-du <path>]");
      System.err.println("           [-dus <path>]");
      System.err.println("           [" + Count.USAGE + "]");
      System.err.println("           [-mv <src> <dst>]");
      System.err.println("           [-cp <src> <dst>]");
      System.err.println("           [-hardlink <src> <dst>]");
      System.err.println("           [-showlinks <srcs ..>] shows the files "
          + "hardlinked to the given file");
      System.err.println("           [-rm [-skipTrash] <path>]");
      System.err.println("           [-rmr [-skipTrash] <path>]");
      System.err.println("           [-rmdir [-ignore-fail-on-non-empty] <path>]");
      System.err.println("           [-expunge]");
      System.err.println("           [-put [-validate] [-rate <bandwidth>] "
          + "<localsrc> ... <dst>]");
      System.err.println("           [-copyFromLocal [-validate] [-rate <bandwidth>] "
          + "<localsrc> ... <dst>]");
      System.err.println("           [-moveFromLocal <localsrc> ... <dst>]");
      System.err.println("           [" + GET_SHORT_USAGE + "]");
      System.err.println("           [-getmerge <src> <localdst> [addnl]]");
      System.err.println("           [-cat <src>]");
      System.err.println("           [-text <src>]");
      System.err.println("           [-decompress <src>]");
      System.err.println("           [-compress <src> <tgt>]");
      System.err.println("           [-undelete [-u <username>] <src>]");
      System.err.println("           [" + COPYTOLOCAL_SHORT_USAGE + "]");
      System.err.println("           [-moveToLocal [-rate <bandwidth>] [-crc] "
          + "<src> <localdst>]");
      System.err.println("           [-mkdir <path>]");
      System.err.println("           [" + SETREP_SHORT_USAGE + "]");
      System.err.println("           [-touchz <path>]");
      System.err.println("           [" + FsShellTouch.TOUCH_USAGE + "]");
      System.err.println("           [-test -[ezd] <path>]");
      System.err.println("           [-stat [format] <path>]");
      System.err.println("           [" + HEAD_USAGE + "]");
      System.err.println("           [" + TAIL_USAGE + "]");
      System.err.println("           [" + FsShellPermissions.CHMOD_USAGE + "]");
      System.err.println("           [" + FsShellPermissions.CHOWN_USAGE + "]");
      System.err.println("           [" + FsShellPermissions.CHGRP_USAGE + "]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-put".equals(cmd) || "-test".equals(cmd) ||
        "-copyFromLocal".equals(cmd) || "-moveFromLocal".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-get".equals(cmd) ||
               "-copyToLocal".equals(cmd) || "-moveToLocal".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-mv".equals(cmd) || "-cp".equals(cmd)
        || "-compress".equals(cmd) || "-hardlink".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-rm".equals(cmd) || "-rmr".equals(cmd) ||
               "-rmdir".equals(cmd) || "-cat".equals(cmd) ||
               "-mkdir".equals(cmd) || "-touchz".equals(cmd) ||
               "-stat".equals(cmd) || "-text".equals(cmd) ||
               "-decompress".equals(cmd) || "-touch".equals(cmd) ||
               "-undelete".equals(cmd) || "-showlinks".equals(cmd)) {
      if (argv.length < 2) {
        printUsage(cmd);
        return exitCode;
      }
    }

    // initialize FsShell
    try {
      init();
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to FS. command aborted.");
      return exitCode;
    }

    exitCode = 0;
    try {
      if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
        boolean validate = false;
        long rate = -1L;
        if (argv[i].equals("-validate")) {
          validate = true;
          i++;
        }
        if (argv[i].equals("-rate")) {
          i++;
          rate = Long.parseLong(argv[i]);
          if (rate <= 0L) {
            throw new IllegalArgumentException(
                "IO Throttler bandwidth must be positive. The input rate is invalid: " +
                rate);
          }
          i++;
        }
        Path[] srcs = new Path[argv.length-1-i];
        for (int j=0 ; i < argv.length-1 ;)
          srcs[j++] = new Path(argv[i++]);
        DataTransferThrottler throttler =
          rate > 0L ? new DataTransferThrottler(rate) : null;
        copyFromLocal(srcs, argv[i++], validate, throttler);
      } else if ("-moveFromLocal".equals(cmd)) {
        Path[] srcs = new Path[argv.length-2];
        for (int j=0 ; i < argv.length-1 ;)
          srcs[j++] = new Path(argv[i++]);
        moveFromLocal(srcs, argv[i++]);
      } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
        long rate = -1L;
        if (argv[i].equals("-rate")) {
          i++;
          rate = Long.parseLong(argv[i]);
          if (rate <= 0L) {
            throw new IllegalArgumentException(
                "IO Throttler bandwidth must be positive. The input rate is invalid: " +
                rate);
          }
          i++;
        }
        DataTransferThrottler throttler =
          rate > 0L ? new DataTransferThrottler(rate) : null;
        copyToLocal(argv, i, throttler);
      } else if ("-getmerge".equals(cmd)) {
        if (argv.length>i+2)
          copyMergeToLocal(argv[i++], new Path(argv[i++]), Boolean.parseBoolean(argv[i++]));
        else
          copyMergeToLocal(argv[i++], new Path(argv[i++]));
      } else if ("-cat".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-text".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-decompress".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-moveToLocal".equals(cmd)) {
        moveToLocal(argv[i++], new Path(argv[i++]));
      } else if ("-setrep".equals(cmd)) {
        setReplication(argv, i);
      } else if ("-chmod".equals(cmd) ||
                 "-chown".equals(cmd) ||
                 "-chgrp".equals(cmd)) {
        FsShellPermissions.changePermissions(cmd, argv, i, this);
      } else if ("-ls".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i);
        } else {
          exitCode = ls(Path.CUR_DIR, EnumSet.noneOf(LsOption.class));
        }
      } else if ("-lsr".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i);
        } else {
          exitCode = ls(Path.CUR_DIR, EnumSet.of(LsOption.Recursive));
        }
      } else if ("-lsrx".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i);
        } else {
          exitCode = ls(Path.CUR_DIR,
                        EnumSet.of(LsOption.Recursive, LsOption.WithBlockSize));
        }
      } else if ("-mv".equals(cmd)) {
        exitCode = rename(argv, getConf());
      } else if ("-cp".equals(cmd)) {
        exitCode = copy(argv, getConf());
      } else if ("-hardlink".equals(cmd)) {
        exitCode = hardlink(argv);
      } else if ("-compress".equals(cmd)) {
        exitCode = compress(argv, getConf());
      } else if ("-rm".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-showlinks".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-rmr".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-rmdir".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-expunge".equals(cmd)) {
        expunge();
      } else if ("-du".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i);
        } else {
          du(".");
        }
      } else if ("-dus".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i);
        } else {
          dus(".");
        }
      } else if (Count.matches(cmd)) {
        exitCode = new Count(argv, i, getConf()).runAll();
      } else if ("-mkdir".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-filecrc".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-undelete".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-touchz".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-touch".equals(cmd)) {
        FsShellTouch.touchFiles(getFS(), argv, i);
      } else if ("-test".equals(cmd)) {
        exitCode = test(argv, i);
      } else if ("-stat".equals(cmd)) {
        if (i + 1 < argv.length) {
          stat(argv[i++].toCharArray(), argv[i++]);
        } else {
          stat("%y".toCharArray(), argv[i]);
        }
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i].substring(1));
        } else {
          printHelp("");
        }
      } else if ("-head".equals(cmd)) {
        head(argv, i);
      } else if ("-tail".equals(cmd)) {
        tail(argv, i);
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " +
                           content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " +
                           ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      //
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " +
                         e.getLocalizedMessage());
    } catch (Exception re) {
      exitCode = -1;
      re.printStackTrace();
      System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage());
    } finally {
    }
    return exitCode;
  }

  public void close() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    FsShell shell = new FsShell();
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }

  /**
   * Accumulate exceptions if there is any.  Throw them at last.
   */
  private abstract class DelayedExceptionThrowing {
    abstract void process(Path p, FileSystem srcFs) throws IOException;

    final void globAndProcess(Path srcPattern, FileSystem srcFs
        ) throws IOException {
      List<IOException> exceptions = new ArrayList<IOException>();
      for(Path p : FileUtil.stat2Paths(srcFs.globStatus(srcPattern),
                                       srcPattern))
        try { process(p, srcFs); }
        catch(IOException ioe) { exceptions.add(ioe); }

      if (!exceptions.isEmpty())
        if (exceptions.size() == 1)
          throw exceptions.get(0);
        else
          throw new IOException("Multiple IOExceptions: " + exceptions);
    }
  }
}
