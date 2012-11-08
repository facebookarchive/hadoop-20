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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.StringUtils;

/** Provide command line access to a FileSystem. */
public class FreightStreamer extends Configured implements Tool {

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
  static final String SETREP_SHORT_USAGE="-setrep [-R] [-w] <rep> <path/file>";
  static final String GET_SHORT_USAGE = "-get [-ignoreCrc] [-crc] <src> <localdst>";
  static final String COPYTOLOCAL_SHORT_USAGE = GET_SHORT_USAGE.replace(
      "-get", "-copyToLocal");
  static final String TAIL_USAGE="-tail [-f] <file>";

  /**
   */
  public FreightStreamer() {
    this(null);
  }

  public FreightStreamer(Configuration conf) {
    super(conf);
    fs = null;
    trash = null;
  }
  
  public void init() throws IOException {
    getConf().setQuietMode(true);
    if (this.fs == null) {
     this.fs = FileSystem.get(getConf());
    }
    if (this.trash == null) {
      this.trash = new Trash(getConf());
    }
  }

  public enum LsOption {
    Recursive,
    WithBlockSize
  };

  /** 
   * Print from src to stdout.
   */
  private void printToStdout(InputStream in) throws IOException {
    try {
      IOUtils.copyBytes(in, System.out, getConf(), false);
    } finally {
      in.close();
    }
  }

  
  /**
   * Obtain the indicated files that match the file pattern <i>srcf</i>
   * and copy them to the local name. srcf is kept.
   * When copying multiple files, the destination must be a directory. 
   * Otherwise, IOException is thrown.
   * @param argv: arguments
   * @param pos: Ignore everything before argv[pos]  
   * @exception: IOException  
   * @see org.apache.hadoop.fs.FileSystem.globStatus 
   */
  void copyToLocal(String[]argv, int pos) throws IOException {
    CommandFormat cf = new CommandFormat("copyToLocal", 2,2,"crc","ignoreCrc");
    
    String srcstr = null;
    String dststr = null;
    try {
      List<String> parameters = cf.parse(argv, pos);
      srcstr = parameters.get(0);
      dststr = parameters.get(1);
    }
    catch(IllegalArgumentException iae) {
      System.err.println("Usage: java FreightStreamer " + GET_SHORT_USAGE);
      throw iae;
    }
    boolean copyCrc = cf.getOpt("crc");
    final boolean verifyChecksum = !cf.getOpt("ignoreCrc");

    if (dststr.equals("-")) {
      if (copyCrc) {
        System.err.println("-crc option is not valid when destination is stdout.");
      }
      cat(srcstr, verifyChecksum);
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
        Path p = status.getPath();
        File f = dstIsDir? new File(dst, p.getName()): dst;
        copyToLocal(srcFS, p, f, copyCrc);
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
   * @param src source path
   * @param dst destination
   * @param copyCrc copy CRC files?
   * @exception IOException If some IO failed
   */
  private void copyToLocal(final FileSystem srcFS, final Path src,
                           final File dst, final boolean copyCrc)
    throws IOException {
    /* Keep the structure similar to ChecksumFileSystem.copyToLocal(). 
     * Ideal these two should just invoke FileUtil.copy() and not repeat
     * recursion here. Of course, copy() should support two more options :
     * copyCrc and useTmpFile (may be useTmpFile need not be an option).
     */
    
    if (!srcFS.getFileStatus(src).isDir()) {
      if (dst.exists()) {
        // match the error message in FileUtil.checkDest():
        throw new IOException("Target " + dst + " already exists");
      }
      
      // use absolute name so that tmp file is always created under dest dir
      File tmp = FileUtil.createLocalTempFile(dst.getAbsoluteFile(),
                                              COPYTOLOCAL_PREFIX, true);
      if (!FileUtil.copy(srcFS, src, tmp, false, srcFS.getConf())) {
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
        copyToLocal(csfs.getRawFileSystem(), csfs.getChecksumFile(src),
                    dstcs, false);
      } 
    } else {
      // once FileUtil.copy() supports tmp file, we don't need to mkdirs().
      dst.mkdirs();
      for(FileStatus path : srcFS.listStatus(src)) {
        copyToLocal(srcFS, path.getPath(), 
                    new File(dst, path.getPath().getName()), copyCrc);
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
  void cat(String src, boolean verifyChecksum) throws IOException {
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
        printToStdout(srcFs.open(p));
      }
    }.globAndProcess(srcPattern, getSrcFileSystem(srcPattern, verifyChecksum));
  }

  private class TextRecordInputStream extends InputStream {
    SequenceFile.Reader r;
    WritableComparable key;
    Writable val;

    DataInputBuffer inbuf;
    DataOutputBuffer outbuf;

    public TextRecordInputStream(FileStatus f) throws IOException {
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
          return new TextRecordInputStream(srcFs.getFileStatus(p));
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
   * Returns the Trash object associated with this shell.
   */
  public Path getCurrentTrashDir() throws IOException {
    return trash.getCurrentTrashDir();
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
      System.err.println("Usage: java FreightStreamer " + TAIL_USAGE);
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

  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int doall(String cmd, String argv[], int startindex) {
    int exitCode = 0;
    int i = startindex;
    boolean rmSkipTrash = false;
    
    // Check for -skipTrash option in rm/rmr
    if(("-rm".equals(cmd) || "-rmr".equals(cmd)) 
        && "-skipTrash".equals(argv[i])) {
      rmSkipTrash = true;
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
          cat(argv[i], true);
        } else if ("-du".equals(cmd)) {
          du(argv[i]);
        } else if ("-dus".equals(cmd)) {
          dus(argv[i]);
        } else if (Count.matches(cmd)) {
          new Count(argv, i, getConf()).runAll();
        } else if ("-text".equals(cmd)) {
          text(argv[i]);
        } else if ("-decompress".equals(cmd)) {
          decompress(argv[i]);
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
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
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
        return exitCode;
      }
    } else if ("-get".equals(cmd) || 
               "-copyToLocal".equals(cmd) || "-moveToLocal".equals(cmd)) {
      if (argv.length < 3) {
        return exitCode;
      }
    } else if ("-mv".equals(cmd) || "-cp".equals(cmd) || "-compress".equals(cmd)) {
      if (argv.length < 3) {
        return exitCode;
      }
    } else if ("-rm".equals(cmd) || "-rmr".equals(cmd) ||
               "-cat".equals(cmd) || "-mkdir".equals(cmd) ||
               "-touchz".equals(cmd) || "-stat".equals(cmd) ||
               "-text".equals(cmd) || "-decompress".equals(cmd)) {
      if (argv.length < 2) {
        return exitCode;
      }
    }

    // initialize FreightStreamer
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
      if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
        copyToLocal(argv, i);
      } else if ("-cat".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-text".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-decompress".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-moveToLocal".equals(cmd)) {
        moveToLocal(argv[i++], new Path(argv[i++]));
      } else if ("-compress".equals(cmd)) {
        exitCode = compress(argv, getConf());
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
      } else if ("-test".equals(cmd)) {
        exitCode = test(argv, i);
      } else if ("-stat".equals(cmd)) {
        if (i + 1 < argv.length) {
          stat(argv[i++].toCharArray(), argv[i++]);
        } else {
          stat("%y".toCharArray(), argv[i]);
        }
      } else if ("-tail".equals(cmd)) {
        tail(argv, i);           
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
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
    FreightStreamer streamer = new FreightStreamer();
    int res;
    try {
      res = ToolRunner.run(streamer, argv);
    } finally {
      streamer.close();
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
