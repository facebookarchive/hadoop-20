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

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.IOThrottler;
import org.apache.hadoop.util.InjectionEventCore;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * A collection of file-processing util methods
 */
public class FileUtil {
  static final Log LOG = LogFactory.getLog(FileUtil.class.getName());
  
  /**
   * convert an array of FileStatus to an array of Path
   * 
   * @param stats
   *          an array of FileStatus objects
   * @return an array of paths corresponding to the input
   */
  public static Path[] stat2Paths(FileStatus[] stats) {
    if (stats == null)
      return null;
    Path[] ret = new Path[stats.length];
    for (int i = 0; i < stats.length; ++i) {
      ret[i] = stats[i].getPath();
    }
    return ret;
  }

  /**
   * convert an array of FileStatus to an array of Path.
   * If stats if null, return path
   * @param stats
   *          an array of FileStatus objects
   * @param path
   *          default path to return in stats is null
   * @return an array of paths corresponding to the input
   */
  public static Path[] stat2Paths(FileStatus[] stats, Path path) {
    if (stats == null)
      return new Path[]{path};
    else
      return stat2Paths(stats);
  }

  /**
   * Delete a directory and all its contents.  If
   * we return false, the directory may be partially-deleted.
   */
  public static boolean fullyDelete(File dir) throws IOException {
    boolean deleted = true;
    File contents[] = dir.listFiles();
    if (contents != null) {
      for (int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          if (!contents[i].delete()) {
            deleted = false;
          }
        } else {
          //try deleting the directory
          // this might be a symlink
          boolean b = false;
          b = contents[i].delete();
          if (b){
            //this was indeed a symlink or an empty directory
            continue;
          }
          // if not an empty directory or symlink let
          // fullydelete handle it.
          if (!fullyDelete(contents[i])) {
            deleted = false;
          }
        }
      }
    }
    return dir.delete() && deleted;
  }

  /**
   * Recursively delete a directory.
   *
   * @param fs {@link FileSystem} on which the path is present
   * @param dir directory to recursively delete
   * @throws IOException
   * @deprecated Use {@link FileSystem#delete(Path, boolean)}
   */
  @Deprecated
  public static void fullyDelete(FileSystem fs, Path dir)
  throws IOException {
    fs.delete(dir, true);
  }

  //
  // If the destination is a subdirectory of the source, then
  // generate exception
  //
  private static void checkDependencies(FileSystem srcFS,
                                        Path src,
                                        FileSystem dstFS,
                                        Path dst)
                                        throws IOException {
    if (srcFS == dstFS) {
      String srcq = src.makeQualified(srcFS).toString() + Path.SEPARATOR;
      String dstq = dst.makeQualified(dstFS).toString() + Path.SEPARATOR;
      if (dstq.startsWith(srcq)) {
        if (srcq.length() == dstq.length()) {
          throw new IOException("Cannot copy " + src + " to itself.");
        } else {
          throw new IOException("Cannot copy " + src + " to its subdirectory " +
                                dst);
        }
      }
    }
  }

  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, Path src, 
                             FileSystem dstFS, Path dst, 
                             boolean deleteSource,
                             Configuration conf) throws IOException {
    return copy(srcFS, src, dstFS, dst, deleteSource, true, conf);
  }

  public static boolean copy(FileSystem srcFS, Path[] srcs, 
                             FileSystem dstFS, Path dst,
                             boolean deleteSource, 
                             boolean overwrite, Configuration conf)
                             throws IOException {
    return copy(srcFS, srcs, dstFS, dst, deleteSource, overwrite, false, conf);
  }
  
  /**
   * copy a list of files from a source file system to a directory dst 
   * in a destination file system
   * 
   * @param srcFS source file system
   * @param srcs a list of source files
   * @param dstFS destination file system
   * @param dst destination directory
   * @param deleteSource if source should be deleted
   * @param overwrite if destination should be overwritten
   * @param validate if destination file should be validated
   * @param conf configuration
   * @return true if copy succeeds
   * @throws IOException if error occurs
   */
  public static boolean copy(FileSystem srcFS, Path[] srcs, 
      FileSystem dstFS, Path dst,
      boolean deleteSource, 
      boolean overwrite, boolean validate, Configuration conf)
      throws IOException {
      return copy(srcFS, srcs, dstFS, dst, deleteSource,
          overwrite, validate, conf, null);
  }

  /**
   * copy a list of files from a source file system to a directory dst 
   * in a destination file system
   * 
   * @param srcFS source file system
   * @param srcs a list of source files
   * @param dstFS destination file system
   * @param dst destination directory
   * @param deleteSource if source should be deleted
   * @param overwrite if destination should be overwritten
   * @param validate if destination file should be validated
   * @param conf configuration
   * @param throttler IOThrottler for copy
   * @return true if copy succeeds
   * @throws IOException if error occurs
   */
  public static boolean copy(FileSystem srcFS, Path[] srcs, 
      FileSystem dstFS, Path dst,
      boolean deleteSource, 
      boolean overwrite, boolean validate,
      Configuration conf,
      IOThrottler throttler)
      throws IOException {
    boolean gotException = false;
    boolean returnVal = true;
    StringBuffer exceptions = new StringBuffer();

    if (srcs.length == 1)
      return copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, 
          validate, conf, throttler);

    // Check if dest is directory
    if (!dstFS.exists(dst)) {
      throw new IOException("`" + dst +"': specified destination directory " +
                            "doest not exist");
    } else {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (!sdst.isDir())
        throw new IOException("copying multiple files, but last argument `" +
                              dst + "' is not a directory");
    }

    for (Path src : srcs) {
      try {
        if (!copy(srcFS, src, dstFS, dst, deleteSource, overwrite, 
            validate, conf, throttler))
          returnVal = false;
      } catch (IOException e) {
        gotException = true;
        exceptions.append(e.getMessage());
        exceptions.append("\n");
      }
    }
    if (gotException) {
      throw new IOException(exceptions.toString());
    }
    return returnVal;
  }

  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, Path src, 
      FileSystem dstFS, Path dst, 
      boolean deleteSource,
      boolean overwrite,
      Configuration conf) throws IOException {
    return copy(srcFS, src, dstFS, dst, deleteSource, overwrite, false, conf);
  }

  /**
   * Copy files between file systems
   * @param srcFS source file system
   * @param src source file
   * @param dstFS destination file system
   * @param dst destination file
   * @param deleteSource if source should be deleted
   * @param overwrite if destination could be overwritten if exists
   * @param validate if source and destination should be validated
   * @param conf configuration
   * @return true if succeed
   * @throws IOException if error occurs
   */
  public static boolean copy(FileSystem srcFS, Path src, 
                             FileSystem dstFS, Path dst, 
                             boolean deleteSource,
                             boolean overwrite, boolean validate,
                             Configuration conf) throws IOException {
    return copy(srcFS, src, dstFS, dst, deleteSource,
        overwrite, validate, conf, null);
  }

  /**
   * Copy files between file systems
   * @param srcFS source file system
   * @param src source file
   * @param dstFS destination file system
   * @param dst destination file
   * @param deleteSource if source should be deleted
   * @param overwrite if destination could be overwritten if exists
   * @param validate if source and destination should be validated
   * @param conf configuration
   * @param throttler IOThrottler for copy
   * @return true if succeed
   * @throws IOException if error occurs
   */
  public static boolean copy(FileSystem srcFS, Path src, 
                             FileSystem dstFS, Path dst, 
                             boolean deleteSource,
                             boolean overwrite, boolean validate,
                             Configuration conf,
                             IOThrottler throttler) throws IOException {
    dst = checkDest(src.getName(), dstFS, dst, overwrite);

    FileStatus srcFileStatus = srcFS.getFileStatus(src);
    if (srcFileStatus == null) {
      throw new FileNotFoundException("File not found: " + src);
    }
    if (srcFileStatus.isDir()) {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i].getPath(), dstFS, 
             new Path(dst, contents[i].getPath().getName()),
             deleteSource, overwrite, validate, conf, throttler);
      }
    } else {
      InputStream in=null;
      OutputStream out = null;
      try {
        in = srcFS.open(src);
        out = dstFS.create(dst, overwrite);
        IOUtils.copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096),
            true, throttler);
        // validate file size
        if (validate) {
          InjectionHandler.processEventIO(
              InjectionEventCore.FILE_TRUNCATION, dstFS, dst);
          FileStatus dstFileStatus = dstFS.getFileStatus(dst);
          if (dstFileStatus == null || 
              dstFileStatus.getLen() != srcFileStatus.getLen()) {
            throw new IOException("Mismatched file length: src=" + src +
                " dst=" + dst);
          }
        }
      } catch (IOException e) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        dstFS.delete(dst, true);
        throw e;
      }
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  }

  /** Copy all files in a directory to one output file (merge). */
  public static boolean copyMerge(FileSystem srcFS, Path srcDir, 
                                  FileSystem dstFS, Path dstFile, 
                                  boolean deleteSource,
                                  Configuration conf, String addString) throws IOException {
    dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);

    if (!srcFS.getFileStatus(srcDir).isDir())
      return false;
   
    OutputStream out = dstFS.create(dstFile);
    
    try {
      FileStatus contents[] = srcFS.listStatus(srcDir);
      for (int i = 0; i < contents.length; i++) {
        if (!contents[i].isDir()) {
          InputStream in = srcFS.open(contents[i].getPath());
          try {
            IOUtils.copyBytes(in, out, conf, false);
            if (addString!=null)
              out.write(addString.getBytes("UTF-8"));

          } finally {
            in.close();
          }
        }
      }
    } finally {
      out.close();
    }


    if (deleteSource) {
      return srcFS.delete(srcDir, true);
    } else {
      return true;
    }
  }

  public static boolean copy(File src, FileSystem dstFS, File dst,
      boolean deleteSource, Configuration conf) throws IOException {
    return copy(src, dstFS, new Path(dst.getAbsolutePath()), deleteSource, conf);
  }

  /** Copy local files to a FileSystem. */
  public static boolean copy(File src,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             Configuration conf) throws IOException {
    dst = checkDest(src.getName(), dstFS, dst, false);

    if (src.isDirectory()) {
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      File contents[] = src.listFiles();
      for (int i = 0; i < contents.length; i++) {
        copy(contents[i], dstFS, new Path(dst, contents[i].getName()),
             deleteSource, conf);
      }
    } else if (src.isFile()) {
      InputStream in = null;
      OutputStream out =null;
      try {
        in = new FileInputStream(src);
        out = dstFS.create(dst);
        IOUtils.copyBytes(in, out, conf);
      } catch (IOException e) {
        IOUtils.closeStream( out );
        IOUtils.closeStream( in );
        throw e;
      }
    } else {
      throw new IOException(src.toString() + 
                            ": No such file or directory");
    }
    if (deleteSource) {
      return FileUtil.fullyDelete(src);
    } else {
      return true;
    }
  }
  
  /** Copy FileSystem files to local files. */
  public static boolean copy(FileSystem srcFS, Path src, 
                             File dst, boolean deleteSource,
                             Configuration conf) throws IOException {
    return copy(srcFS, src, dst, deleteSource, conf, false, 0L);
  }
  
  public static void printChecksumToStderr(long checksum, Path file) {
    System.err.println(Long.toHexString(checksum) + " " + file);
  }

  /** Copy FileSystem files to local files.
   * If src is a file:
   *    genCrc = true: print the CRC checksum of file to stderr
   *    offset >=0 : copy starts from offset
   *    offset < 0 : copy starts from fileLen + offset (throw exception if 
   *                 file is still under-construction) 
   */
  public static boolean copy(FileSystem srcFS, Path src, 
                             File dst, boolean deleteSource,
                             Configuration conf, boolean genCrc, 
                             long offset) throws IOException {
    return copy(srcFS, src, dst, deleteSource, false, conf, genCrc, offset);
  }

  public static boolean copy(FileSystem srcFS, Path src, File dst,
      boolean deleteSource, boolean validate,
      Configuration conf, boolean genCrc, long offset) throws IOException {
    return copy(srcFS, src, dst, deleteSource, validate,
        conf, genCrc, offset, null);
  }

  public static boolean copy(FileSystem srcFS, Path src, File dst,
      boolean deleteSource, boolean validate,
      Configuration conf, boolean genCrc, long offset,
      IOThrottler throttler) throws IOException {
    FileStatus fStat = srcFS.getFileStatus(src);
    if (fStat == null) {
      throw new FileNotFoundException(src.toString());
    }
    if (fStat.isDir()) {
      if (!dst.mkdirs()) {
        return false;
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i].getPath(), 
             new File(dst, contents[i].getPath().getName()),
             deleteSource, validate, conf, genCrc, offset, throttler);
      }
    } else {
      FSDataInputStream in = srcFS.open(src);
      if (offset < 0) {
        if (in.isUnderConstruction()) {
          throw new IOException("Couldn't copy under-construction file with " +
                                "negative offset " + offset);
        }
        offset += fStat.getLen();
      }
      in.seek(offset);
      if (!genCrc) {
        IOUtils.copyBytes(in, new FileOutputStream(dst),
            conf.getInt("io.file.buffer.size", 4096), true, throttler);
      } else {
        long checksum = IOUtils.copyBytesAndGenerateCRC(in,
            new FileOutputStream(dst), conf.getInt("io.file.buffer.size", 4096),
            true, throttler);
        printChecksumToStderr(checksum, src);
      }
      if (validate) {
        InjectionHandler.processEventIO(
            InjectionEventCore.FILE_TRUNCATION, FileSystem.getLocal(conf), dst);
        if (fStat.getLen() - offset != dst.length()) {
          throw new IOException("Mismatched file length: src=" + src +
              " dst=" + dst);
        }
      }
    } 

    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  }

  private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
      boolean overwrite) throws IOException {
    if (dstFS.exists(dst)) {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (sdst.isDir()) {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
      } else if (!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
    }
    return dst;
  }

  /**
   * This class is only used on windows to invoke the cygpath command.
   */
  private static class CygPathCommand extends Shell {
    String[] command;
    String result;
    CygPathCommand(String path) throws IOException {
      command = new String[]{"cygpath", "-u", path};
      run();
    }
    String getResult() throws IOException {
      return result;
    }
    protected String[] getExecString() {
      return command;
    }
    protected void parseExecResult(BufferedReader lines) throws IOException {
      String line = lines.readLine();
      if (line == null) {
        throw new IOException("Can't convert '" + command[2] +
                              " to a cygwin path");
      }
      result = line;
    }
  }

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param filename The filename to convert
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  public static String makeShellPath(String filename) throws IOException {
    if (Path.WINDOWS) {
      return new CygPathCommand(filename).getResult();
    } else {
      return filename;
    }
  }

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param file The filename to convert
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  public static String makeShellPath(File file) throws IOException {
    return makeShellPath(file, false);
  }

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param file The filename to convert
   * @param makeCanonicalPath
   *          Whether to make canonical path for the file passed
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  public static String makeShellPath(File file, boolean makeCanonicalPath)
  throws IOException {
    if (makeCanonicalPath) {
      return makeShellPath(file.getCanonicalPath());
    } else {
      return makeShellPath(file.toString());
    }
  }

  /**
   * Takes an input dir and returns the du on that local directory. Very basic
   * implementation.
   *
   * @param dir
   *          The input dir to get the disk space of this local dir
   * @return The total disk space of the input local directory
   */
  public static long getDU(File dir) {
    long size = 0;
    if (!dir.exists())
      return 0;
    if (!dir.isDirectory()) {
      return dir.length();
    } else {
      size = dir.length();
      File[] allFiles = dir.listFiles();
      for (int i = 0; i < allFiles.length; i++) {
        size = size + getDU(allFiles[i]);
      }
      return size;
    }
  }

  /**
   * Given a File input it will unzip the file in a the unzip directory
   * passed as the second parameter
   * @param inFile The zip file as input
   * @param unzipDir The unzip directory where to unzip the zip file.
   * @throws IOException
   */
  public static void unZip(File inFile, File unzipDir) throws IOException {
    Enumeration<? extends ZipEntry> entries;
    ZipFile zipFile = new ZipFile(inFile);

    try {
      entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = zipFile.getInputStream(entry);
          try {
            File file = new File(unzipDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create " +
                                      file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      zipFile.close();
    }
  }

  /**
   * Given a Tar File as input it will untar the file in a the untar directory
   * passed as the second parameter
   *
   * This utility will untar ".tar" files and ".tar.gz","tgz" files.
   *
   * @param inFile The tar file as input.
   * @param untarDir The untar directory where to untar the tar file.
   * @throws IOException
   */
  public static void unTar(File inFile, File untarDir) throws IOException {
    if (!untarDir.mkdirs()) {
      if (!untarDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create " + untarDir);
      }
    }

    StringBuffer untarCommand = new StringBuffer();
    boolean gzipped = inFile.toString().endsWith("gz");
    if (gzipped) {
      untarCommand.append(" gzip -dc '");
      untarCommand.append(FileUtil.makeShellPath(inFile));
      untarCommand.append("' | (");
    }
    untarCommand.append("cd '");
    untarCommand.append(FileUtil.makeShellPath(untarDir));
    untarCommand.append("' ; ");
    untarCommand.append("tar -xf ");

    if (gzipped) {
      untarCommand.append(" -)");
    } else {
      untarCommand.append(FileUtil.makeShellPath(inFile));
    }
    String[] shellCmd = { "bash", "-c", untarCommand.toString() };
    ShellCommandExecutor shexec = new ShellCommandExecutor(shellCmd);
    shexec.execute();
    int exitcode = shexec.getExitCode();
    if (exitcode != 0) {
      throw new IOException("Error untarring file " + inFile +
                  ". Tar process exited with exit code " + exitcode);
    }
  }

  /**
   * Create a soft link between a src and destination
   * only on a local disk. HDFS does not support this
   * @param target the target for symlink
   * @param linkname the symlink
   * @return value returned by the command
   */
  public static int symLink(String target, String linkname) throws IOException{
    String cmd = "ln -s " + target + " " + linkname;
    Process p = Runtime.getRuntime().exec(cmd, null);
    int returnVal = -1;
    try{
      returnVal = p.waitFor();
    } catch(InterruptedException e){
      //do nothing as of yet
    }
    return returnVal;
  }

  /**
   * Change the permissions on a filename.
   * @param filename the name of the file to change
   * @param perm the permission string
   * @return the exit code from the command
   * @throws IOException
   * @throws InterruptedException
   */
  public static int chmod(String filename, String perm
                          ) throws IOException, InterruptedException {
    return chmod(filename, perm, false);
  }

  /**
   * Change the permissions on a file / directory, recursively, if
   * needed.
   * @param filename name of the file whose permissions are to change
   * @param perm permission string
   * @param recursive true, if permissions should be changed recursively
   * @return the exit code from the command.
   * @throws IOException
   * @throws InterruptedException
   */
  public static int chmod(String filename, String perm, boolean recursive)
                            throws IOException, InterruptedException {
    StringBuffer cmdBuf = new StringBuffer();
    cmdBuf.append("chmod ");
    if (recursive) {
      cmdBuf.append("-R ");
    }
    cmdBuf.append(perm).append(" ");
    cmdBuf.append(filename);
    String[] shellCmd = {"bash", "-c" ,cmdBuf.toString()};
    ShellCommandExecutor shExec = new ShellCommandExecutor(shellCmd);
    try {
      shExec.execute();
    }catch(IOException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Error while changing permission : " + filename 
            +" Exception: " + StringUtils.stringifyException(e));
      }
    }
    return shExec.getExitCode();
  }
  
  /**
   * Create a tmp file for a base file.
   * @param basefile the base file of the tmp
   * @param prefix file name prefix of tmp
   * @param isDeleteOnExit if true, the tmp will be deleted when the VM exits
   * @return a newly created tmp file
   * @exception IOException If a tmp file cannot created
   * @see java.io.File#createTempFile(String, String, File)
   * @see java.io.File#deleteOnExit()
   */
  public static final File createLocalTempFile(final File basefile,
                                               final String prefix,
                                               final boolean isDeleteOnExit)
    throws IOException {
    File tmp = File.createTempFile(prefix + basefile.getName(),
                                   "", basefile.getParentFile());
    if (isDeleteOnExit) {
      tmp.deleteOnExit();
    }
    return tmp;
  }

  /**
   * Return an array of FileStatus objects for each file beneath
   * the given path to a specified depth (inclusive). Directories at
   * the given depth will be returned, while directories above that
   * depth will not.
   * @param fs FileSystem on which to operate
   * @param f starting path
   * @param depth the depth of recursion. If 0 the function will
   *        return the status of the current file or directory.
   * @exception IOException If this operation fails
   */
  public static FileStatus[] listStatus(FileSystem fs, Path f, int depth)
      throws IOException {
    if (null == f) {
      throw new IllegalArgumentException("Path cannot be null.");
    }
    if (null == fs) {
      throw new IllegalArgumentException("File system cannot be null.");
    }
    if (depth < 0) {
      throw new IllegalArgumentException("Depth cannot be negative.");
    }

    if (depth == 0) {
      FileStatus[] fileStatus = {fs.getFileStatus(f)};
      return fileStatus;
    }

    List<FileStatus> fileStatuses = listStatusHelper(fs, f, depth,
        new ArrayList<FileStatus>());
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  /**
   * Core logic for listStatus
   */
  public static List<FileStatus> listStatusHelper(FileSystem fs, Path path,
      int depth, List<FileStatus> acc) throws IOException {
    FileStatus[] fileStatusResults = fs.listStatus(path);
    if (fileStatusResults == null) {
      throw new IOException("Path does not exist: " + path);
    }

    for (FileStatus f : fileStatusResults) {
      Path subPath = f.getPath();
      if (!f.isDir()) {
        acc.add(f); // Accumulate all files
      } else {
        if (depth > 1) {
          listStatusHelper(fs, subPath, depth - 1, acc);
        } else {
          acc.add(f); // Accumulate all leaves
        }
      }
    }

    return acc;
  }

  /**
   * pass in a directory path, get the list of statuses of leaf directories
   * @param fs file system
   * @param path path to scan
   * @param acc the collection of file status of leaf directories
   * @throws IOException if any error occurs
   */
  public static void listStatusForLeafDir(FileSystem fs,
      FileStatus pathStatus,
      List<FileStatus> acc) throws IOException {
    if (!pathStatus.isDir())
      return;
    FileStatus[] fileStatusResults = fs.listStatus(pathStatus.getPath());
    if (fileStatusResults == null) {
      throw new IOException("Path does not exist: " + pathStatus.getPath());
    }

    boolean leafDir = true;
    for (FileStatus f : fileStatusResults) {
      if (f.isDir()) {
        leafDir = false;
        listStatusForLeafDir(fs, f, acc);
      }
    }
    if (leafDir) {
      acc.add(pathStatus); // Accumulate leaf dir
    }
  }
  
  /**
   * A wrapper for {@link File#listFiles()}. This java.io API returns null when
   * a dir is not a directory or for any I/O error. Instead of having null check
   * everywhere File#listFiles() is used, we will add utility API to get around
   * this problem. For the majority of cases where we prefer an IOException to
   * be thrown.
   * 
   * @param dir  directory for which listing should be performed
   * @return list of files or empty list
   * @exception IOException
   *              for invalid directory or for a bad disk.
   */
  public static File[] listFiles(File dir) throws IOException {
    File[] files = dir.listFiles();
    if (files == null) {
      throw new IOException("Invalid directory or I/O error occurred for dir: "
          + dir.toString());
    }
    return files;
  }

  /**
   * Move the src file to the name specified by target.
   * @param src the source file
   * @param target the target file
   * @exception IOException If this operation fails
   */
  public static void replaceFile(File src, File target) throws IOException {
    /* renameTo() has two limitations on Windows platform.
     * src.renameTo(target) fails if
     * 1) If target already exists OR
     * 2) If target is already open for reading/writing.
     */
    if (!src.renameTo(target)) {
      int retries = 5;
      while (target.exists() && !target.delete() && retries-- >= 0) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new IOException("replaceFile interrupted.");
        }
      }
      if (!src.renameTo(target)) {
        throw new IOException("Unable to rename " + src +
                              " to " + target);
      }
    }
  }
}
