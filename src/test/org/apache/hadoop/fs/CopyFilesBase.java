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

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.log4j.Level;
/**
 * A JUnit test for copying files recursively.
 */
public class CopyFilesBase {
  {
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.StateChange")
        ).getLogger().setLevel(Level.OFF);
    DataNode.LOG.getLogger().setLevel(Level.OFF);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)DistCp.LOG).getLogger().setLevel(Level.ALL);
  }
  
  public static final Log LOG = LogFactory.getLog(CopyFilesBase.class);
  protected static final URI LOCAL_FS = URI.create("file:///");
  
  protected static final Random RAN = new Random();
  protected static final int NFILES = 20;
  protected static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');

  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  public static class MyFile {
    private static Random gen = new Random();
    private static final int MAX_LEVELS = 3;
    private static final int MAX_SIZE = 8*1024;
    private static String[] dirNames = {
      "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
    };
    private final String name;
    private int size = 0;
    private long seed = 0L;

    MyFile() {
      this(gen.nextInt(MAX_LEVELS));
    }
    MyFile(int nLevels) {
      String xname = "";
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        xname = sb.toString();
      }
      long fidx = gen.nextLong() & Long.MAX_VALUE;
      name = xname + Long.toString(fidx);
      reset();
    }
    void reset() {
      final int oldsize = size;
      do { size = gen.nextInt(MAX_SIZE); } while (oldsize == size);
      final long oldseed = seed;
      do { seed = gen.nextLong() & Long.MAX_VALUE; } while (oldseed == seed);
    }
    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }

  static MyFile[] createFiles(URI fsname, String topdir)
    throws IOException {
    return createFiles(FileSystem.get(fsname, new Configuration()), topdir);
  }

  /** create NFILES with random names and directory hierarchies
   * with random (but reproducible) data in them.
   */
  protected static MyFile[] createFiles(FileSystem fs, String topdir)
    throws IOException {
    Path root = new Path(topdir);
    MyFile[] files = new MyFile[NFILES];
    for (int i = 0; i < NFILES; i++) {
      files[i] = createFile(root, fs);
    }
    return files;
  }

  static MyFile createFile(Path root, FileSystem fs, int levels)
      throws IOException {
    MyFile f = levels < 0 ? new MyFile() : new MyFile(levels);
    Path p = new Path(root, f.getName());
    byte[] toWrite = new byte[f.getSize()];
    new Random(f.getSeed()).nextBytes(toWrite);
    createFileWithContent(fs, p, toWrite);
    FileSystem.LOG.info("created: " + p + ", size=" + f.getSize());
    return f;
  }

  static MyFile createFile(Path root, FileSystem fs) throws IOException {
    return createFile(root, fs, -1);
  }

  static boolean checkFiles(FileSystem fs, String topdir, MyFile[] files
      ) throws IOException {
    return checkFiles(fs, topdir, files, false);    
  }

  public static boolean checkContentOfFile(FileSystem fs, Path filePath, byte[] content)
      throws IOException {
    FSDataInputStream in = null;
    try {
      FileStatus fileStatus = fs.getFileStatus(filePath);
      if (fileStatus.getLen() != content.length) {
        return false;
      }
      in = fs.open(filePath);
      byte[] toRead = new byte[content.length];
      in.readFully(toRead);
      for (int i = 0; i < content.length; ++i) {
        if (content[i] != toRead[i]) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      LOG.warn("Get exception in checkContentOfFile.", e);
      throw e;
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  protected static boolean checkFiles(FileSystem fs, String topdir, MyFile[] files,
      boolean existingOnly) throws IOException {
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < files.length; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      try {
        byte[] toCompare = new byte[files[idx].getSize()];
        Random rb = new Random(files[idx].getSeed());
        rb.nextBytes(toCompare);
        if (!checkContentOfFile(fs, fPath, toCompare)) {
          return false;
        }
      }
      catch(FileNotFoundException fnfe) {
        if (!existingOnly) {
          throw fnfe;
        }
      }
      catch(EOFException eofe){
        throw (EOFException)new EOFException("Cannot read file" + fPath );
      }
    }
    
    return true;
  }

  protected static void updateFiles(FileSystem fs, String topdir, MyFile[] files,
        int nupdate) throws IOException {
    assert nupdate <= NFILES;

    Path root = new Path(topdir);

    for (int idx = 0; idx < nupdate; ++idx) {
      Path fPath = new Path(root, files[idx].getName());
      // overwrite file
      assertTrue(fPath.toString() + " does not exist", fs.exists(fPath));
      FSDataOutputStream out = fs.create(fPath);
      files[idx].reset();
      byte[] toWrite = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toWrite);
      out.write(toWrite);
      out.close();
    }
  }

  protected static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, MyFile[] files) throws IOException {
    return getFileStatus(fs, topdir, files, false);
  }
  
  protected static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, MyFile[] files, boolean existingOnly) throws IOException {
    Path root = new Path(topdir);
    List<FileStatus> statuses = new ArrayList<FileStatus>();
    for (int idx = 0; idx < NFILES; ++idx) {
      try {
        statuses.add(fs.getFileStatus(new Path(root, files[idx].getName())));
      } catch(FileNotFoundException fnfe) {
        if (!existingOnly) {
          throw fnfe;
        }
      }
    }
    return statuses.toArray(new FileStatus[statuses.size()]);
  }

  protected static boolean checkUpdate(FileSystem fs, FileStatus[] old,
      String topdir, MyFile[] upd, final int nupdate) throws IOException {
    Path root = new Path(topdir);

    // overwrote updated files
    for (int idx = 0; idx < nupdate; ++idx) {
      final FileStatus stat =
        fs.getFileStatus(new Path(root, upd[idx].getName()));
      if (stat.getModificationTime() <= old[idx].getModificationTime()) {
        return false;
      }
    }
    // did not overwrite files not updated
    for (int idx = nupdate; idx < NFILES; ++idx) {
      final FileStatus stat =
        fs.getFileStatus(new Path(root, upd[idx].getName()));
      if (stat.getModificationTime() != old[idx].getModificationTime()) {
        return false;
      }
    }
    return true;
  }

  /** delete directory and everything underneath it.*/
  protected static void deldir(FileSystem fs, String topdir) throws IOException {
    fs.delete(new Path(topdir), true);
  }

  static final long now = System.currentTimeMillis();

  static UnixUserGroupInformation createUGI(String name, boolean issuper) {
    String username = name + now;
    String group = issuper? "supergroup": username;
    return UnixUserGroupInformation.createImmutable(
        new String[]{username, group});
  }

  static Path createHomeDirectory(FileSystem fs, UserGroupInformation ugi
      ) throws IOException {
    final Path home = new Path("/user/" + ugi.getUserName());
    fs.mkdirs(home);
    fs.setOwner(home, ugi.getUserName(), ugi.getGroupNames()[0]);
    fs.setPermission(home, new FsPermission((short)0700));
    return home;
  }

  public static void createFileWithContent(FileSystem fs, Path filePath, byte[] content)
      throws IOException {
    FSDataOutputStream out = fs.create(filePath);
    try {
      out.write(content);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  protected static void create(FileSystem fs, Path f) throws IOException {
    byte[] b = new byte[1024 + RAN.nextInt(1024)];
    RAN.nextBytes(b);
    createFileWithContent(fs, f, b);
  }
  
  protected static String execCmd(FsShell shell, String... args) throws Exception {
    ByteArrayOutputStream baout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baout, true);
    PrintStream old = System.out;
    System.setOut(out);
    shell.run(args);
    out.close();
    System.setOut(old);
    return baout.toString();
  }
  
  protected static String removePrefix(String lines, String prefix) {
    final int prefixlen = prefix.length();
    final StringTokenizer t = new StringTokenizer(lines, "\n");
    final StringBuffer results = new StringBuffer(); 
    for(; t.hasMoreTokens(); ) {
      String s = t.nextToken();
      results.append(s.substring(s.indexOf(prefix) + prefixlen) + "\n");
    }
    return results.toString();
  }
}
