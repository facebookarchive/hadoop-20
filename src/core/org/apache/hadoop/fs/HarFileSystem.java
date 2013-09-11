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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Progressable;

/**
 * This is an implementation of the Hadoop Archive
 * Filesystem. This archive Filesystem has index files
 * of the form _index* and has contents of the form
 * part-*. The index files store the indexes of the
 * real files. The index files are of the form _masterindex
 * and _index. The master index is a level of indirection
 * in to the index file to make the look ups faster. the index
 * file is sorted with hash code of the paths that it contains
 * and the master index contains pointers to the positions in
 * index for ranges of hashcodes.
 * 
 * More exactly, the data is stored as following.
 * foo.har/  - directory in hdfs
 *   |----   _masterindex 
 *   |----   _index
 *   |----   part-0
 *   |----   part-1
 *   |----   .....
 *   -----   part-n
 *   
 * "_index" file has the number of lines equal to the number of files and directories in archive,
 * including top-level root dir.
 * Let @path be the relative path to item in archive.
 *     example: if we have files A.txt and B/C.txt  in archive 
 *     then the @paths would be: /A.txt and /B/C.txt.
 * Relative path of the root dir in archive is "/".
 * Each line consists of the following space separated fields.
 * 1. encodedPath - path to the item URL-encoded as URLEncoder.encode(@path, "UTF-8")
 * 2. type - type of item in archive. Equals to "dir" or "file"
 * 3. partFileName - name of part file which stores the actual data of file
 *        (for directories equals to "none")
 * 4. offset - start byte of file  (0 for directories)
 * 5. len - length in bytes of file (0 for directories)

 * for files:
 * 6. encodedProperty - URL-encoded property string of the file
 * 
 * property string consists of the following space separated fields
 *    a. modification time
 *    b. access time
 *    c. permission as short
 *    d. owner (URL-encoded)
 *    e. group (URL-encoded)
 *    
 * for directories:
 * 6. ecnodedChildName - URL-encoded name of child 1
 * 7. encodedChildName - URL-encoded name of child 2
 * 
 * Field 3 for directories could contain encodedProperty instead of "none".
 * The lines in "_index" are sorted by HarFileSystem.getHarHash(@path) key. 
 * 
 * "_masterindex".
 * First line stores VERSION
 * "_index" is divided to blocks, each containing 1000 lines (maybe except the last one)
 * For each block there is a line in "_masterindex".
 * The line consisted of the following space separated fields
 * 1. startHash - @hash field of the first line in block  
 * 2. endHash - @hash field of the last line in block
 * 3. startPos - position (in bytes) of the beginning of the block
 * 4. endPos - position (in bytes) of the end of the block
 * 
 * 
 */

public class HarFileSystem extends FilterFileSystem {
  public static final int VERSION = 2;

  /** name of the index file **/
  public static final String INDEX_NAME = "_index";
  
  /** name of the master index file **/
  public static final String MASTER_INDEX_NAME = "_masterindex";
  
  // uri representation of this Har filesystem
  private URI uri;
  // the version of this har filesystem
  private int version;
  // underlying uri
  private URI underLyingURI;
  // the top level path of the archive
  // in the underlying file system
  private Path archivePath;
  // the masterIndex of the archive
  private Path masterIndex;
  // the index file
  private Path archiveIndex;
  // the har auth
  private String harAuth;

  /**
   * public construction of harfilesystem
   *
   */
  public HarFileSystem() {
  }

  /**
   * Constructor to create a HarFileSystem with an
   * underlying filesystem.
   * @param fs
   */
  public HarFileSystem(FileSystem fs) {
    super(fs);
  }

  /**
   * Initialize a Har filesystem per har archive. The
   * archive home directory is the top level directory
   * in the filesystem that contains the HAR archive.
   * Be careful with this method, you do not want to go
   * on creating new Filesystem instances per call to
   * path.getFileSystem().
   * the uri of Har is
   * har://underlyingfsscheme-host:port/archivepath.
   * or
   * har:///archivepath. This assumes the underlying filesystem
   * to be used in case not specified.
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    //decode the name
    underLyingURI = decodeHarURI(name, conf);
    //  we got the right har Path- now check if this is
    //truly a har filesystem
    Path harPath = archivePath(
      new Path(name.getScheme(), name.getAuthority(), name.getPath()));
    if (harPath == null) {
      throw new IOException("Invalid path for the Har Filesystem. " +
                           name.toString());
    }
    if (fs == null) {
      fs = FileSystem.get(underLyingURI, conf);
    }
    this.uri = harPath.toUri();
    this.archivePath = new Path(this.uri.getPath());
    this.harAuth = getHarAuth(this.underLyingURI);
    //check for the underlying fs containing
    // the index file
    this.masterIndex = new Path(archivePath, HarFileSystem.MASTER_INDEX_NAME);
    this.archiveIndex = new Path(archivePath, HarFileSystem.INDEX_NAME);
    if (!fs.exists(masterIndex) || !fs.exists(archiveIndex)) {
      throw new IOException("Invalid path for the Har Filesystem. " +
          "No index file in " + harPath);
    }
    try{
      this.version = getHarVersion();
    } catch(IOException io) {
      throw new IOException("Unable to " +
          "read the version of the Har file system: " + this.archivePath);
    }
    // make it always backwards-compatible
    if (this.version > HarFileSystem.VERSION) {
      throw new IOException("Invalid version " +
          this.version + " expected " + HarFileSystem.VERSION);
    }
  }

  // get the version of the filesystem from the masterindex file
  // the version is currently not useful since its the first version
  // of archives
  public int getHarVersion() throws IOException {
    FSDataInputStream masterIn = fs.open(masterIndex);
    LineReader lmaster = new LineReader(masterIn, getConf());
    Text line = new Text();
    lmaster.readLine(line);
    try {
      masterIn.close();
    } catch(IOException e){
      //disregard it.
      // its a read.
    }
    String versionLine = line.toString();
    String[] arr = versionLine.split(" ");
    int version = Integer.parseInt(arr[0]);
    return version;
  }

  /*
   * find the parent path that is the
   * archive path in the path. The last
   * path segment that ends with .har is
   * the path that will be returned.
   */
  private Path archivePath(Path p) {
    Path retPath = null;
    Path tmp = p;
    for (int i=0; i< p.depth(); i++) {
      if (tmp.toString().endsWith(".har")) {
        retPath = tmp;
        break;
      }
      tmp = tmp.getParent();
    }
    return retPath;
  }

  /**
   * decode the raw URI to get the underlying URI
   * @param rawURI raw Har URI
   * @return filtered URI of the underlying fileSystem
   */
  private URI decodeHarURI(URI rawURI, Configuration conf) throws IOException {
    String tmpAuth = rawURI.getAuthority();
    //we are using the default file
    //system in the config
    //so create a underlying uri and
    //return it
    if (tmpAuth == null) {
      //create a path
      return FileSystem.getDefaultUri(conf);
    }
    String host = rawURI.getHost();
    String[] str = host.split("-", 2);
    if (str[0] == null) {
      throw new IOException("URI: " + rawURI + " is an invalid Har URI.");
    }
    String underLyingScheme = str[0];
    String underLyingHost = (str.length > 1)? str[1]:null;
    int underLyingPort = rawURI.getPort();
    String auth = (underLyingHost == null && underLyingPort == -1)?
                  null:(underLyingHost+":"+underLyingPort);
    URI tmp = null;
    if (rawURI.getQuery() != null) {
      // query component not allowed
      throw new IOException("query component in Path not supported  " + rawURI);
    }
    try {
      tmp = new URI(underLyingScheme, auth, rawURI.getPath(),
            rawURI.getQuery(), rawURI.getFragment());
    } catch (URISyntaxException e) {
        // do nothing should not happen
    }
    return tmp;
  }

  private String decodeFileName(String fname) throws UnsupportedEncodingException {
    return decodeFileName(fname, version);
  }
  
  static String decodeFileName(String fname, int archiveVersion)
    throws UnsupportedEncodingException {

    if (archiveVersion >= 2){
      return URLDecoder.decode(fname, "UTF-8");
    }
    return fname;
  }

  /**
   * return the top level archive.
   */
  public Path getWorkingDirectory() {
    return new Path(uri.toString());
  }

  /**
   * Create a har specific auth
   * har-underlyingfs:port
   * @param underLyingURI the uri of underlying
   * filesystem
   * @return har specific auth
   */
  private String getHarAuth(URI underLyingUri) {
    String auth = underLyingUri.getScheme() + "-";
    if (underLyingUri.getHost() != null) {
      auth += underLyingUri.getHost() + ":";
      if (underLyingUri.getPort() != -1) {
        auth +=  underLyingUri.getPort();
      }
    }
    else {
      auth += ":";
    }
    return auth;
  }

  /**
   * Returns the uri of this filesystem.
   * The uri is of the form
   * har://underlyingfsschema-host:port/pathintheunderlyingfs
   */
  @Override
  public URI getUri() {
    return this.uri;
  }

  /**
   * this method returns the path
   * inside the har filesystem.
   * this is relative path inside
   * the har filesystem.
   * @param path the fully qualified path in the har filesystem.
   * @return relative path in the filesystem.
   */
  public Path getPathInHar(Path path) {
    Path harPath = new Path(path.toUri().getPath());
    if (archivePath.compareTo(harPath) == 0)
      return new Path(Path.SEPARATOR);
    Path tmp = new Path(harPath.getName());
    Path parent = harPath.getParent();
    while (!(parent.compareTo(archivePath) == 0)) {
      if (parent.toString().equals(Path.SEPARATOR)) {
        tmp = null;
        break;
      }
      tmp = new Path(parent.getName(), tmp);
      parent = parent.getParent();
    }
    if (tmp != null)
      tmp = new Path(Path.SEPARATOR, tmp);
    return tmp;
  }

  //the relative path of p. basically
  // getting rid of /. Parsing and doing
  // string manipulation is not good - so
  // just use the path api to do it.
  private Path makeRelative(String initial, Path p) {
    String scheme = this.uri.getScheme();
    String authority = this.uri.getAuthority();
    Path root = new Path(Path.SEPARATOR);
    if (root.compareTo(p) == 0)
      return new Path(scheme, authority, initial);
    Path retPath = new Path(p.getName());
    Path parent = p.getParent();
    for (int i=0; i < p.depth()-1; i++) {
      retPath = new Path(parent.getName(), retPath);
      parent = parent.getParent();
    }
    return new Path(new Path(scheme, authority, initial),
      retPath.toString());
  }

  /* this makes a path qualified in the har filesystem
   * (non-Javadoc)
   * @see org.apache.hadoop.fs.FilterFileSystem#makeQualified(
   * org.apache.hadoop.fs.Path)
   */
  @Override
  public Path makeQualified(Path path) {
    // make sure that we just get the
    // path component
    Path fsPath = path;
    if (!path.isAbsolute()) {
      fsPath = new Path(archivePath, path);
    }

    URI tmpURI = fsPath.toUri();
    //change this to Har uri
    return new Path(uri.getScheme(), harAuth, tmpURI.getPath());
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    // HarFileSystem has a bug where this method does not work properly
    // if the underlying FS is HDFS. See MAPREDUCE-1877 for more
    // information. This method is from FileSystem.
    FileStatus status = getFileStatus(f);
    if (!status.isDir()) {
      // f is a file
      return new ContentSummary(status.getLen(), 1, 0);
    }
    // f is a directory
    long[] summary = {0, 0, 1};
    for(FileStatus s : listStatus(f)) {
      ContentSummary c = s.isDir() ? getContentSummary(s.getPath()) :
                                     new ContentSummary(s.getLen(), 1, 0);
      summary[0] += c.getLength();
      summary[1] += c.getFileCount();
      summary[2] += c.getDirectoryCount();
    }
    return new ContentSummary(summary[0], summary[1], summary[2]);
  }

  /**
   * Fix offset and length of block locations.
   * Note that this method modifies the original array.
   * @param locations block locations of har part file
   * @param start the start of the desired range in the contained file
   * @param len the length of the desired range
   * @param fileOffsetInHar the offset of the desired file in the har part file
   * @return block locations with fixed offset and length
   */  
  static BlockLocation[] fixBlockLocations(BlockLocation[] locations,
                                           long start,
                                           long len,
                                           long fileOffsetInHar) {
    // offset 1 past last byte of desired range
    long end = start + len;
    
    for (BlockLocation location : locations) {
      // offset of part block relative to beginning of desired file
      // (may be negative if file starts in this part block)
      long harBlockStart = location.getOffset() - fileOffsetInHar;
      // offset 1 past last byte of har block relative to beginning of
      // desired file
      long harBlockEnd = harBlockStart + location.getLength();
      
      if (start > harBlockStart) {
        // desired range starts after beginning of this har block
        // fix offset to beginning of relevant range (relative to desired file)
        location.setOffset(start);
        // fix length to relevant portion of har block
        location.setLength(location.getLength() - (start - harBlockStart));
      } else {
        // desired range includes beginning of this har block
        location.setOffset(harBlockStart);
      }
      
      if (harBlockEnd > end) {
        // range ends before end of this har block
        // fix length to remove irrelevant portion at the end
        location.setLength(location.getLength() - (harBlockEnd - end));
      }
    }
    
    return locations;
  }


  /**
   * Get block locations from the underlying fs and fix their
   * offsets and lengths.
   * @param file the input filestatus to get block locations
   * @param start the start of the desired range in the contained file
   * @param len the length of the desired range
   * @return block locations for this segment of file
   * @throws IOException
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
                                               long len) throws IOException {
    HarStatus hstatus = getFileHarStatus(file.getPath(), null);
    Path partPath = new Path(archivePath, hstatus.getPartName());
    FileStatus partStatus = fs.getFileStatus(partPath);

    // get all part blocks that overlap with the desired file blocks
    BlockLocation[] locations = 
      fs.getFileBlockLocations(partStatus,
                               hstatus.getStartIndex() + start, len);

    return fixBlockLocations(locations, start, len, hstatus.getStartIndex());
  }

  /**
   * the hash of the path p inside inside
   * the archive
   * @param p the path in the HarFileSystem
   * @return the hash code of the path.
   */
  public static int getHarHash(String p) {
    return (p.hashCode() & 0x7fffffff);
  }

  static class Store {
    public Store() {
      begin = end = startHash = endHash = 0;
    }
    public Store(long begin, long end, int startHash, int endHash) {
      this.begin = begin;
      this.end = end;
      this.startHash = startHash;
      this.endHash = endHash;
    }
    public long begin;
    public long end;
    public int startHash;
    public int endHash;
  }

  static public String decode(String string) throws UnsupportedEncodingException {
    return URLDecoder.decode(string, "UTF-8");
  }

  static public String encode(String string) throws UnsupportedEncodingException {
    return URLEncoder.encode(string, "UTF-8");
  }
  
  /**
   * Get filestatuses of all the children of a given directory. This just reads
   * through index file and reads line by line to get all statuses for children
   * of a directory. Its a brute force way of getting all such filestatuses
   *
   * @param parent
   *          the parent path directory
   * @param statuses
   *          the list to add the children filestatuses to
   * @param children
   *          the string list of children for this parent
   * @param archiveIndexStat
   *          the archive index filestatus
   */
  private void fileStatusesInIndex(HarStatus parent, List<FileStatus> statuses,
      List<String> children, FileStatus archiveIndexStat) throws IOException {
    // read the index file
    FSDataInputStream aIn = null;
    try {
      aIn = fs.open(archiveIndex);
      LineReader aLin;
      long read = 0;
      aLin = new LineReader(aIn, getConf());
      String parentString = parent.getName();
      if (!parentString.endsWith(Path.SEPARATOR)){
          parentString += Path.SEPARATOR;
      }
      Path harPath = new Path(parentString);
      int harlen = harPath.depth();
      Text line = new Text();
      while (read < archiveIndexStat.getLen()) {
        int tmp = aLin.readLine(line);
        read += tmp;
        String lineFeed = line.toString();
        String child = decodeFileName(lineFeed.substring(0, lineFeed.indexOf(" ")));
        if ((child.startsWith(parentString))) {
          Path thisPath = new Path(child);
          if (thisPath.depth() == harlen + 1) {
            // bingo!
            HarStatus hstatus = new HarStatus(lineFeed);
            FileStatus childStatus = new FileStatus(hstatus.isDir() ? 0
                : hstatus.getLength(), hstatus.isDir(), (int) archiveIndexStat
                .getReplication(), archiveIndexStat.getBlockSize(),
                hstatus.getModificationTime(), hstatus
                    .getAccessTime(), new FsPermission(hstatus
                    .getPermission()), hstatus.getOwner(),
                hstatus.getGroup(), makeRelative(this.uri.getPath(),
                    new Path(hstatus.getName())));
            statuses.add(childStatus);
          }
          line.clear();
        }
      }
    } finally {
      if (aIn != null) {
        aIn.close();
      }
    }
  }

  // make sure that this harPath is relative to the har filesystem
  // this only works for relative paths. This returns the line matching
  // the file in the index. Returns a null if there is not matching
  // filename in the index file.
  private String fileStatusInIndex(Path harPath) throws IOException {
    // read the index file
    int hashCode = getHarHash(harPath.toString());
    // get the master index to find the pos
    // in the index file
    FSDataInputStream in = fs.open(masterIndex);
    FileStatus masterStat = fs.getFileStatus(masterIndex);
    LineReader lin = new LineReader(in, getConf());
    Text line = new Text();
    long read = lin.readLine(line);
   //ignore the first line. this is the header of the index files
    String[] readStr = null;
    List<Store> stores = new ArrayList<Store>();
    while(read < masterStat.getLen()) {
      int b = lin.readLine(line);
      read += b;
      readStr = line.toString().split(" ");
      int startHash = Integer.parseInt(readStr[0]);
      int endHash  = Integer.parseInt(readStr[1]);
      if (startHash <= hashCode && hashCode <= endHash) {
        stores.add(new Store(Long.parseLong(readStr[2]),
            Long.parseLong(readStr[3]), startHash,
            endHash));
      }
      line.clear();
    }
    try {
      lin.close();
    } catch(IOException io){
      // do nothing just a read.
    }
    FSDataInputStream aIn = fs.open(archiveIndex);
    LineReader aLin;
    String retStr = null;
    // now start reading the real index file
    for (Store s: stores) {
      read = 0;
      aIn.seek(s.begin);
      aLin = new LineReader(aIn, getConf());
      while (read + s.begin < s.end) {
        int tmp = aLin.readLine(line);
        read += tmp;
        String lineFeed = line.toString();
        String[] parsed = lineFeed.split(" ");
        parsed[0] = decodeFileName(parsed[0]);
        if (harPath.compareTo(new Path(parsed[0])) == 0) {
          // bingo!
          retStr = lineFeed;
          break;
        }
        line.clear();
      }
      if (retStr != null)
        break;
    }
    try {
      aIn.close();
    } catch(IOException io) {
      //do nothing
    }
    return retStr;
  }

  /**
   * return the filestatus of files in har archive.
   * The permission returned are that of the archive
   * index files. The permissions are not persisted
   * while creating a hadoop archive.
   * @param f the path in har filesystem
   * @return filestatus.
   * @throws IOException
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus archiveStatus = fs.getFileStatus(archiveIndex);
    HarStatus hstatus = getFileHarStatus(f, archiveStatus);
    return new FileStatus(hstatus.isDir()?0:hstatus.getLength(), hstatus.isDir(),
        (int)archiveStatus.getReplication(), archiveStatus.getBlockSize(),
        hstatus.getModificationTime(), hstatus.getAccessTime(),
        new FsPermission(
        hstatus.getPermission()), hstatus.getOwner(),
        hstatus.getGroup(),
            makeRelative(this.uri.getPath(), new Path(hstatus.getName())));
  }
  
  private HarStatus getFileHarStatus(Path f, FileStatus underlying) throws IOException {
    Path p = makeQualified(f);
    Path harPath = getPathInHar(p);
    if (harPath == null) {
      throw new IOException("Invalid file name: " + f + " in " + uri);
    }
    String readStr = fileStatusInIndex(harPath);
    if (readStr == null) {
      throw new FileNotFoundException(f + ": not found in " + archivePath);
    }
    return new HarStatus(readStr, underlying, version);
  }

  /**
   * Returns a har input stream which fakes end of
   * file. It reads the index files to get the part
   * file name and the size and start of the file.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    HarStatus hstatus = getFileHarStatus(f, null);
    // we got it.. woo hooo!!!
    if (hstatus.isDir()) {
      throw new FileNotFoundException(f + " : not a file in " +
                archivePath);
    }
    return new HarFSDataInputStream(fs, new Path(archivePath,
        hstatus.getPartName()),
        hstatus.getStartIndex(), hstatus.getLength(), bufferSize);
  }

  /*
   * create throws an exception in Har filesystem.
   * The archive once created cannot be changed.
   */
  public FSDataOutputStream create(Path f, int bufferSize)
                                    throws IOException {
    throw new IOException("Har: Create not allowed");
  }

  public FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    throw new IOException("Har: create not allowed.");
  }

  @Override
  public void close() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch(IOException ie) {
        //this might already be closed
        // ignore
      }
    }
  }

  /**
   * Not implemented.
   */
  @Override
  public boolean setReplication(Path src, short replication) throws IOException{
    throw new IOException("Har: setreplication not allowed");
  }

  /**
   * Not implemented.
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new IOException("Har: delete not allowed");
  }

  /**
   * liststatus returns the children of a directory
   * after looking up the index files.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    //need to see if the file is an index in file
    //get the filestatus of the archive directory
    // we will create fake filestatuses to return
    // to the client
    List<FileStatus> statuses = new ArrayList<FileStatus>();
    FileStatus archiveStatus = fs.getFileStatus(archiveIndex);
    Path tmpPath = makeQualified(f);
    Path harPath = getPathInHar(tmpPath);
    String readStr = fileStatusInIndex(harPath);
    if (readStr == null) {
      throw new FileNotFoundException("File " + f + " not found in " + archivePath);
    }
    HarStatus hstatus = new HarStatus(readStr,archiveStatus, version);
    if (!hstatus.isDir()) {
        statuses.add(new FileStatus(hstatus.getLength(),
            hstatus.isDir(),
            archiveStatus.getReplication(), archiveStatus.getBlockSize(),
            hstatus.getModificationTime(), hstatus.getAccessTime(),
            new FsPermission(hstatus.getPermission()),
            hstatus.getOwner(), hstatus.getGroup(),
            makeRelative(this.uri.getPath(), new Path(hstatus.getName()))));
    } else {
      fileStatusesInIndex(hstatus, statuses, hstatus.getChildren(), archiveStatus);
    }
    return statuses.toArray(new FileStatus[statuses.size()]);
  }

  /**
   * returns the archive index
   */
  public Path getArchiveIndex() {
    return archiveIndex;
  }

  /**
   * return the top level archive path.
   */
  public Path getHomeDirectory() {
    return new Path(uri.toString());
  }

  public void setWorkingDirectory(Path newDir) {
    //does nothing.
  }

  /**
   * not implemented.
   */
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new IOException("Har: mkdirs not allowed");
  }

  /**
   * not implemented.
   */
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws
        IOException {
    throw new IOException("Har: copyfromlocalfile not allowed");
  }

  /**
   * copies the file in the har filesystem to a local file.
   */
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    FileUtil.copy(this, src, getLocal(getConf()), dst, false, getConf());
  }

  /**
   * not implemented.
   */
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    throw new IOException("Har: startLocalOutput not allowed");
  }

  /**
   * not implemented.
   */
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    throw new IOException("Har: completeLocalOutput not allowed");
  }

  /**
   * not implemented.
   */
  public void setOwner(Path p, String username, String groupname)
    throws IOException {
    throw new IOException("Har: setowner not allowed");
  }

  /**
   * Not implemented.
   */
  public void setPermission(Path p, FsPermission permisssion)
    throws IOException {
    throw new IOException("Har: setPermission not allowed");
  }

  /**
   * Hadoop archives input stream. This input stream fakes EOF
   * since archive files are part of bigger part files.
   */
  public static class HarFSDataInputStream extends FSDataInputStream {
    /**
     * Create an input stream that fakes all the reads/positions/seeking.
     */
    private static class HarFsInputStream extends FSInputStream {
      private long position, start, end;
      //The underlying data input stream that the
      // underlying filesystem will return.
      private FSDataInputStream underLyingStream;
      //one byte buffer
      private byte[] oneBytebuff = new byte[1];
      HarFsInputStream(FileSystem fs, Path path, long start,
          long length, int bufferSize) throws IOException {
        underLyingStream = fs.open(path, bufferSize);
        underLyingStream.seek(start);
        // the start of this file in the part file
        this.start = start;
        // the position pointer in the part file
        this.position = start;
        // the end pointer in the part file
        this.end = start + length;
      }

      public synchronized int available() throws IOException {
        long remaining = end - underLyingStream.getPos();
        if (remaining > (long)Integer.MAX_VALUE) {
          return Integer.MAX_VALUE;
        }
        return (int) remaining;
      }

      public synchronized  void close() throws IOException {
        underLyingStream.close();
        super.close();
      }

      //not implemented
      @Override
      public void mark(int readLimit) {
        // do nothing
      }

      /**
       * reset is not implemented
       */
      public void reset() throws IOException {
        throw new IOException("reset not implemented.");
      }

      public synchronized int read() throws IOException {
        int ret = read(oneBytebuff, 0, 1);
        return (ret <= 0) ? -1: (oneBytebuff[0] & 0xff);
      }

      public synchronized int read(byte[] b) throws IOException {
        int ret = read(b, 0, b.length);
        if (ret != -1) {
          position += ret;
        }
        return ret;
      }

      /**
       *
       */
      public synchronized int read(byte[] b, int offset, int len)
        throws IOException {
        int newlen = len;
        int ret = -1;
        if (position + len > end) {
          newlen = (int) (end - position);
        }
        // end case
        if (newlen == 0)
          return ret;
        ret = underLyingStream.read(b, offset, newlen);
        position += ret;
        return ret;
      }

      public synchronized long skip(long n) throws IOException {
        long tmpN = n;
        if (tmpN > 0) {
          if (position + tmpN > end) {
            tmpN = end - position;
          }
          underLyingStream.seek(tmpN + position);
          position += tmpN;
          return tmpN;
        }
        return (tmpN < 0)? -1 : 0;
      }

      public synchronized long getPos() throws IOException {
        return (position - start);
      }

      public synchronized void seek(long pos) throws IOException {
        if (pos < 0 || (start + pos > end)) {
          throw new IOException("Failed to seek: EOF");
        }
        position = start + pos;
        underLyingStream.seek(position);
      }

      public boolean seekToNewSource(long targetPos) throws IOException {
        //do not need to implement this
        // hdfs in itself does seektonewsource
        // while reading.
        return false;
      }

      /**
       * implementing position readable.
       */
      public int read(long pos, byte[] b, int offset, int length)
      throws IOException {
        int nlength = length;
        if (start + nlength + pos > end) {
          nlength = (int) (end - (start + pos));
        }
        return underLyingStream.read(pos + start , b, offset, nlength);
      }

      /**
       * position readable again.
       */
      public void readFully(long pos, byte[] b, int offset, int length)
      throws IOException {
        if (start + length + pos > end) {
          throw new IOException("Not enough bytes to read.");
        }
        underLyingStream.readFully(pos + start, b, offset, length);
      }

      public void readFully(long pos, byte[] b) throws IOException {
          readFully(pos, b, 0, b.length);
      }

    }

    /**
     * constructors for har input stream.
     * @param fs the underlying filesystem
     * @param p The path in the underlying filesystem
     * @param start the start position in the part file
     * @param length the length of valid data in the part file
     * @param bufsize the buffer size
     * @throws IOException
     */
    public HarFSDataInputStream(FileSystem fs, Path  p, long start,
        long length, int bufsize) throws IOException {
        super(new HarFsInputStream(fs, p, start, length, bufsize));
    }

    /**
     * constructor for har input stream.
     * @param fs the underlying filesystem
     * @param p the path in the underlying file system
     * @param start the start position in the part file
     * @param length the length of valid data in the part file.
     * @throws IOException
     */
    public HarFSDataInputStream(FileSystem fs, Path  p, long start, long length)
      throws IOException {
        super(new HarFsInputStream(fs, p, start, length, 0));
    }
  }
}
