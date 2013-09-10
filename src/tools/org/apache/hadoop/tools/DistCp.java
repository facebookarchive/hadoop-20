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

package org.apache.hadoop.tools;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.tools.FastCopy;
import org.apache.hadoop.hdfs.tools.FastCopy.CopyResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Map-reduce program to recursively copy directories between
 * different file-systems.
 */
public class DistCp implements Tool {
  public static final Log LOG = LogFactory.getLog(DistCp.class);

  private static final String NAME = "distcp";
  private static final int DEFAULT_TOS_VALUE = 4;

  private static final String usage = NAME
    + " [OPTIONS] <srcurl>* <desturl>" +
    "\n\nOPTIONS:" +
    "\n-p[rbugp]              Preserve status" +
    "\n                       r: replication number" +
    "\n                       b: block size" +
    "\n                       u: user" + 
    "\n                       g: group" +
    "\n                       p: permission" +
    "\n                       t: modification and access times" +
    "\n                       -p alone is equivalent to -prbugpt" +
    "\n-i                     Ignore failures" +
    "\n-basedir <basedir>     Use <basedir> as the base directory when copying files from <srcurl>" +
    "\n-log <logdir>          Write logs to <logdir>" +
    "\n-m <num_maps>          Maximum number of simultaneous copies" +
    "\n-r <num_reducers>      Maximum number of reducers" +
    "\n-maxfiles <max_files_per_mapper>   Maximum number of files will be copied per mapper" + 
    "\n-maxfiles_r <max_files_per_reducer>   Maximum number of files will be renamed per reducer" +
    "\n-tos <tos_value>       The TOS value (valid values are from -1 to 191, inclusive)" +
    "\n-overwrite             Overwrite destination" +
    "\n-update                Overwrite if src size different from dst size" +
    "\n-skipcrccheck          Do not use CRC check to determine if src is " +
    "\n                       different from dest. Relevant only if -update" +
    "\n                       is specified" +
    "\n-copybychunk           Chop files in chunk and copy" +
    "\n-f <urilist_uri>       Use list at <urilist_uri> as src list" +
    "\n-filelimit <n>         Limit the total number of files to be <= n" +
    "\n-sizelimit <n>         Limit the total size to be <= n bytes" +
    "\n-delete                Delete the files existing in the dst but not in src" +
    "\n-mapredSslConf <f>     Filename of SSL configuration for mapper task" +
    "\n-usefastcopy           Use FastCopy (applicable to DFS only)" +
    "\n-skipunderconstruction Skip under construction files" +
    
    "\n\nNOTE 1: if -overwrite or -update are set, each source URI is " +
    "\n      interpreted as an isomorphic update to an existing directory." +
    "\nFor example:" +
    "\nhadoop " + NAME + " -p -update \"hdfs://A:8020/user/foo/bar\" " +
    "\"hdfs://B:8020/user/foo/baz\"\n" +
    "\n     would update all descendants of 'baz' also in 'bar'; it would " +
    "\n     *not* update /user/foo/baz/bar" + 

    "\n\nNOTE 2: The parameter <n> in -filelimit and -sizelimit can be " +
    "\n     specified with symbolic representation.  For examples," +
    "\n       1230k = 1230 * 1024 = 1259520" +
    "\n       891g = 891 * 1024^3 = 956703965184" +
    
    "\n";
  
  private static final long BYTES_PER_MAP =  256 * 1024 * 1024;
  private static final int MAX_MAPS_PER_NODE = 20;
  private static final int MAX_MAPS_DEFAULT = 4000;
  private static final int MAX_REDUCERS_DEFAULT = 1;
  private static final int SYNC_FILE_MAX = 10;
  // we will copy 1000 files per mapper at most.
  private static final int MAX_FILES_PER_MAPPER_DEFAULT = 1000;
  // we will rename 4000 files per reducer at most.
  private static final int MAX_FILES_PER_REDUCER_DEFAULT = 4000;
  private static final short SRC_FILES_LIST_REPL_DEFAULT = 10;

  static enum Counter {
    COPY, SKIP, FAIL, BYTESCOPIED, BYTESEXPECTED, BLOCKSCOPIED
  }

  public static enum Options {
    DELETE("-delete", NAME + ".delete"),
    FILE_LIMIT("-filelimit", NAME + ".limit.file"),
    SIZE_LIMIT("-sizelimit", NAME + ".limit.size"),
    IGNORE_READ_FAILURES("-i", NAME + ".ignore.read.failures"),
    PRESERVE_STATUS("-p", NAME + ".preserve.status"),
    OVERWRITE("-overwrite", NAME + ".overwrite.always"),
    UPDATE("-update", NAME + ".overwrite.ifnewer"),
    SKIPCRC("-skipcrccheck", NAME + ".skip.crc.check"),
    COPYBYCHUNK("-copybychunk", NAME + ".copy.by.chunk"),
    USEFASTCOPY("-usefastcopy", NAME + ".use.fastcopy"),
    SKIPUNDERCONSTRUCTION("-skipunderconstruction", 
                          NAME + ".skip.under.construction");

    final String cmd, propertyname;

    private Options(String cmd, String propertyname) {
      this.cmd = cmd;
      this.propertyname = propertyname;
    }
    
    private long parseLong(String[] args, int offset) {
      if (offset ==  args.length) {
        throw new IllegalArgumentException("<n> not specified in " + cmd);
      }
      long n = StringUtils.TraditionalBinaryPrefix.string2long(args[offset]);
      if (n <= 0) {
        throw new IllegalArgumentException("n = " + n + " <= 0 in " + cmd);
      }
      return n;
    }
  }
  static enum FileAttribute {
    BLOCK_SIZE, REPLICATION, USER, GROUP, PERMISSION, TIMES;

    final char symbol;

    private FileAttribute() {symbol = toString().toLowerCase().charAt(0);}
    
    static EnumSet<FileAttribute> parse(String s) {
      if (s == null || s.length() == 0) {
        return EnumSet.allOf(FileAttribute.class);
      }

      EnumSet<FileAttribute> set = EnumSet.noneOf(FileAttribute.class);
      FileAttribute[] attributes = values();
      for(char c : s.toCharArray()) {
        int i = 0;
        for(; i < attributes.length && c != attributes[i].symbol; i++);
        if (i < attributes.length) {
          if (!set.contains(attributes[i])) {
            set.add(attributes[i]);
          } else {
            throw new IllegalArgumentException("There are more than one '"
                + attributes[i].symbol + "' in " + s); 
          }
        } else {
          throw new IllegalArgumentException("'" + c + "' in " + s
              + " is undefined.");
        }
      }
      return set;
    }
  }

  static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
  static final String DST_DIR_LABEL = NAME + ".dest.path";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String MAX_MAPS_LABEL = NAME + ".max.map.tasks";
  static final String MAX_REDUCE_LABEL = NAME + ".max.reduce.tasks";
  static final String MAX_FILES_PER_MAPPER_LABEL = 
                                        NAME + ".max.files.per.mapper";
  static final String MAX_FILES_PER_REDUCER_LABEL = 
                                        NAME + ".max.files.per.reducer";
  static final String SRC_LIST_LABEL = NAME + ".src.list";
  static final String SRC_COUNT_LABEL = NAME + ".src.count";
  static final String TOTAL_SIZE_LABEL = NAME + ".total.size";
  static final String TOTAL_BLOCKS_LABEL = NAME + ".total.blocks";
  static final String DST_DIR_LIST_LABEL = NAME + ".dst.dir.list";
  static final String DST_CHUNK_FILE_LIST_LABEL = NAME + ".dst.chunk.file.list";
  static final String SPLIT_LIST_LABEL = NAME + ".file.split.list";
  public static final String BYTES_PER_MAP_LABEL = NAME + ".bytes.per.map";
  static final String PRESERVE_STATUS_LABEL
      = Options.PRESERVE_STATUS.propertyname + ".value";

  private JobConf conf;

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public DistCp(Configuration conf) {
    setConf(conf);
  }

  /**
   * An input/output pair of filenames.
   * offset and length field are used for chopping files into chunk files
   * and copying file chunks instead of one big file
   * offset tells where is the starting point of the chuck file in the 
   * original file 
   * length tells the length of the chuck file
   * If the FilePair are directories or we don't want to copy file by chunks,
   * both offset and length will be set to 0
   * Also we use the index to tell the order of the chunks
   *
   */
  static class FileChunkPair implements Writable {
    FileStatus input = new FileStatus();
    String output;
    long offset;
    long length;
    int chunkIndex;
    FileChunkPair() { }
    FileChunkPair(FileStatus input, String output, long offset, 
        long length, int chunkIndex) {
      this.input = input;
      this.output = output;
      this.offset = offset;
      this.length = length;
      this.chunkIndex = chunkIndex;
    }
    public void readFields(DataInput in) throws IOException {
      input.readFields(in);
      output = Text.readString(in);
      offset = in.readLong();
      length = in.readLong();
      chunkIndex = in.readInt();
    }
    public void write(DataOutput out) throws IOException {
      input.write(out);
      Text.writeString(out, output);
      out.writeLong(offset);
      out.writeLong(length);
      out.writeInt(chunkIndex);
    }
    public String toString() {
      return input + " : " + output + " : " + offset + " : " + length  
      + " : " + chunkIndex ;
    }
  }
  
  /**
   * An input/output pair of filenames.
   */  
  static class FilePairComparable implements WritableComparable {
    FileStatus input = new FileStatus();
    String output;
    FilePairComparable() { }
    FilePairComparable(FileStatus input, String output) {
      this.input = input;
      this.output = output;
    }
    public int compareTo(Object otherObj) { 
      FilePairComparable other = (FilePairComparable) otherObj;
      return this.input.compareTo(other.input);
    }
    public void readFields(DataInput in) throws IOException {
      input.readFields(in);
      output = Text.readString(in);
    }
    public void write(DataOutput out) throws IOException {
      input.write(out);
      Text.writeString(out, output);
    }
    public String toString() {
      return input + " : " + output;
    }
  }

  /**
   * Computes the total number of blocks in a file.
   * 
   * @param len
   *          the length of the file.
   * @param blockSize
   *          the block size for the file.
   * @return the total number of blocks.
   */
  private static long getBlocks(long len, long blockSize) {
    return (len == 0 || blockSize == 0) ? 0 : (len - 1) / blockSize + 1;
  }

  /**
   * InputFormat of a distcp job responsible for generating splits of the src
   * file list when FastCopy is being used for copying.
   */
  static class FastCopyInputFormat extends CopyInputFormat {

    @Override
    protected long getIncrement(LongWritable key, FilePairComparable value) {
      return getBlocks(value.input.getLen(), value.input.getBlockSize());
    }

    @Override
    protected long getTargetSize(JobConf job, int numSplits) {
      long totalBlocks = job.getLong(TOTAL_BLOCKS_LABEL, -1);
      return totalBlocks / numSplits;
    }
  }
  
  /**
   * InputFormat of a distcp job responsible for generating splits of the src
   * file list.
   */
  static class CopyInputFormat implements InputFormat<Text, Text> {

    protected long getIncrement(LongWritable key, FilePairComparable value) {
      return key.get();
    }

    protected long getTargetSize(JobConf job, int numSplits) {
      long cbsize = job.getLong(TOTAL_SIZE_LABEL, -1);
      return cbsize / numSplits;
    }

    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     * @param job The handle to the JobConf object
     * @param numSplits Number of splits requested
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      int cnfiles = job.getInt(SRC_COUNT_LABEL, -1);
      long cbsize = job.getLong(TOTAL_SIZE_LABEL, -1);
      long blocks = job.getLong(TOTAL_BLOCKS_LABEL, -1);
      String srcfilelist = job.get(SRC_LIST_LABEL, "");
      if (cnfiles < 0 || cbsize < 0 || blocks < 0 || "".equals(srcfilelist)) {
        throw new RuntimeException("Invalid metadata: #files(" + cnfiles +
                                   ") total_size(" + cbsize + ") listuri(" +
                                   srcfilelist + ")");
      }
      Path src = new Path(srcfilelist);
      FileSystem fs = src.getFileSystem(job);
      FileStatus srcst = fs.getFileStatus(src);

      ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
      LongWritable key = new LongWritable();
      FilePairComparable value = new FilePairComparable();
      final long targetsize = getTargetSize(job, numSplits);
      long pos = 0L;
      long last = 0L;
      long acc = 0L;
      long cbrem = srcst.getLen();
      SequenceFile.Reader sl = null;
      try {
        sl = new SequenceFile.Reader(fs, src, job);
        for (; sl.next(key, value); last = sl.getPosition()) {
          // if adding this split would put this split past the target size,
          // cut the last split and put this next file in the next split.
          long increment = getIncrement(key, value);
          if (acc + increment > targetsize && acc != 0) {
            long splitsize = last - pos;
            splits.add(new FileSplit(src, pos, splitsize, (String[])null));
            cbrem -= splitsize;
            pos = last;
            acc = 0L;
          }
          acc += increment;
        }
      }
      finally {
        checkAndClose(sl);
      }
      if (cbrem != 0) {
        splits.add(new FileSplit(src, pos, cbrem, (String[])null));
      }

      return splits.toArray(new FileSplit[splits.size()]);
    }

    /**
     * Returns a reader for this split of the src file list.
     */
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, Text>(job, (FileSplit)split);
    }
  }
  
  /**
   * InputFormat of a distcp job responsible for generating splits from the 
   * split list we create during the setup phase.
   * 
   */
  static class CopyByChunkInputFormat extends CopyInputFormat 
  implements InputFormat<Text, Text> {

    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     * @param job The handle to the JobConf object
     * @param numSplits Number of splits requested
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      int cnfiles = job.getInt(SRC_COUNT_LABEL, -1);
      long cbsize = job.getLong(TOTAL_SIZE_LABEL, -1);
      String srcFileList = job.get(SRC_LIST_LABEL, "");
      Path srcFileListPath = new Path(srcFileList);
      if (cnfiles < 0 || cbsize < 0 || "".equals(srcFileList)) {
        throw new RuntimeException("Invalid metadata: #files(" + cnfiles +
            ") total_size(" + cbsize + ") src_chunk_file_list_uri(" +
            srcFileList + ")");
      }
      ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
      SequenceFile.Reader sl = null;
      String splitList = job.get(SPLIT_LIST_LABEL, "");
      if("".equals(splitList)) {
        throw new RuntimeException("Invalid metadata: split_list_uri(" +
            srcFileList + ")");
      }
      //split file list which contains start pos and split length pairs
      //they are used to split srcChunkFileList
      Path splitListPath = new Path(splitList);        
      FileSystem splitListFs = splitListPath.getFileSystem(job);
      try{
        sl = new SequenceFile.Reader(splitListFs, splitListPath, job);
        LongWritable startpos = new LongWritable();
        LongWritable length = new LongWritable();
        while (sl.next(startpos, length)) {
          splits.add(new FileSplit(srcFileListPath, startpos.get(), 
              length.get(), (String[])null));
        }
      }
      finally{
        checkAndClose(sl);
      }
      return splits.toArray(new FileSplit[splits.size()]);
    }
  }
  /**
   * FSCopyFilesTask: The mapper for copying files between FileSystems.
   */
  static class CopyFilesTask
      implements Mapper<LongWritable, FilePairComparable, FilePairComparable, Text>,
                 Reducer<FilePairComparable, Text, FilePairComparable, Text> {
    // config
    private int sizeBuf = 128 * 1024;
    private FileSystem destFileSys = null;
    private boolean ignoreReadFailures;
    private boolean preserve_status;
    private EnumSet<FileAttribute> preserved;
    private boolean overwrite;
    private boolean update;
    private Path destPath = null;
    private Path tmpRoot = null;
    private Path attemptTmpRoot = null;
    private byte[] buffer = null;
    private JobConf job;
    private boolean skipCRCCheck = false;
    private boolean useFastCopy = false;
    private boolean skipUnderConstructionFile = false;
    
    private FastCopy fc = null;

    // stats
    private int failcount = 0;
    private int skipcount = 0;
    private int copycount = 0;

    private String getCountString() {
      return "Copied: " + copycount + " Skipped: " + skipcount
      + " Failed: " + failcount;
    }
    private void updateStatus(Reporter reporter) {
      reporter.setStatus(getCountString());
    }

    /**
     * Return true if dst should be replaced by src and the update flag is set.
     * Right now, this merely checks that the src and dst len are not equal. 
     * This should be improved on once modification times, CRCs, etc. can
     * be meaningful in this context.
     * @throws IOException 
     */
    private boolean needsUpdate(FileStatus srcstatus,
        FileSystem dstfs, Path dstpath) throws IOException {
      return update && !sameFile(srcstatus.getPath().getFileSystem(job),
          srcstatus, dstfs, dstpath, skipCRCCheck);
    }

    private FSDataOutputStream create(Path f, Reporter reporter,
        FileStatus srcstat) throws IOException {
      if (destFileSys.exists(f)) {
        destFileSys.delete(f, false);
      }
      if (!preserve_status) {
        return destFileSys.create(f, true, sizeBuf, reporter);
      }

      FsPermission permission = preserved.contains(FileAttribute.PERMISSION)?
          srcstat.getPermission(): null;
      short replication = preserved.contains(FileAttribute.REPLICATION)?
          srcstat.getReplication(): destFileSys.getDefaultReplication();
      long blockSize = preserved.contains(FileAttribute.BLOCK_SIZE)?
          srcstat.getBlockSize(): destFileSys.getDefaultBlockSize();
      return destFileSys.create(f, permission, true, sizeBuf, replication,
          blockSize, reporter);
    }

    /**
     * Copy a file to a destination without breaking file into chunks
     * @param filePair the pair of source and dest
     * @param outc map output collector
     * @param reporter
     */
    private void copy(FilePairComparable filePair,
        OutputCollector<FilePairComparable, Text> outc, Reporter reporter)
        throws IOException {
      FileStatus srcstat = filePair.input;
      Path relativedst = new Path(filePair.output);
      Path absdst = new Path(destPath, relativedst);
      Path tmpFile = new Path(attemptTmpRoot, relativedst);
      Path finalTmpFile = new Path(tmpRoot, relativedst);
      int totfiles = job.getInt(SRC_COUNT_LABEL, -1);
      assert totfiles >= 0 : "Invalid file count " + totfiles;

      // if a directory, ensure created even if empty
      if (srcstat.isDir()) {
        if (destFileSys.exists(absdst)) {
          if (!destFileSys.getFileStatus(absdst).isDir()) {
            throw new IOException("Failed to mkdirs: " + absdst+" is a file.");
          }
        }
        else if (!destFileSys.mkdirs(absdst)) {
          throw new IOException("Failed to mkdirs " + absdst);
        }
        // TODO: when modification times can be set, directories should be
        // emitted to reducers so they might be preserved. Also, mkdirs does
        // not currently return an error when the directory already exists;
        // if this changes, all directory work might as well be done in reduce
        return;
      }

      if ((destFileSys.exists(absdst) && !overwrite
          && !needsUpdate(srcstat, destFileSys, absdst)) ||
            destFileSys.exists(finalTmpFile)) {
        outc.collect(filePair, new Text("SKIP: " + srcstat.getPath()));
        ++skipcount;
        reporter.incrCounter(Counter.SKIP, 1);
        updateStatus(reporter);
        return;
      }
      
      FileSystem srcFileSys = srcstat.getPath().getFileSystem(job);
      reporter.incrCounter(Counter.BYTESEXPECTED, srcstat.getLen());
      if (useFastCopy) {
        try {
          if (fc == null) {
            throw new IOException("FastCopy object has not been instantiated.");
          }
          LOG.info("Use FastCopy to copy File from " + srcstat.getPath() +" to " + tmpFile);
          CopyResult ret = fc.copy(srcstat.getPath().toString(), tmpFile.toString(), 
              DFSUtil.convertToDFS(srcFileSys),
              DFSUtil.convertToDFS(destFileSys), reporter);
          
          // update the skip count;
          if (ret.equals(CopyResult.SKIP)) {
            outc.collect(filePair, new Text("SKIP: " + srcstat.getPath()));
            ++ skipcount;
            reporter.incrCounter(Counter.SKIP, 1);
            updateStatus(reporter);
            return;
          }
        } catch (Exception e) {
          throw new IOException("FastCopy throws exception", e);
        }
        reporter.setStatus("Copied " + srcstat.getPath().toString());
      } else {
        long cbcopied = 0L;
        FSDataInputStream in = null;
        FSDataOutputStream out = null;
        try {
          if (!srcstat.isDir() && skipUnderConstructionFile) {
            // skip under construction file.
            DistributedFileSystem dfs = DFSUtil.convertToDFS(srcFileSys);
            LocatedBlocks locatedBlks = dfs.getClient().getLocatedBlocks(
                srcstat.getPath().toUri().getPath(), 0, srcstat.getLen());
            if (locatedBlks.isUnderConstruction()) {
              LOG.debug("Skip under construnction file: " + srcstat.getPath());
              outc.collect(filePair, new Text("SKIP: " + srcstat.getPath()));
              ++ skipcount;
              reporter.incrCounter(Counter.SKIP, 1);
              updateStatus(reporter);
              return;
            }
          }
          // open src file
          in = srcstat.getPath().getFileSystem(job).open(srcstat.getPath());
          // open tmp file
          out = create(tmpFile, reporter, srcstat);
          // copy file
          for(int cbread; (cbread = in.read(buffer)) >= 0; ) {
            out.write(buffer, 0, cbread);
            cbcopied += cbread;
            reporter.setStatus(
                String.format("%.2f ", cbcopied*100.0/srcstat.getLen())
                + absdst + " [ " +
                StringUtils.humanReadableInt(cbcopied) + " / " +
                StringUtils.humanReadableInt(srcstat.getLen()) + " ]");
          }
        } finally {
          checkAndClose(in);
          checkAndClose(out);
        }
        
        if (cbcopied != srcstat.getLen()) {
          throw new IOException("File size not matched: copied "
              + bytesString(cbcopied) + " to tmpFile (=" + tmpFile
              + ") but expected " + bytesString(srcstat.getLen()) 
              + " from " + srcstat.getPath());        
        }
        long copiedFile = destFileSys.getFileStatus(tmpFile).getLen();
        if (copiedFile != srcstat.getLen()) {
          throw new IOException("File size not matched: original "
              + bytesString(srcstat.getLen()) + " copied "
              + bytesString(copiedFile)
              + " from " + srcstat.getPath());
        }
      }
      
      if (totfiles == 1) {
        // Copying a single file; use dst path provided by user as destination
        // rather than destination directory, if a file
        Path dstparent = finalTmpFile.getParent();
        if (!(destFileSys.exists(dstparent) &&
              destFileSys.getFileStatus(dstparent).isDir())) {
          finalTmpFile = dstparent;
        }
      }
      if (destFileSys.exists(finalTmpFile) &&
          destFileSys.getFileStatus(finalTmpFile).isDir()) {
        throw new IOException(finalTmpFile + " is a directory");
      }
      if (!destFileSys.mkdirs(finalTmpFile.getParent())) {
        throw new IOException("Failed to create parent dir: " + finalTmpFile.getParent());
      }
      outc.collect(filePair, new Text("COPIED"));
      destFileSys.rename(tmpFile, finalTmpFile);


      // report at least once for each file
      ++copycount;
      reporter.incrCounter(Counter.BYTESCOPIED, srcstat.getLen());
      reporter.incrCounter(Counter.BLOCKSCOPIED,
          getBlocks(srcstat.getLen(), srcstat.getBlockSize()));
      reporter.incrCounter(Counter.COPY, 1);
      updateStatus(reporter);
    }
    
    /** rename tmp to dst, delete dst if already exists */
    private void rename(Path tmp, Path dst) throws IOException {
      try {
        if (destFileSys.exists(dst)) {
          destFileSys.delete(dst, true);
        }
        if (!destFileSys.rename(tmp, dst)) {
          throw new IOException();
        }
      }
      catch(IOException cause) {
        throw (IOException)new IOException("Fail to rename tmp file (=" + tmp 
            + ") to destination file (=" + dst + ")").initCause(cause);
      }
    }

    private void updateDestStatus(FileStatus src, FileStatus dst
        ) throws IOException {
      if (preserve_status) {
        DistCp.updateDestStatus(src, dst, preserved, destFileSys);
      }
      DistCp.checkReplication(src, dst, preserved, destFileSys);
    }

    static String bytesString(long b) {
      return b + " bytes (" + StringUtils.humanReadableInt(b) + ")";
    }

    /** Mapper configuration.
     * Extracts source and destination file system, as well as
     * top-level paths on source and destination directories.
     * Gets the named file systems, to be used later in map.
     */
    public void configure(JobConf job)
    {
      destPath = new Path(job.get(DST_DIR_LABEL, "/"));
      tmpRoot = new Path(job.get(TMP_DIR_LABEL));
      String attemptId = job.get("mapred.task.id");
      attemptTmpRoot = new Path(tmpRoot, attemptId);

      try {
        destFileSys = destPath.getFileSystem(job);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to get the named file system.", ex);
      }
      sizeBuf = job.getInt("copy.buf.size", 128 * 1024);
      buffer = new byte[sizeBuf];
      ignoreReadFailures = job.getBoolean(Options.IGNORE_READ_FAILURES.propertyname, false);
      preserve_status = job.getBoolean(Options.PRESERVE_STATUS.propertyname, false);
      if (preserve_status) {
        preserved = FileAttribute.parse(job.get(PRESERVE_STATUS_LABEL));
      }
      update = job.getBoolean(Options.UPDATE.propertyname, false);
      overwrite = !update && job.getBoolean(Options.OVERWRITE.propertyname, false);
      skipCRCCheck = job.getBoolean(Options.SKIPCRC.propertyname, false);
      useFastCopy = job.getBoolean(Options.USEFASTCOPY.propertyname, false);
      skipUnderConstructionFile = 
          job.getBoolean(Options.SKIPUNDERCONSTRUCTION.propertyname, false);
      
      if (useFastCopy) {
        try {
          fc = new FastCopy(job, skipUnderConstructionFile);
        } catch (Exception e) {
          LOG.error("Exception during fastcopy instantiation", e);
        }
      }
      this.job = job;
    }

    /** Map method. Copies one file from source file system to destination.
     * @param key src len
     * @param value FilePair (FileStatus src, Path dst)
     * @param out Log of failed copies
     * @param reporter
     */
    public void map(LongWritable key,
                    FilePairComparable value,
                    OutputCollector<FilePairComparable, Text> out,
                    Reporter reporter) throws IOException {
      final FileStatus srcstat = value.input;
      final Path relativedst = new Path(value.output);
      try {
        copy(value, out, reporter);
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FAIL, 1);
        updateStatus(reporter);
        final String sfailure = "FAIL " + relativedst + " : " +
                          StringUtils.stringifyException(e);
        out.collect(value, new Text(sfailure));
        LOG.info(sfailure);
        try {
          for (int i = 0; i < 3; ++i) {
            try {
              final Path tmp = new Path(attemptTmpRoot, relativedst);
              if (destFileSys.delete(tmp, true))
                break;
            } catch (Throwable ex) {
              // ignore, we are just cleaning up
              LOG.debug("Ignoring cleanup exception", ex);
            }
            // update status, so we don't get timed out
            updateStatus(reporter);
            Thread.sleep(3 * 1000);
          }
        } catch (InterruptedException inte) {
          throw (IOException)new IOException().initCause(inte);
        }
      } finally {
        updateStatus(reporter);
      }
    }

    public void reduce(FilePairComparable file, Iterator<Text> statuses,
        OutputCollector<FilePairComparable, Text> out, Reporter reporter)
        throws IOException {
      final FileStatus srcStat = file.input;
      final Path relativeDest = new Path(file.output);
      Path absdst = new Path(destPath, relativeDest);
      Path finalTmpFile = new Path(tmpRoot, relativeDest);

      int totfiles = job.getInt(SRC_COUNT_LABEL, -1);
      assert totfiles >= 0 : "Invalid file count " + totfiles;

      if (totfiles == 1) {
        // Copying a single file; use dst path provided by user as destination
        // rather than destination directory, if a file
        Path dstparent = absdst.getParent();
        if (!(destFileSys.exists(dstparent) &&
              destFileSys.getFileStatus(dstparent).isDir())) {
          absdst = dstparent;
        }
      }
      if (!destFileSys.exists(finalTmpFile)) {
        // This is a rerun of a reducer and finalTmp has been moved into the
        // proper location already.
        // Another option is that the file has not been copied over in the first
        // place, but that was caught in the mapper
        LOG.info("Skipping " + absdst + " as it has been moved already");
        return;
      }
      if (destFileSys.exists(absdst) &&
          destFileSys.getFileStatus(absdst).isDir()) {
        throw new IOException(absdst + " is a directory");
      }
      if (!destFileSys.mkdirs(absdst.getParent())) {
        throw new IOException("Failed to create parent dir: " +
            absdst.getParent());
      }
      rename(finalTmpFile, absdst);
      
      FileStatus dststat = destFileSys.getFileStatus(absdst);
      if (dststat.getLen() != srcStat.getLen()) {
        destFileSys.delete(absdst, false);
        throw new IOException("File size not matched: copied "
            + bytesString(dststat.getLen()) + " to dst (=" + absdst 
            + ") but expected " + bytesString(srcStat.getLen()) 
            + " from " + srcStat.getPath());        
      }
      updateDestStatus(srcStat, dststat);
    }

    public void close() throws IOException {
      if (fc != null) {
        fc.shutdown();
      }
      if (0 == failcount || ignoreReadFailures) {
        return;
      }
      throw new IOException(getCountString());
    }
  }
  
  static class CopyFilesByChunkReducer
      implements Reducer<Text, IntWritable, WritableComparable<?>, Text> {

    private FileSystem destFileSys = null;
    private DistributedFileSystem dstDistFs = null;
    private Path destPath = null;
    private Path tmpPath = null;
    private int totFiles = -1;
    private JobConf job;
    
    @Override
    public void configure(JobConf job) {
      destPath = new Path(job.get(DST_DIR_LABEL, "/"));
      try {
        destFileSys = destPath.getFileSystem(job);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to get the named file system.", ex);
      }
      
      dstDistFs = DFSUtil.convertToDFS(destFileSys);
      if (dstDistFs == null) {
        throw new RuntimeException("No distributed file system found!");
      }
      
      tmpPath = new Path(job.get(TMP_DIR_LABEL));
      totFiles = job.getInt(SRC_COUNT_LABEL, -1);
      this.job = job;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<WritableComparable<?>, Text> output, Reporter reporter)
        throws IOException {
      if (!values.hasNext()) {
        return;
      }
      
      int chunkNum = 0;
      while (values.hasNext()) {
        chunkNum = Math.max(chunkNum, values.next().get());
      }
      
      String dstFilePath = key.toString();
      stitchChunkFile(job, job, dstDistFs, dstFilePath, chunkNum, destPath, 
          tmpPath, totFiles);
    }
    
  }
  
  /**
   * FSCopyFilesTask: The mapper for copying files between FileSystems.
   */
  static class CopyFilesByChunkMapper 
      implements Mapper<LongWritable, FileChunkPair, Text, IntWritable> {
    // config
    private int sizeBuf = 128 * 1024;
    private FileSystem destFileSys = null;
    private boolean ignoreReadFailures;
    private boolean preserve_status;
    private EnumSet<FileAttribute> preserved;
    private boolean overwrite;
    private boolean update;
    private Path destPath = null;
    private byte[] buffer = null;
    private JobConf job;
    private boolean skipCRCCheck = false;

    // stats
    private int failcount = 0;
    private int skipcount = 0;
    private int copycount = 0;

    private String getCountString() {
      return "Copied: " + copycount + " Skipped: " + skipcount
      + " Failed: " + failcount;
    }
    private void updateStatus(Reporter reporter) {
      reporter.setStatus(getCountString());
    }

    /**
     * Return true if dst should be replaced by src and the update flag is set.
     * Right now, this merely checks that the src and dst len are not equal. 
     * This should be improved on once modification times, CRCs, etc. can
     * be meaningful in this context.
     * @throws IOException 
     */
    private boolean needsUpdate(FileStatus srcstatus,
        FileSystem dstfs, Path dstpath) throws IOException {
      return update && !sameFile(srcstatus.getPath().getFileSystem(job),
          srcstatus, dstfs, dstpath, skipCRCCheck);
    }

    private FSDataOutputStream create(Path f, Reporter reporter,
        FileStatus srcstat) throws IOException {
      if (destFileSys.exists(f)) {
        destFileSys.delete(f, false);
      }
      if (!preserve_status) {
        return destFileSys.create(f, true, sizeBuf, reporter);
      }

      FsPermission permission = preserved.contains(FileAttribute.PERMISSION)?
          srcstat.getPermission(): null;
      short replication = preserved.contains(FileAttribute.REPLICATION)?
          srcstat.getReplication(): destFileSys.getDefaultReplication();
      long blockSize = preserved.contains(FileAttribute.BLOCK_SIZE)?
          srcstat.getBlockSize(): destFileSys.getDefaultBlockSize();
      return destFileSys.create(f, permission, true, sizeBuf, replication,
          blockSize, reporter);
    }

    /**
     * Copy a file to a destination.
     * @param srcstat src path and metadata
     * @param relativedst dst path
     * @param offset the start point of the file chunk
     * @param length the length of the file chunk
     * @param chunkIndex the chunkIndex of the file chunk
     * @param reporter
     */
    private void copy(FileStatus srcstat, Path relativedst, long offset, long length,
        int chunkIndex, OutputCollector<Text, IntWritable> outc, 
        Reporter reporter) throws IOException {
      Path absdst = new Path(destPath, relativedst);
      int totfiles = job.getInt(SRC_COUNT_LABEL, -1);
      assert totfiles >= 0 : "Invalid file count " + totfiles;

      // if a directory, ensure created even if empty
      if (srcstat.isDir()) {
        if (destFileSys.exists(absdst)) {
          if (!destFileSys.getFileStatus(absdst).isDir()) {
            throw new IOException("Failed to mkdirs: " + absdst+" is a file.");
          }
        }
        else if (!destFileSys.mkdirs(absdst)) {
          throw new IOException("Failed to mkdirs " + absdst);
        }
        // TODO: when modification times can be set, directories should be
        // emitted to reducers so they might be preserved. Also, mkdirs does
        // not currently return an error when the directory already exists;
        // if this changes, all directory work might as well be done in reduce
        return;
      }

      //if a file
      //here skip count actually counts how many file chunks are skipped
      if (destFileSys.exists(absdst) && !overwrite
          && !needsUpdate(srcstat, destFileSys, absdst)) {
        ++skipcount;
        reporter.incrCounter(Counter.SKIP, 1);
        updateStatus(reporter);
        return;
      }

      Path chunkFile = new Path(job.get(TMP_DIR_LABEL),
          createFileChunkPath(relativedst, chunkIndex));
      destFileSys.mkdirs(chunkFile.getParent());

      // No need to copy over the chunk file if it already exists.
      if (destFileSys.exists(chunkFile) &&
          !needsUpdate(srcstat, destFileSys, chunkFile)) {
        ++skipcount;
        reporter.incrCounter(Counter.SKIP, 1);
        updateStatus(reporter);
        return;
      }

      String attemptId = job.get("mapred.task.id");
      Path tmpFile = new Path(job.get(TMP_DIR_LABEL),
          createFileChunkPath(new Path(relativedst, "_tmp_" + attemptId),
            chunkIndex));

      long cbcopied = 0L;
      long needCopied = length;
      FSDataInputStream in = null;
      FSDataOutputStream out = null;
      try {
        // open src file
        in = srcstat.getPath().getFileSystem(job).open(srcstat.getPath());
        reporter.incrCounter(Counter.BYTESEXPECTED, length);
        // open tmp file
        out = create(tmpFile, reporter, srcstat);
        in.seek(offset);
        if(in.getPos() != offset){
          throw new IOException("File byte number doesn't match the offset.");
        }
        for(int cbread; (cbread = in.read(buffer)) >= 0; ) {
          if(needCopied == 0)
            break;
          if(needCopied >= cbread) {
            out.write(buffer, 0, cbread);
            cbcopied += cbread;
            needCopied -= cbread;
          }
          else {
            out.write(buffer, 0, (int) needCopied);
            cbcopied += needCopied;
            needCopied -= needCopied;
          }
          reporter.setStatus(
              String.format("%.2f ", cbcopied*100.0/length)
              + absdst + " [ " +
              StringUtils.humanReadableInt(cbcopied) + " / " +
              StringUtils.humanReadableInt(length) + " ]");
          if(needCopied == 0)
            break;
        }

      } finally {
        if (cbcopied != length) {
          LOG.warn("Deleting temp file : " + tmpFile
              + ", since the copy failed");
          destFileSys.delete(tmpFile, false);
        }
        checkAndClose(in);
        checkAndClose(out);
      }

      if (cbcopied != length) {
        throw new IOException("File size not matched: copied "
            + bytesString(cbcopied) + " to chunkFile (=" + chunkFile
            + ") but expected " + bytesString(length) 
            + " from " + srcstat.getPath());        
      }
      else {
        if (!destFileSys.rename(tmpFile, chunkFile)) {
          // If the chunkFile already exists, rename will fail and this is an
          // error we should ignore since the copy has already been
          // done by a speculated task.
          if (!destFileSys.exists(chunkFile)) {
            throw new IOException("Rename " + tmpFile + " to "
                + chunkFile + " failed");
          }
        }
        FileStatus chunkFileStat = destFileSys.getFileStatus(chunkFile);
        updateDestStatus(srcstat, chunkFileStat);
      }

      outc.collect(new Text(relativedst.toUri().getPath()), 
          new IntWritable(chunkIndex + 1));
      
      // report at least once for each file chunk
      ++copycount;
      reporter.incrCounter(Counter.BYTESCOPIED, cbcopied);
      reporter.incrCounter(Counter.COPY, 1);
      updateStatus(reporter);
    }

    /**
     * create one directory for each file, call it filename_chunkfiles
     * and store the file chunks into that directory, name them by it's chunk chunkIndex
     * @return the path of the directory for that file
     */
    private Path createFileChunkPath(Path dst, int chunkIndex) 
        throws IOException{
      Path chunkFileDir = new Path(dst + "_chunkfiles");
      return new Path(chunkFileDir,Long.toString(chunkIndex));
    }

    private void updateDestStatus(FileStatus src, FileStatus dst
        ) throws IOException {
      if (preserve_status) {
        DistCp.updateDestStatus(src, dst, preserved, destFileSys);
      }
      DistCp.checkReplication(src, dst, preserved, destFileSys);
    }

    static String bytesString(long b) {
      return b + " bytes (" + StringUtils.humanReadableInt(b) + ")";
    }

    /** Mapper configuration.
     * Extracts source and destination file system, as well as
     * top-level paths on source and destination directories.
     * Gets the named file systems, to be used later in map.
     */
    public void configure(JobConf job)
    {
      destPath = new Path(job.get(DST_DIR_LABEL, "/"));
      try {
        destFileSys = destPath.getFileSystem(job);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to get the named file system.", ex);
      }
      sizeBuf = job.getInt("copy.buf.size", 128 * 1024);
      buffer = new byte[sizeBuf];
      ignoreReadFailures = job.getBoolean(Options.IGNORE_READ_FAILURES.propertyname, false);
      preserve_status = job.getBoolean(Options.PRESERVE_STATUS.propertyname, false);
      if (preserve_status) {
        preserved = FileAttribute.parse(job.get(PRESERVE_STATUS_LABEL));
      }
      update = job.getBoolean(Options.UPDATE.propertyname, false);
      overwrite = !update && job.getBoolean(Options.OVERWRITE.propertyname, false);
      skipCRCCheck = job.getBoolean(Options.SKIPCRC.propertyname, false);
      this.job = job;
    }

    /** Map method. Copies one file from source file system to destination.
     * @param key src len
     * @param value FileChunkPair (FileStatus src, Path dst, long offset, 
     * long length, int chunkIndex)
     * @param out Log of failed copies
     * @param reporter
     */
    public void map(LongWritable key,
                    FileChunkPair value,
                    OutputCollector<Text, IntWritable> out,
                    Reporter reporter) throws IOException {
      final FileStatus srcstat = value.input;
      final Path relativedst = new Path(value.output);
      final long offset = value.offset;
      final long length = value.length;
      final int chunkIndex = value.chunkIndex;
      LOG.info(relativedst + " offset :"  + offset + " length : " + length + " chunkIndex : " + chunkIndex);
      try {
          copy(srcstat, relativedst, offset, length, chunkIndex, out, reporter);
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FAIL, 1);
        updateStatus(reporter);
        final String sfailure = "FAIL " + relativedst + " : " +
                          StringUtils.stringifyException(e);
        LOG.info(sfailure);
        try {
          for (int i = 0; i < 3; ++i) {
            try {
              final Path tmp = new Path(job.get(TMP_DIR_LABEL), relativedst);
              if (destFileSys.delete(tmp, true))
                break;
            } catch (Throwable ex) {
              // ignore, we are just cleaning up
              LOG.debug("Ignoring cleanup exception", ex);
            }
            // update status, so we don't get timed out
            updateStatus(reporter);
            Thread.sleep(3 * 1000);
          }
        } catch (InterruptedException inte) {
          throw (IOException)new IOException().initCause(inte);
        }
      } finally {
        updateStatus(reporter);
      }
    }

    public void close() throws IOException {
      if (0 == failcount || ignoreReadFailures) {
        return;
      }
      throw new IOException(getCountString());
    }
  }

  private static List<Path> fetchFileList(Configuration conf, Path srcList)
      throws IOException {
    List<Path> result = new ArrayList<Path>();
    FileSystem fs = srcList.getFileSystem(conf);
    BufferedReader input = null;
    try {
      input = new BufferedReader(new InputStreamReader(fs.open(srcList)));
      String line = input.readLine();
      while (line != null) {
        result.add(new Path(line));
        line = input.readLine();
      }
    } finally {
      checkAndClose(input);
    }
    return result;
  }

  @Deprecated
  public static void copy(Configuration conf, String srcPath,
                          String destPath, Path logPath,
                          boolean srcAsList, boolean ignoreReadFailures)
      throws IOException {
    final Path src = new Path(srcPath);
    List<Path> tmp = new ArrayList<Path>();
    if (srcAsList) {
      tmp.addAll(fetchFileList(conf, src));
    } else {
      tmp.add(src);
    }
    EnumSet<Options> flags = ignoreReadFailures
      ? EnumSet.of(Options.IGNORE_READ_FAILURES)
      : EnumSet.noneOf(Options.class);

    final Path dst = new Path(destPath);
    copy(conf, new Arguments(tmp, null, dst, logPath, flags, null,
        Long.MAX_VALUE, Long.MAX_VALUE, null));
  }

  /** Sanity check for srcPath */
  private static void checkSrcPath(Configuration conf, List<Path> srcPaths
      ) throws IOException {
    List<IOException> rslt = new ArrayList<IOException>();
    List<Path> unglobbed = new LinkedList<Path>();
    for (Path p : srcPaths) {
      FileSystem fs = p.getFileSystem(conf);
      FileStatus[] inputs = fs.globStatus(p);

      if(inputs != null && inputs.length > 0) {
        for (FileStatus onePath: inputs) {
          unglobbed.add(onePath.getPath());
        }
      } else {
        rslt.add(new IOException("Input source " + p + " does not exist."));
      }
    }
    if (!rslt.isEmpty()) {
      throw new InvalidInputException(rslt);
    }
    srcPaths.clear();
    srcPaths.addAll(unglobbed);
  }

  /**
   *  A copier class initialized for Distcp containing all the
   *  information needed for clients to launch the MapReduce job
   *  with method finalizeJob() to finalize the job and cleanupJob()
   *  to clean up the states.
   *
   *  Here is a typical pattern to use it:
   * 
   *     DistCopier copier = getCopier(conf, args);
   *    
   *     if (copier != null) {
   *       try {
   *         // launch job copier.getJobConf() and waits it to finish
   *         copier.finalizeCopiedFiles();
   *       } finally {
   *         copier.cleanupJob();
   *       }
   *     }
   */
  public static class DistCopier {
    private Configuration conf;
    private Arguments args;
    private JobConf jobToRun;
    private JobClient client;
    private boolean copyByChunk;
    
    /**
     * @return MapReduce job conf for the copying
     */
    public JobConf getJobConf() {
      return jobToRun;
    }

    public JobClient getJobClient() {
      return client;
    }
    
    /**
     * Finalize copied files after the MapReduce job. So far all it
     * does is to set metadata information of the copied files to be
     * the same as source files.
     * @throws IOException
     */
    public void finalizeCopiedFiles() throws IOException  {
      if (jobToRun != null) {
        DistCp.finalize(conf, jobToRun, args.dst, args.preservedAttributes);
      }
    }

    /**
     * Clean up temp files used for copying. It should be put
     * in finally block.
     * @throws IOException
     */
    public void cleanupJob()  throws IOException  {
      if (jobToRun != null) {
        cleanupJobInternal(jobToRun);
      }
    }

    private DistCopier(Configuration conf, Arguments args) throws IOException {
      this.conf = conf;
      this.args = args;
    }
    
    static public boolean canUseFastCopy(List<Path> srcs, Path dst,
        Configuration conf) throws IOException {
      return inSameCluster(srcs, dst, conf);
    }
    
    /**
     * @param srcs   source paths
     * @param dst    destination path
     * @param conf
     * @return True if, all source paths and destination path are DFS
     *         locations, and they are from the same DFS clusters. If
     *         it can't find the DFS cluster name since an older server
     *         build, it will assume cluster name matches.
     * @throws IOException
     */
    static public boolean inSameCluster(List<Path> srcs, Path dst,
        Configuration conf) throws IOException {
      DistributedFileSystem dstdfs = DFSUtil.convertToDFS(dst
          .getFileSystem(conf));
      if (dstdfs == null) {
        return false;
      }

      String dstClusterName = dstdfs.getClusterName();
      for (Path src : srcs) {
        DistributedFileSystem srcdfs = DFSUtil.convertToDFS(src
            .getFileSystem(conf));
        if (srcdfs == null) {
          return false;
        } else if (dstClusterName != null) {
          // We assume those clusterName == null case was older
          // version of DFS. We always enable fastcopy for those
          // cases.
          String srcClusterName = srcdfs.getClusterName();
          if (srcClusterName != null && !srcClusterName.equals(dstClusterName)) {
            return false;
          }
        }
      }
      return true;
    }

    private void setupJob() throws IOException  {
      LOG.info("srcPaths=" + args.srcs);
      LOG.info("destPath=" + args.dst);
      checkSrcPath(conf, args.srcs);
      JobConf job;
      copyByChunk = false;
      // if the -copybychunk flag is set, check if the file system allows using 
      // concat
      if(args.flags.contains(Options.COPYBYCHUNK)) {
        FileSystem dstfs = args.dst.getFileSystem(conf);
        //for raidDFS
        if(dstfs instanceof FilterFileSystem) {
            dstfs = ((FilterFileSystem) dstfs).getRawFileSystem();
        }
        if(dstfs instanceof DistributedFileSystem) {
          DistributedFileSystem dstdistfs = (DistributedFileSystem) dstfs;
          //set copybychunk to false if the concat method is not available for the
          //distributed file system
          DFSClient dfsClient = dstdistfs.getClient();
          if(dfsClient.isConcatAvailable())
            copyByChunk = true;
        }
        LOG.debug("After check, copy by chunk is set to: " + copyByChunk);
      }

      boolean useFastCopy = (args.flags.contains(Options.USEFASTCOPY) && canUseFastCopy(
          args.srcs, args.dst, conf));
      
      if(copyByChunk) {
        job = createJobConfForCopyByChunk(conf);
      } else {
        job = createJobConf(conf, useFastCopy);
      }
      
      // set the tos value for each task
      job.setInt(NetUtils.DFS_CLIENT_TOS_CONF,
          conf.getInt(NetUtils.DFS_CLIENT_TOS_CONF, DEFAULT_TOS_VALUE));
      if (args.preservedAttributes != null) {
        job.set(PRESERVE_STATUS_LABEL, args.preservedAttributes);
      }
      if (args.mapredSslConf != null) {
        job.set("dfs.https.client.keystore.resource", args.mapredSslConf);
      }

      try {
        try {
          if (client == null) {
            client = new JobClient(job);
          }
        } catch (IOException ex) {
          throw new IOException("Error creating JobClient", ex);
        }
        if(copyByChunk) {
          if (setupForCopyByChunk(conf, job, client, args)) {
            jobToRun = job;
          } else {
            finalizeCopiedFilesInternal(job);
          }
        } else {
          if (setup(conf, job, client, args, useFastCopy)) {
            jobToRun = job;
          } else {
            finalizeCopiedFilesInternal(job);
          }
        }
      } finally {
        if (jobToRun == null) {
          cleanupJobInternal(job);
        }
      }
    }
    
    private void finalizeCopiedFilesInternal(JobConf job) throws IOException {
      if (copyByChunk) {
        // if we copy the files by chunks, then need to stitch the file chunks
        // together back to the original file after the map
        LOG.debug("copy by chunk and stitch!");
        stitchChunks(conf, job, args);
      }
      DistCp.finalize(conf, job, args.dst, args.preservedAttributes);
    }

    private void cleanupJobInternal(JobConf job) throws IOException {
      //delete tmp
      fullyDelete(job.get(TMP_DIR_LABEL), job);
      //delete jobDirectory
      fullyDelete(job.get(JOB_DIR_LABEL), job);
    }
    
  }

  /**
   * Return a DistCopier object for copying the files.
   * 
   * If a MapReduce job is needed, a DistCopier instance will be
   * initialized and returned.
   * If no MapReduce job is needed (for empty directories), all the
   * other work will be done in this function, and NULL will be
   * returned. If caller sees NULL returned, the copying has
   * succeeded.
   * 
   * @param conf
   * @param args
   * @return
   * @throws IOException
   */
  public static DistCopier getCopier(final Configuration conf,
      final Arguments args) throws IOException {
    DistCopier dc = new DistCopier(conf, args);
    dc.setupJob();
    if (dc.getJobConf() != null) {
      return dc;
    } else {
      return null;
    }
  }

  /**
   * Driver to copy srcPath to destPath depending on required protocol.
   * @param args arguments
   */
  static void copy(final Configuration conf, final Arguments args
      ) throws IOException {
    DistCopier copier = getCopier(conf, args);
    
    if (copier != null) {
      try {
        JobClient client = copier.getJobClient();
        RunningJob job = client.submitJob(copier.getJobConf());
        try {
          if (!client.monitorAndPrintJob(copier.getJobConf(), job)) {
            throw new IOException("Job failed!");
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
        copier.finalizeCopiedFiles();
      } finally {
        copier.cleanupJob();
      }
    }
  }

  /**
   * In the dst file system, go through all the file chunks, stitch them
   * together to get back the original files.
   * After the mappers, file chunks are stored in the tmp directory.
   * 1. Read in the dst_chunk_file_dir_list, it contains key value pairs
   * such that key = num of chunk files within that directory, value = the 
   * original dst file name. 
   * For example, one entry in the list could be key = 3, value = /destdat/File1
   * 2. That entry tells us that after the mapper, we should have a directory
   * filename_chunkfiles in the tmp directory of the dst file system. 
   * Check whether that directory exist and check whether it contains 
   * number of file chunks that equals to the key value.
   * For example, we will check if there is a directory called
   * /destdat/tmp/File1_chunkfiles/, and if there are 3 files in that directory
   * 3. Concatenate the rest file chunks(if there is any) to the renamed 
   * chunk file 0
   * For examples:we will concatenate /destdat/tmp/File1_chunkfiles/1,
   * /destdat/tmp/File1_chunkfiles/2 to /destdat/tmp/File1_chunkfiles/0
   * 4. Rename the concatenated file
   * For examples: rename /destdat/tmp/File1_chunkfiles/0 to be
   * /destdat/File1
   * @throws IOException 
   */
  private static void stitchChunks(Configuration conf, JobConf jobConf,
      final Arguments args) throws IOException {
    //check if the file system is the dfs 
    FileSystem dstfs = args.dst.getFileSystem(conf);
    DistributedFileSystem dstdistfs = DFSUtil.convertToDFS(dstfs); 
    if(dstdistfs == null) {
      throw new IOException("No distributed file system found!");
    }
    else {
      int totfiles = jobConf.getInt(SRC_COUNT_LABEL, -1);
      String dstChunkFileDirList = jobConf.get(DST_CHUNK_FILE_LIST_LABEL);
      if ( "".equals(dstChunkFileDirList)) {
        throw new RuntimeException("Invalid chunk file dir list (" +
            dstChunkFileDirList + ")");
      }
      Path dstPath = args.dst;
      Path tmpPath = new Path(jobConf.get(TMP_DIR_LABEL));
      SequenceFile.Reader in = null;
      try {
        in = new SequenceFile.Reader(
            new Path(dstChunkFileDirList).getFileSystem(jobConf), 
            new Path(dstChunkFileDirList), jobConf);
        IntWritable chunkNum = new IntWritable();
        Text dstFilePathText = new Text();
        while (in.next(chunkNum, dstFilePathText)) {
          stitchChunkFile(conf, jobConf, dstdistfs, dstFilePathText.toString(),
              chunkNum.get(), dstPath, tmpPath, totfiles);
        }
      }
      finally{
        checkAndClose(in);
      }
    }
  }
  
  public static void stitchChunkFile(Configuration conf, JobConf jobConf,
      DistributedFileSystem dstdistfs,
      String dstFilePathStr, int chunkNum, Path dstPath, 
      Path tmpPath, int totFiles) throws IOException {
    
    Path dstFilePath = new Path(dstFilePathStr.toString());
    Path absDstPath = new Path(dstPath, dstFilePath);
    //path of directory that stores the chunk file in the tmp
    Path tmpChunkFileDir = new Path(tmpPath, dstFilePath + "_chunkfiles");
    if(!dstdistfs.exists(tmpChunkFileDir)){
      throw new IOException("Directory " + tmpChunkFileDir + 
      " storing chunk files doesn't exist");
    }
    FileStatus tmpChunkFileStatus = dstdistfs.getFileStatus(tmpChunkFileDir);
    if(!tmpChunkFileStatus.isDir()) {
      throw new IOException(tmpChunkFileStatus + "should be a directory");
    }
    //get the path to all the chunk filed
    Path [] chunkFiles = getChunkFilePaths(conf, dstdistfs, 
        tmpChunkFileDir, chunkNum);
    
    if (chunkFiles.length == 0) {
      // it means the file already been renamed in previous retry.
      // we simply do nothing here.
      return;
    }
    //using the concat method will change the orginal time we get for the
    //file. so we need to store the time before applying concat, and reset
    //it.
    FileStatus chunkFileStatus = dstdistfs.getFileStatus(chunkFiles[0]);
    long modification_time = chunkFileStatus.getModificationTime();
    long access_time = chunkFileStatus.getAccessTime();
    //copy a single file, the dst path is used as a dstfile name 
    //instead of a dst directory name
    if(totFiles == 1) {
      Path dstparent = absDstPath.getParent();
      if (!(dstdistfs.exists(dstparent) &&
          dstdistfs.getFileStatus(dstparent).isDir())) {
        absDstPath = dstparent;
      }
    }
    //concat only happens on files within the same directory
    //if there are more than one file concat the rest file chunks
    // to that file chunk0 and then rename filechunk 0
    if(chunkFiles.length > 1) {
      Path [] restChunkFiles = new Path[chunkFiles.length - 1];
      System.arraycopy(chunkFiles, 1, restChunkFiles, 0, 
          chunkFiles.length - 1);
      dstdistfs.concat(chunkFiles[0], restChunkFiles, true);
    }
    //only rename the file, after everything done the whole tmp will be
    //deleted
    renameAfterStitch(dstdistfs, chunkFiles[0], absDstPath);
    dstdistfs.setTimes(absDstPath, modification_time, access_time);
    
  }

  /**go to the directory we created for the chunk files
   * the chunk files are named as 0, 1, 2, 3....
   * For example, if a file File1 is chopped into 3 chunks, 
   * the we should have a directory /File1_chunkfiles, and
   * there are three files in that directory:
   * /File1_chunkfiles/0, /File1_chunkfiles/1, File1_chunkfiles/2
   * The returned chunkFilePath arrays contains the paths of 
   * those chunks in sorted order. Also we can make sure there is 
   * no missing chunks by checking the chunk file name .
   * For example, if we only have /File1_chunkfiles/0, File1_chunkfiles/2
   * we know that /File1_chunkfiles/1 is missing.
   * @param chunkFileDir the directory named with filename_chunkfiles
   * @return the paths to all the chunk files in the chunkFileDir
   * @throws IOException 
   */
  private static Path[] getChunkFilePaths(Configuration conf,
      FileSystem dstfs, Path chunkFileDir, int chunkNum) throws IOException{
    FileStatus [] chunkFileStatus = dstfs.listStatus(chunkFileDir);
    HashSet <String> chunkFilePathSet = new HashSet<String>(chunkFileStatus.length);
    for(FileStatus chunkfs:chunkFileStatus){
      chunkFilePathSet.add(chunkfs.getPath().toUri().getPath());
    }
    List<Path> chunkFilePathList = new ArrayList<Path>();
    Path verifiedPath = new Path(chunkFileDir, "verified");
    boolean needVerification = 
        !chunkFilePathSet.contains(verifiedPath.toUri().getPath());
    for(int i = 0; i < chunkNum; ++i) {
      //make sure we add the chunk file in order,and the chunk file name is 
      //named in number
      Path chunkFile = new Path(chunkFileDir, Integer.toString(i));
      //make sure the chunk file is not missing
      if(chunkFilePathSet.contains(chunkFile.toUri().getPath())) {
        chunkFilePathList.add(chunkFile);
      } else {
        if (needVerification) {
          throw new IOException("Chunk File: " + chunkFile.toUri().getPath() +
              "doesn't exist!");
        }
      }
    }
    if (needVerification) {
      // write the flag to indicate the map outputs have been verified.
      FSDataOutputStream out = dstfs.create(verifiedPath);
      out.close();
    }
    return chunkFilePathList.toArray(new Path[] {});
  }

  private static void updateDestStatus(FileStatus src, FileStatus dst,
      EnumSet<FileAttribute> preserved, FileSystem destFileSys
      ) throws IOException {
    String owner = null;
    String group = null;
    if (preserved.contains(FileAttribute.USER)
        && !src.getOwner().equals(dst.getOwner())) {
      owner = src.getOwner();
    }
    if (preserved.contains(FileAttribute.GROUP)
        && !src.getGroup().equals(dst.getGroup())) {
      group = src.getGroup();
    }
    if (owner != null || group != null) {
      destFileSys.setOwner(dst.getPath(), owner, group);
    }
    if (preserved.contains(FileAttribute.PERMISSION)
        && !src.getPermission().equals(dst.getPermission())) {
      destFileSys.setPermission(dst.getPath(), src.getPermission());
    }
    if (preserved.contains(FileAttribute.TIMES)) {
      try {
        destFileSys.setTimes(dst.getPath(), src.getModificationTime(), src.getAccessTime());
      } catch (IOException exc) {
        if (!dst.isDir()) { //hadoop 0.20 doesn't allow setTimes on dirs
          throw exc;
        }
      }
    }
  }
  
  private static void checkReplication(FileStatus src, FileStatus dst,
        EnumSet<FileAttribute> preserved, FileSystem destFileSys) 
        throws IOException {
    
    if ((preserved != null && preserved.contains(FileAttribute.REPLICATION))
        || src.isDir()) {
      return;
    }
    
    // if we do not preserve the replication from the src file,
    // set it to be the default one in the destination cluster (should be 3).
    short repl = destFileSys.getDefaultReplication();
    if (repl != dst.getReplication()) {
      destFileSys.setReplication(dst.getPath(), repl);
    }
  }

  private static void renameAfterStitch(FileSystem destFileSys, Path tmp, Path dst
  ) throws IOException{
    try {
      if (destFileSys.exists(dst)) {
        destFileSys.delete(dst, true);
      }
      if (!destFileSys.rename(tmp, dst)) {
        throw new IOException();
      }
    }
    catch(IOException cause) {
      throw (IOException)new IOException("Fail to rename tmp file (=" + tmp 
          + ") to destination file (=" + dst + ")").initCause(cause);
    }
  }

  static private void finalize(Configuration conf, JobConf jobconf,
      final Path destPath, String preservedAttributes) throws IOException {
    if (preservedAttributes == null) {
      return;
    }
    EnumSet<FileAttribute> preserved = FileAttribute.parse(preservedAttributes);
    if (!preserved.contains(FileAttribute.USER)
        && !preserved.contains(FileAttribute.GROUP)
        && !preserved.contains(FileAttribute.PERMISSION)) {
      return;
    }

    FileSystem dstfs = destPath.getFileSystem(conf);
    Path dstdirlist = new Path(jobconf.get(DST_DIR_LIST_LABEL));
    SequenceFile.Reader in = null;
    try {
      in = new SequenceFile.Reader(dstdirlist.getFileSystem(jobconf),
          dstdirlist, jobconf);
      Text dsttext = new Text();
      FilePairComparable pair = new FilePairComparable(); 
      for(; in.next(dsttext, pair); ) {
        Path absdst = new Path(destPath, pair.output);
        updateDestStatus(pair.input, dstfs.getFileStatus(absdst),
            preserved, dstfs);
      }
    } finally {
      checkAndClose(in);
    }
  }

  static public class Arguments {
    final List<Path> srcs;
    final Path basedir;
    final Path dst;
    final Path log;
    final EnumSet<Options> flags;
    final String preservedAttributes;
    final long filelimit;
    final long sizelimit;
    final String mapredSslConf;
    
    /**
     * Arguments for distcp
     * @param srcs List of source paths
     * @param basedir Base directory for copy
     * @param dst Destination path
     * @param log Log output directory
     * @param flags Command-line flags
     * @param preservedAttributes Preserved attributes 
     * @param filelimit File limit
     * @param sizelimit Size limit
     */
    public Arguments(List<Path> srcs, Path basedir, Path dst, Path log,
        EnumSet<Options> flags, String preservedAttributes,
        long filelimit, long sizelimit, String mapredSslConf) {
      this.srcs = srcs;
      this.basedir = basedir;
      this.dst = dst;
      this.log = log;
      this.flags = flags;
      this.preservedAttributes = preservedAttributes;
      this.filelimit = filelimit;
      this.sizelimit = sizelimit;
      this.mapredSslConf = mapredSslConf;
      
      if (LOG.isTraceEnabled()) {
        LOG.trace("this = " + this);
      }
    }

    static Arguments valueOf(String[] args, Configuration conf
        ) throws IOException {
      List<Path> srcs = new ArrayList<Path>();
      Path dst = null;
      Path log = null;
      Path basedir = null;
      EnumSet<Options> flags = EnumSet.noneOf(Options.class);
      String preservedAttributes = null;
      String mapredSslConf = null;
      long filelimit = Long.MAX_VALUE;
      long sizelimit = Long.MAX_VALUE;
      int tosValue = DEFAULT_TOS_VALUE;

      for (int idx = 0; idx < args.length; idx++) {
        Options[] opt = Options.values();
        int i = 0;
        for(; i < opt.length && !args[idx].startsWith(opt[i].cmd); i++);

        if (i < opt.length) {
          flags.add(opt[i]);
          if (opt[i] == Options.PRESERVE_STATUS) {
            preservedAttributes =  args[idx].substring(2);         
            FileAttribute.parse(preservedAttributes); //validation
          }
          else if (opt[i] == Options.FILE_LIMIT) {
            filelimit = Options.FILE_LIMIT.parseLong(args, ++idx);
          }
          else if (opt[i] == Options.SIZE_LIMIT) {
            sizelimit = Options.SIZE_LIMIT.parseLong(args, ++idx);
          }
        } else if ("-f".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("urilist_uri not specified in -f");
          }
          srcs.addAll(fetchFileList(conf, new Path(args[idx])));
        } else if ("-log".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("logdir not specified in -log");
          }
          log = new Path(args[idx]);
        } else if ("-basedir".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("basedir not specified in -basedir");
          }
          basedir = new Path(args[idx]);
        } else if ("-mapredSslConf".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("ssl conf file not specified in -mapredSslConf");
          }
          mapredSslConf = args[idx];
        } else if ("-m".equals(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException("num_maps not specified in -m");
          }
          try {
            conf.setInt(MAX_MAPS_LABEL, Integer.valueOf(args[idx]));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid argument to -m: " +
                                               args[idx]);
          }
        } else if ("-maxfiles".equals(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException("max_files_per_mapper not specified in -maxfiles");
          }
          try {
            conf.setInt(MAX_FILES_PER_MAPPER_LABEL, Integer.valueOf(args[idx]));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid argument to -maxfiles: " +
                args[idx]);
          }
        } else if ("-r".equals(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException(
                "num_reducers not specified in -r");
          }
          try {
            conf.setInt(MAX_REDUCE_LABEL, Integer.valueOf(args[idx]));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid argument to -m: "
                + args[idx]);
          }
        } else if ("-maxfiles_r".equals(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException("max_files_per_reducer not specified in -maxfiles_r");
          }
          try {
            conf.setInt(MAX_FILES_PER_REDUCER_LABEL, Integer.valueOf(args[idx]));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid argument to -maxfiles_r: " +
                args[idx]);
          }
        } else if ("-tos".equals(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException(
                "tos value not specified in -tos");
          }
          try {
            int value = Integer.valueOf(args[idx]);
            if (NetUtils.isValidTOSValue(value)) {
              tosValue = value;
            } else {
              throw new IllegalArgumentException(
                  "Invalid argument to -tos: " + args[idx]);
            }
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid argument to -tos: "
                + args[idx]);
          }
        } else if ('-' == args[idx].codePointAt(0)) {
          throw new IllegalArgumentException("Invalid switch " + args[idx]);
        } else if (idx == args.length -1) {
          dst = new Path(args[idx]);
        } else {
          srcs.add(new Path(args[idx]));
        }
      }
      
      // overwrite the tos value
      conf.setInt(NetUtils.DFS_CLIENT_TOS_CONF, tosValue);
      // mandatory command-line parameters
      if (srcs.isEmpty() || dst == null) {
        throw new IllegalArgumentException("Missing "
            + (dst == null ? "dst path" : "src"));
      }
      // incompatible command-line flags
      final boolean isOverwrite = flags.contains(Options.OVERWRITE);
      final boolean isUpdate = flags.contains(Options.UPDATE);
      final boolean isDelete = flags.contains(Options.DELETE);
      final boolean skipCRC = flags.contains(Options.SKIPCRC);
      final boolean copyByChunk = flags.contains(Options.COPYBYCHUNK); 
      final boolean useFastCopy = flags.contains(Options.USEFASTCOPY);
        
      if (isOverwrite && isUpdate) {
        throw new IllegalArgumentException("Conflicting overwrite policies");
      }
      if (isDelete && !isOverwrite && !isUpdate) {
        throw new IllegalArgumentException(Options.DELETE.cmd
            + " must be specified with " + Options.OVERWRITE + " or "
            + Options.UPDATE + ".");
      }
      if (!isUpdate && skipCRC) {
        throw new IllegalArgumentException(
            Options.SKIPCRC.cmd + " is relevant only with the " +
            Options.UPDATE.cmd + " option");
      }
      if (isUpdate && copyByChunk)  {
        throw new IllegalArgumentException(
            Options.UPDATE.cmd + " cannot be used with " +
            Options.COPYBYCHUNK.cmd + ".");       
      }
      if (copyByChunk && useFastCopy) {
        throw new IllegalArgumentException(
            Options.USEFASTCOPY.cmd + " cannot be used with " +
            Options.COPYBYCHUNK.cmd + ".");
      }
      
      return new Arguments(srcs, basedir, dst, log, flags, preservedAttributes,
          filelimit, sizelimit, mapredSslConf);
    }
    
    /** {@inheritDoc} */
    public String toString() {
      return getClass().getName() + "{"
          + "\n  srcs = " + srcs 
          + "\n  dst = " + dst 
          + "\n  log = " + log 
          + "\n  flags = " + flags
          + "\n  preservedAttributes = " + preservedAttributes 
          + "\n  filelimit = " + filelimit 
          + "\n  sizelimit = " + sizelimit
          + "\n  mapredSslConf = " + mapredSslConf
          + "\n}"; 
    }
  }

  /**
   * This is the main driver for recursively copying directories
   * across file systems. It takes at least two cmdline parameters. A source
   * URL and a destination URL. It then essentially does an "ls -lR" on the
   * source URL, and writes the output in a round-robin manner to all the map
   * input files. The mapper actually copies the files allotted to it. The
   * reduce is empty.
   */
  public int run(String[] args) {
    try {
      copy(conf, Arguments.valueOf(args, conf));
      return 0;
    } catch (IllegalArgumentException e) {
      System.err.println(StringUtils.stringifyException(e) + "\n" + usage);
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    } catch (InvalidInputException e) {
      System.err.println(StringUtils.stringifyException(e) + "\n");
      return -1;
    } catch (DuplicationException e) {
      System.err.println(StringUtils.stringifyException(e));
      return DuplicationException.ERROR_CODE;
    } catch (RemoteException e) {
      final IOException unwrapped = e.unwrapRemoteException(
          FileNotFoundException.class, 
          AccessControlException.class,
          QuotaExceededException.class);
      System.err.println(StringUtils.stringifyException(unwrapped));
      return -3;
    } catch (Exception e) {
      System.err.println("With failures, global counters are inaccurate; " +
          "consider running with -i");
      System.err.println("Copy failed: " + StringUtils.stringifyException(e));
      return -999;
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf job = new JobConf(DistCp.class);
    DistCp distcp = new DistCp(job);
    int res = ToolRunner.run(distcp, args);
    System.exit(res);
  }

  /**
   * Make a path relative with respect to a root path.
   * absPath is always assumed to descend from root.
   * Otherwise returned path is null.
   */
  static String makeRelative(Path root, Path absPath) {
    if (!absPath.isAbsolute()) {
      throw new IllegalArgumentException("!absPath.isAbsolute(), absPath="
          + absPath);
    }
    String p = absPath.toUri().getPath();

    StringTokenizer pathTokens = new StringTokenizer(p, "/");
    for(StringTokenizer rootTokens = new StringTokenizer(
        root.toUri().getPath(), "/"); rootTokens.hasMoreTokens(); ) {
      if (!rootTokens.nextToken().equals(pathTokens.nextToken())) {
        return null;
      }
    }
    StringBuilder sb = new StringBuilder();
    for(; pathTokens.hasMoreTokens(); ) {
      sb.append(pathTokens.nextToken());
      if (pathTokens.hasMoreTokens()) { sb.append(Path.SEPARATOR); }
    }
    return sb.length() == 0? ".": sb.toString();
  }

  /**
   * Calculate how many maps to run.
   * Number of maps is bounded by a minimum of the cumulative size of the
   * copy / (distcp.bytes.per.map, default BYTES_PER_MAP or -m on the
   * command line) and at most (distcp.max.map.tasks, default
   * MAX_MAPS_PER_NODE * nodes in the cluster).
   * @param fileCount  Count of total files for job.
   * @param totalBytes Count of total bytes for job
   * @param job The job to configure
   * @param client JobClient object to access the cluster
   * @return Count of maps to run.
   */
  private static int setMapCount(long fileCount, long totalBytes, 
                  JobConf job, JobClient client)
      throws IOException {
    int numMaps = Math.max(
      (int)(totalBytes / job.getLong(BYTES_PER_MAP_LABEL, BYTES_PER_MAP)), 
      (int)(fileCount / job.getInt(MAX_FILES_PER_MAPPER_LABEL, 
                                    MAX_FILES_PER_MAPPER_DEFAULT)));
        
    int numTasks = MAX_MAPS_DEFAULT;
    try {
      numTasks = client.getClusterStatus().getTaskTrackers();
    } catch (UnsupportedOperationException uex) {
      // This is corona client that does not support the getClusterStatus()
    }

    numMaps = Math.min(numMaps, 
        job.getInt(MAX_MAPS_LABEL, MAX_MAPS_PER_NODE * numTasks));
    job.setNumMapTasks(Math.max(numMaps, 1));
    return Math.max(numMaps, 1);
  }

  /** Fully delete dir */
  static void fullyDelete(String dir, Configuration conf) throws IOException {
    if (dir != null) {
      Path tmp = new Path(dir);
      tmp.getFileSystem(conf).delete(tmp, true);
    }
  }
  
  /**
   * Calculate how many reducers to run.
   * @param fileCount
   * @param job
   * @param client
   * @return
   */
  private static int setReducerCount(long fileCount, JobConf job, 
      JobClient client) {
    // calculate the max number of reducers.
    int numReducers = Math.max(job.getInt(MAX_REDUCE_LABEL, MAX_REDUCERS_DEFAULT),
        (int) (fileCount / job.getInt(MAX_FILES_PER_REDUCER_LABEL, 
            MAX_FILES_PER_REDUCER_DEFAULT)));
    
    // make sure we at least have 1.
    numReducers = Math.max(numReducers, 1);
    job.setNumReduceTasks(numReducers);
    return numReducers;
  }

  //Job configuration
  private static JobConf createJobConf(Configuration conf, boolean useFastCopy) {
    Class<? extends InputFormat> inputFormat =
      (useFastCopy) ? FastCopyInputFormat.class : CopyInputFormat.class;
    JobConf jobconf = new JobConf(conf, DistCp.class);
    jobconf.setJobName(NAME);

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobconf.setReduceSpeculativeExecution(false);
    jobconf.setMapOutputKeyClass(FilePairComparable.class);
    jobconf.setMapOutputValueClass(Text.class);
    jobconf.setOutputKeyClass(FilePairComparable.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setInputFormat(inputFormat);
    jobconf.setMapperClass(CopyFilesTask.class);
    jobconf.setReducerClass(CopyFilesTask.class);
      
    // Prevent the reducer from starting until all maps are done.
    jobconf.setInt("mapred.job.rushreduce.reduce.threshold", 0);
    jobconf.setFloat("mapred.reduce.slowstart.completed.maps", 1.0f);
    
    return jobconf;
  }

  //Job configuration
  @SuppressWarnings("deprecation")
  private static JobConf createJobConfForCopyByChunk(Configuration conf) {
    JobConf jobconf = new JobConf(conf, DistCp.class);
    jobconf.setJobName(NAME);

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobconf.setReduceSpeculativeExecution(false);

    jobconf.setMapOutputKeyClass(Text.class);
    jobconf.setMapOutputValueClass(IntWritable.class);
    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setInputFormat(CopyByChunkInputFormat.class);
    jobconf.setMapperClass(CopyFilesByChunkMapper.class);
    jobconf.setReducerClass(CopyFilesByChunkReducer.class);
    
    // Prevent the reducer from starting until all maps are done.
    jobconf.setInt("mapred.job.rushreduce.reduce.threshold", 0);
    jobconf.setFloat("mapred.reduce.slowstart.completed.maps", 1.0f);
    
    return jobconf;
  }
  
  private static final Random RANDOM = new Random();
  public static String getRandomId() {
    return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
  }
  
  /**
   * Initialize DFSCopyFileMapper specific job-configuration.
   * @param conf : The dfs/mapred configuration.
   * @param jobConf : The handle to the jobConf object to be initialized.
   * @param args Arguments
   * @return true if it is necessary to launch a job.
   */
  private static boolean setup(Configuration conf, JobConf jobConf,
                            JobClient client, final Arguments args,
                            boolean useFastCopy)
      throws IOException {
    jobConf.set(DST_DIR_LABEL, args.dst.toUri().toString());

    //set boolean values
    final boolean update = args.flags.contains(Options.UPDATE);
    final boolean skipCRCCheck = args.flags.contains(Options.SKIPCRC);
    final boolean overwrite = !update && args.flags.contains(Options.OVERWRITE);
    final boolean skipUnderConstructionFile = 
        args.flags.contains(Options.SKIPUNDERCONSTRUCTION);
    jobConf.setBoolean(Options.UPDATE.propertyname, update);
    jobConf.setBoolean(Options.SKIPCRC.propertyname, skipCRCCheck);
    jobConf.setBoolean(Options.OVERWRITE.propertyname, overwrite);
    jobConf.setBoolean(Options.IGNORE_READ_FAILURES.propertyname,
        args.flags.contains(Options.IGNORE_READ_FAILURES));
    jobConf.setBoolean(Options.PRESERVE_STATUS.propertyname,
        args.flags.contains(Options.PRESERVE_STATUS));
    jobConf.setBoolean(Options.USEFASTCOPY.propertyname, useFastCopy);
    jobConf.setBoolean(Options.SKIPUNDERCONSTRUCTION.propertyname, 
              skipUnderConstructionFile);

    final String randomId = getRandomId();
    Path jobDirectory = new Path(client.getSystemDir(), NAME + "_" + randomId);
    jobConf.set(JOB_DIR_LABEL, jobDirectory.toString());

    FileSystem dstfs = args.dst.getFileSystem(conf);
    boolean dstExists = dstfs.exists(args.dst);
    boolean dstIsDir = false;
    if (dstExists) {
      dstIsDir = dstfs.getFileStatus(args.dst).isDir();
    }

    // default logPath
    Path logPath = args.log; 
    if (logPath == null) {
      String filename = "_distcp_logs_" + randomId;
      if (!dstExists || !dstIsDir) {
        Path parent = args.dst.getParent();
        if (!dstfs.exists(parent)) {
          dstfs.mkdirs(parent);
        }
        logPath = new Path(parent, filename);
      } else {
        logPath = new Path(args.dst, filename);
      }
    }
    FileOutputFormat.setOutputPath(jobConf, logPath);

    // create src list, dst list
    FileSystem jobfs = jobDirectory.getFileSystem(jobConf);

    Path srcfilelist = new Path(jobDirectory, "_distcp_src_files");
    jobConf.set(SRC_LIST_LABEL, srcfilelist.toString());
    SequenceFile.Writer src_writer = SequenceFile.createWriter(jobfs, jobConf,
        srcfilelist, LongWritable.class, FilePairComparable.class,
        jobfs.getConf().getInt("io.file.buffer.size", 4096),
        SRC_FILES_LIST_REPL_DEFAULT, jobfs.getDefaultBlockSize(),
        SequenceFile.CompressionType.NONE, 
        new DefaultCodec(), null, new Metadata());

    Path dstfilelist = new Path(jobDirectory, "_distcp_dst_files");
    SequenceFile.Writer dst_writer = SequenceFile.createWriter(jobfs, jobConf,
        dstfilelist, Text.class, Text.class,
        SequenceFile.CompressionType.NONE);

    Path dstdirlist = new Path(jobDirectory, "_distcp_dst_dirs");
    jobConf.set(DST_DIR_LIST_LABEL, dstdirlist.toString());
    SequenceFile.Writer dir_writer = SequenceFile.createWriter(jobfs, jobConf,
        dstdirlist, Text.class, FilePairComparable.class,
        SequenceFile.CompressionType.NONE);

    // handle the case where the destination directory doesn't exist
    // and we've only a single src directory OR we're updating/overwriting
    // the contents of the destination directory.
    final boolean special =
      (args.srcs.size() == 1 && !dstExists) || update || overwrite;
    int srcCount = 0, cnsyncf = 0, dirsyn = 0;
    long fileCount = 0L, dirCount = 0L, byteCount = 0L, cbsyncs = 0L, blockCount = 0L;

    Path basedir = null;
    HashSet<Path> parentDirsToCopy = new HashSet<Path>();
    if (args.basedir != null) {
      FileSystem basefs = args.basedir.getFileSystem(conf);
      basedir = args.basedir.makeQualified(basefs);
      if (!basefs.isDirectory(basedir)) {
        throw new IOException("Basedir " + basedir + " is not a directory.");
      }
    }

    try {
      for(Iterator<Path> srcItr = args.srcs.iterator(); srcItr.hasNext(); ) {
        final Path src = srcItr.next();
        FileSystem srcfs = src.getFileSystem(conf);
        FileStatus srcfilestat = srcfs.getFileStatus(src);
        Path root = special && srcfilestat.isDir()? src: src.getParent();

        if (basedir != null) {
          root = basedir;
          Path parent = src.getParent().makeQualified(srcfs);
          while (parent != null && !parent.equals(basedir)) {
            if (!parentDirsToCopy.contains(parent)){
              parentDirsToCopy.add(parent);
              String dst = makeRelative(root, parent);
              FileStatus pst = srcfs.getFileStatus(parent);
              src_writer.append(new LongWritable(0), new FilePairComparable(pst, dst));
              dst_writer.append(new Text(dst), new Text(parent.toString()));
              dir_writer.append(new Text(dst), new FilePairComparable(pst, dst));
              if (++dirsyn > SYNC_FILE_MAX) {
                dirsyn = 0;
                dir_writer.sync();
              }
            }
            parent = parent.getParent();
          }

          if (parent == null) {
            throw new IOException("Basedir " + basedir +
                " is not a prefix of source path " + src);
          }
        }

        if (srcfilestat.isDir()) {
          ++srcCount;
          ++dirCount;
          final String dst = makeRelative(root,src);
          src_writer.append(new LongWritable(0), new FilePairComparable(srcfilestat, dst));
          dst_writer.append(new Text(dst), new Text(src.toString()));
        }

        Stack<FileStatus> pathstack = new Stack<FileStatus>();
        for(pathstack.push(srcfilestat); !pathstack.empty(); ) {
          FileStatus cur = pathstack.pop();
          FileStatus[] children = srcfs.listStatus(cur.getPath());
          for(int i = 0; i < children.length; i++) {
            boolean skipfile = false;
            final FileStatus child = children[i]; 
            final String dst = makeRelative(root, child.getPath());
            ++srcCount;

            if (child.isDir()) {
              pathstack.push(child);
              ++dirCount;
            }
            else {
              //skip file if it exceed file limit or size limit.
              //check on whether src and dest files are same will be
              //done in mapper. 
              skipfile |= fileCount == args.filelimit
                          || byteCount + child.getLen() > args.sizelimit; 

              if (!skipfile) {
                ++fileCount;
                byteCount += child.getLen();
                blockCount += getBlocks(child.getLen(), child.getBlockSize());

                if (LOG.isTraceEnabled()) {
                  LOG.trace("adding file " + child.getPath());
                }

                ++cnsyncf;
                cbsyncs += child.getLen();
                if (cnsyncf > SYNC_FILE_MAX || cbsyncs > BYTES_PER_MAP) {
                  src_writer.sync();
                  dst_writer.sync();
                  cnsyncf = 0;
                  cbsyncs = 0L;
                }
              }
            }

            if (!skipfile) {
              src_writer.append(new LongWritable(child.isDir()? 0: child.getLen()),
                  new FilePairComparable(child, dst));
            }

            dst_writer.append(new Text(dst),
                new Text(child.getPath().toString()));
          }

          if (cur.isDir()) {
            String dst = makeRelative(root, cur.getPath());
            dir_writer.append(new Text(dst), new FilePairComparable(cur, dst));
            if (++dirsyn > SYNC_FILE_MAX) {
              dirsyn = 0;
              dir_writer.sync();                
            }
          }
        }
      }
    } finally {
      checkAndClose(src_writer);
      checkAndClose(dst_writer);
      checkAndClose(dir_writer);
    }

    FileStatus dststatus = null;
    try {
      dststatus = dstfs.getFileStatus(args.dst);
    } catch(FileNotFoundException fnfe) {
      LOG.info(args.dst + " does not exist.");
    }

    // create dest path dir if copying > 1 file
    if (dststatus == null) {
      if (srcCount > 1 && !dstfs.mkdirs(args.dst)) {
        throw new IOException("Failed to create" + args.dst);
      }
    }
    
    final Path sorted = new Path(jobDirectory, "_distcp_sorted"); 
    checkDuplication(jobfs, dstfilelist, sorted, conf);

    if (dststatus != null && args.flags.contains(Options.DELETE)) {
      deleteNonexisting(dstfs, dststatus, sorted,
          jobfs, jobDirectory, jobConf, conf);
    }

    Path tmpDir = new Path(
        (dstExists && !dstIsDir) || (!dstExists && srcCount == 1)?
        args.dst.getParent(): args.dst, "_distcp_tmp_" + randomId);
    jobConf.set(TMP_DIR_LABEL, tmpDir.toUri().toString());
    LOG.info("sourcePathsCount=" + srcCount);
    LOG.info("filesToCopyCount=" + fileCount);
    LOG.info("bytesToCopyCount=" + StringUtils.humanReadableInt(byteCount));
    jobConf.setInt(SRC_COUNT_LABEL, srcCount);
    jobConf.setLong(TOTAL_SIZE_LABEL, byteCount);
    jobConf.setLong(TOTAL_BLOCKS_LABEL, blockCount);
    setMapCount(fileCount, byteCount, jobConf, client);
    setReducerCount(fileCount, jobConf, client);
    return fileCount > 0 || dirCount > 0;
  }

  /**
   * Initialize DFSCopyFileMapper specific job-configuration.
   * @param conf : The dfs/mapred configuration.
   * @param jobConf : The handle to the jobConf object to be initialized.
   * @param args Arguments
   * @return true if it is necessary to launch a job.
   */
  private static boolean setupForCopyByChunk(
              Configuration conf, JobConf jobConf,
              JobClient client, final Arguments args)
      throws IOException {
    jobConf.set(DST_DIR_LABEL, args.dst.toUri().toString());

    //set boolean values
    final boolean update = args.flags.contains(Options.UPDATE);
    final boolean skipCRCCheck = args.flags.contains(Options.SKIPCRC);
    final boolean overwrite = !update && args.flags.contains(Options.OVERWRITE);
    final boolean skipUnderConstructionFile = 
        args.flags.contains(Options.SKIPUNDERCONSTRUCTION);
    
    jobConf.setBoolean(Options.UPDATE.propertyname, update);
    jobConf.setBoolean(Options.SKIPCRC.propertyname, skipCRCCheck);
    jobConf.setBoolean(Options.OVERWRITE.propertyname, overwrite);
    jobConf.setBoolean(Options.IGNORE_READ_FAILURES.propertyname,
        args.flags.contains(Options.IGNORE_READ_FAILURES));
    jobConf.setBoolean(Options.PRESERVE_STATUS.propertyname,
        args.flags.contains(Options.PRESERVE_STATUS));

    final String randomId = getRandomId();
    Path jobDirectory = new Path(client.getSystemDir(), NAME + "_" + randomId);
    jobConf.set(JOB_DIR_LABEL, jobDirectory.toString());
    
    FileSystem dstfs = args.dst.getFileSystem(conf);
    
    boolean dstExists = dstfs.exists(args.dst);
    boolean dstIsDir = false;
    if (dstExists) {
      dstIsDir = dstfs.getFileStatus(args.dst).isDir();
    }

    // default logPath
    Path logPath = args.log; 
    if (logPath == null) {
      String filename = "_distcp_logs_" + randomId;
      if (!dstExists || !dstIsDir) {
        Path parent = args.dst.getParent();
        if (!dstfs.exists(parent)) {
          dstfs.mkdirs(parent);
        }
        logPath = new Path(parent, filename);
      } else {
        logPath = new Path(args.dst, filename);
      }
    }
    FileOutputFormat.setOutputPath(jobConf, logPath);

    // create src list, dst list
    FileSystem jobfs = jobDirectory.getFileSystem(jobConf);

    //for creating splits
    ArrayList<FilePairComparable> filePairList = new ArrayList<FilePairComparable> ();
    ArrayList<LongWritable> fileLengthList = new ArrayList<LongWritable> ();

    //for sorting, checking duplication
    Path dstfilelist = new Path(jobDirectory, "_distcp_dst_files");
    SequenceFile.Writer dst_writer = SequenceFile.createWriter(jobfs, jobConf,
        dstfilelist, Text.class, Text.class,
        SequenceFile.CompressionType.NONE);

    //for finalize, updated the preserved attributes
    Path dstdirlist = new Path(jobDirectory, "_distcp_dst_dirs");
    jobConf.set(DST_DIR_LIST_LABEL, dstdirlist.toString());
    SequenceFile.Writer dir_writer = SequenceFile.createWriter(jobfs, jobConf,
        dstdirlist, Text.class, FilePairComparable.class,
        SequenceFile.CompressionType.NONE);

    // handle the case where the destination directory doesn't exist
    // and we've only a single src directory OR we're updating/overwriting
    // the contents of the destination directory.
    final boolean special =
      (args.srcs.size() == 1 && !dstExists) || update || overwrite;
    int srcCount = 0, cnsyncf = 0, dirsyn = 0;
    long fileCount = 0L, dirCount = 0L, byteCount = 0L, cbsyncs = 0L;

    Path basedir = null;
    HashSet<Path> parentDirsToCopy = new HashSet<Path>();
    if (args.basedir != null) {
      FileSystem basefs = args.basedir.getFileSystem(conf);
      basedir = args.basedir.makeQualified(basefs);
      if (!basefs.isDirectory(basedir)) {
        throw new IOException("Basedir " + basedir + " is not a directory.");
      }
    }

    try {
      for(Iterator<Path> srcItr = args.srcs.iterator(); srcItr.hasNext(); ) {
        final Path src = srcItr.next();
        FileSystem srcfs = src.getFileSystem(conf);
        FileStatus srcfilestat = srcfs.getFileStatus(src);
        Path root = special && srcfilestat.isDir()? src: src.getParent();

        if (basedir != null) {
          root = basedir;
          Path parent = src.getParent().makeQualified(srcfs);
          while (parent != null && !parent.equals(basedir)) {
            if (!parentDirsToCopy.contains(parent)){
              parentDirsToCopy.add(parent);
              String dst = makeRelative(root, parent);
              FileStatus pst = srcfs.getFileStatus(parent);
              filePairList.add(new FilePairComparable(pst, dst));
              fileLengthList.add(new LongWritable(0));
              dst_writer.append(new Text(dst), new Text(parent.toString()));
              dir_writer.append(new Text(dst), new FilePairComparable(pst, dst));
              if (++dirsyn > SYNC_FILE_MAX) {
                dirsyn = 0;
                dir_writer.sync();
              }
            }
            parent = parent.getParent();
          }

          if (parent == null) {
            throw new IOException("Basedir " + basedir +
                " is not a prefix of source path " + src);
          }
        }

        if (srcfilestat.isDir()) {
          ++srcCount;
          ++dirCount;
          final String dst = makeRelative(root,src);
          filePairList.add(new FilePairComparable(srcfilestat, dst));
          fileLengthList.add(new LongWritable(0));
          dst_writer.append(new Text(dst), new Text(src.toString()));
        }

        Stack<FileStatus> pathstack = new Stack<FileStatus>();
        for(pathstack.push(srcfilestat); !pathstack.empty(); ) {
          FileStatus cur = pathstack.pop();
          FileStatus[] children = srcfs.listStatus(cur.getPath());
          for(int i = 0; i < children.length; i++) {
            boolean skipfile = false;
            final FileStatus child = children[i];
            final String dst = makeRelative(root, child.getPath());
            ++srcCount;

            if (child.isDir()) {
              pathstack.push(child);
              ++dirCount;
            }
            else {
              //skip file if it exceed file limit or size limit.
              //check on whether src and dest files are same will be
              //done in mapper. 
              skipfile |= fileCount == args.filelimit
                          || byteCount + child.getLen() > args.sizelimit; 

              if (!skipfile) {
                ++fileCount;
                byteCount += child.getLen();

                if (LOG.isTraceEnabled()) {
                  LOG.trace("adding file " + child.getPath());
                }

                ++cnsyncf;
                cbsyncs += child.getLen();
                if (cnsyncf > SYNC_FILE_MAX || cbsyncs > BYTES_PER_MAP) {
                  dst_writer.sync();
                  cnsyncf = 0;
                  cbsyncs = 0L;
                }
              }
            }

            if (!skipfile) {
              filePairList.add(new FilePairComparable(child, dst));
              fileLengthList.add(
                  new LongWritable(child.isDir()? 0: child.getLen()));
               
            }

            dst_writer.append(new Text(dst),
                new Text(child.getPath().toString()));
          }

          if (cur.isDir()) {
            String dst = makeRelative(root, cur.getPath());
            dir_writer.append(new Text(dst), new FilePairComparable(cur, dst));
            if (++dirsyn > SYNC_FILE_MAX) {
              dirsyn = 0;
              dir_writer.sync();                
            }
          }
        }
      }
    } finally {
      checkAndClose(dst_writer);
      checkAndClose(dir_writer);
    }

    FileStatus dststatus = null;
    try {
      dststatus = dstfs.getFileStatus(args.dst);
    } catch(FileNotFoundException fnfe) {
      LOG.info(args.dst + " does not exist.");
    }

    // create dest path dir if copying > 1 file
    if (dststatus == null) {
      if (srcCount > 1 && !dstfs.mkdirs(args.dst)) {
        throw new IOException("Failed to create" + args.dst);
      }
    }
    
    final Path sorted = new Path(jobDirectory, "_distcp_sorted"); 
    checkDuplication(jobfs, dstfilelist, sorted, conf);

    if (dststatus != null && args.flags.contains(Options.DELETE)) {
      deleteNonexisting(dstfs, dststatus, sorted,
          jobfs, jobDirectory, jobConf, conf);
    }

    Path tmpDir = new Path(
        (dstExists && !dstIsDir) || (!dstExists && srcCount == 1)?
        args.dst.getParent(): args.dst, "_distcp_tmp_" + randomId);
    jobConf.set(TMP_DIR_LABEL, tmpDir.toUri().toString());
    LOG.info("sourcePathsCount=" + srcCount);
    LOG.info("filesToCopyCount=" + fileCount);
    LOG.info("bytesToCopyCount=" + StringUtils.humanReadableInt(byteCount));
    jobConf.setInt(SRC_COUNT_LABEL, srcCount);
    jobConf.setLong(TOTAL_SIZE_LABEL, byteCount);
    int numOfMaps = setMapCount(fileCount, byteCount, jobConf, client);
    setReducerCount(fileCount, jobConf, client);
    long targetSize = byteCount / numOfMaps;
    LOG.info("Num of Maps : " + numOfMaps + " Target Size : " + targetSize);
    createFileChunkList(jobConf, jobDirectory, jobfs, filePairList, 
        fileLengthList, targetSize, skipUnderConstructionFile);
    return fileCount > 0 || dirCount > 0;
  }

  /** 
   * <p>It contains two steps:<p>
   * 
   * <p>step 1. change the src file list into the src chunk file list
   * src file list is a list of (LongWritable, FilePair). LongWritable is
   * the length of the file while FilePair contains info of src and dst.
   * In order to copy file by chunks, we want to convert src file list into
   * src chunk file list, which is a list of (LongWritable, FileChunkPair)
   * Files are chopped into chunks based on follow rule:
   * Given the targeSize, which can be calculated based on the num of splits
   * and tells how many bytes each mapper roughly wants to copy, we try
   * to fill the targeSize by file blocks. 
   * A chunk will be generated either when 
   * i) we meet the targetSize, this mapper is full. or 
   * ii) it's already the end of the file
   * For example, the targeSize is 2000, and we have two files with 
   * size 980(fileA) and 3200(fileB) respectively, both of them has 
   * block size 512. For fileA, we have 2 blocks: one of size 512, 
   * the other of size 468. 512 + 468 < 2000, so we put both 
   * of them in the 1st mapper, which means fileA only has 1 chunk file. 
   * We still have 2000 - 980 = 1020 bytes left for the 1st mapper, 
   * so we will keep feed blocks from fileB to this mapper until 
   * it exceed the target size. In this case, we can still feed 2 more 
   * blocks from fileB to the 1st mapper. And those 2 blocks
   * will be the 1st chunk of fileB(with offset = 0, length = 1024). 
   * For the rest of fileB, we will create another split for 2nd mapper,
   * and they will be the 2nd chunk of fileB(with offset = 1024, 
   * length = 2176)<p>
   * 
   * <p>step 2. generate splits from src chunk file list
   * go through the src chunk file list, and generate FileSplit by providing
   * the starting pos and splitsize<p>
   * 
   * <p>At the same time, another list called dst chunk file dir list is 
   * generated.<p>
   * 
   * <p>When the -copybychunk flag is turned on, for each file, we will create
   * a directory in the dst file system first. For example, for fileA, we 
   * will create a directry named fileA_chunkfiles, which contains all the
   * file chunks with name 0, 1, 2.. dst chunk file dir list contains the
   * original file name, and how many chunks each dir contains. We will 
   * use this list when stitching file chunks together after all the 
   * mappers are done.<p>
   */
  static private void createFileChunkList(JobConf job, Path jobDirectory, 
      FileSystem jobfs, ArrayList<FilePairComparable> filePairList, 
      ArrayList<LongWritable> fileLengthList,
      long targetSize, boolean skipUnderConstructionFile)
      throws IOException{
    boolean preserve_status = 
      job.getBoolean(Options.PRESERVE_STATUS.propertyname, false);
    EnumSet<FileAttribute> preserved = null;
    boolean preserve_block_size = false;
    if (preserve_status) {
      preserved = FileAttribute.parse(job.get(PRESERVE_STATUS_LABEL));
      if(preserved.contains(FileAttribute.BLOCK_SIZE)) {
        preserve_block_size = true;
      }
    }
    
    Path destPath = new Path(job.get(DST_DIR_LABEL, "/"));
    FileSystem destFileSys;
    try {
      destFileSys = destPath.getFileSystem(job);
    } catch (IOException ex) {
      throw new RuntimeException("Unable to get the named file system.", ex);
    }
    
    //create src chunk file list for splits
    Path srcFileListPath = new Path(jobDirectory, 
        "_distcp_src_files");
    job.set(SRC_LIST_LABEL, srcFileListPath.toString());
    SequenceFile.Writer src_file_writer = SequenceFile.createWriter(jobfs, 
        job, srcFileListPath, LongWritable.class, FileChunkPair.class,
        jobfs.getConf().getInt("io.file.buffer.size", 4096),
        SRC_FILES_LIST_REPL_DEFAULT, jobfs.getDefaultBlockSize(),
        SequenceFile.CompressionType.NONE,
        new DefaultCodec(), null, new Metadata());

    //store the file chunk information based on the target size
      long acc = 0L;
      long pos = 0L;
      long last = 0L;
      long cbsyncs = 0L;
      int cnsyncf = 0;
      int dstsyn = 0;
      //create the split files which can be directly read by getSplit()
      Path splitPath = new Path(jobDirectory, "_distcp_file_splits");
      job.set(SPLIT_LIST_LABEL, splitPath.toString());
      SequenceFile.Writer split_writer = SequenceFile.createWriter(jobfs, 
          job, splitPath, LongWritable.class, LongWritable.class,
          SequenceFile.CompressionType.NONE);

      //for stitching chunk files
      Path dstChunkFileDirListPath = new Path(jobDirectory, 
      "_distcp_dst_chunk_files_dir");
      job.set(DST_CHUNK_FILE_LIST_LABEL, dstChunkFileDirListPath.toString());
      SequenceFile.Writer  dst_chunk_file_writer = 
        SequenceFile.createWriter(jobfs,job, dstChunkFileDirListPath, 
            IntWritable.class, Text.class, SequenceFile.CompressionType.NONE);   
      try{        
        for(int i = 0; i < filePairList.size(); ++i) { 
          FilePairComparable fp = filePairList.get(i);
          
          // check if the source file is under construnction
          if (!fp.input.isDir() && skipUnderConstructionFile) {
            FileSystem srcFileSys = fp.input.getPath().getFileSystem(job);
            LOG.debug("Check file :" + fp.input.getPath());
            LocatedBlocks locatedBlks = 
                DFSUtil.convertToDFS(srcFileSys).getLocatedBlocks(
                    fp.input.getPath(), 0, fp.input.getLen());
            if (locatedBlks.isUnderConstruction()) {
              LOG.debug("Skip under construnction file: " + fp.input.getPath());
              continue;
            }
          }
          
          long blockSize = destFileSys.getDefaultBlockSize();
          if(preserve_block_size) {
            blockSize = fp.input.getBlockSize();
          }
          long restFileLength = fileLengthList.get(i).get();
          long offset = 0;
          long chunkLength = 0;
          long lengthAddedToChunk = 0;
          int chunkIndex = 0;
          if(restFileLength == 0) {
            src_file_writer.append(new LongWritable(0), 
                new FileChunkPair(fp.input, fp.output, 0, 0, 0));
            ++chunkIndex;
          }
          else{
            while (restFileLength > 0) {
              if(restFileLength > blockSize) {
                lengthAddedToChunk = blockSize;
              }
              else {
                lengthAddedToChunk = restFileLength;
              }
              chunkLength += lengthAddedToChunk;
              //reach the end of the target map size, write blocks in one chunk
              //then start a new split
              if(acc + lengthAddedToChunk > targetSize){
                src_file_writer.append(fileLengthList.get(i), 
                    new FileChunkPair(fp.input,fp.output, offset,
                        chunkLength, chunkIndex));
                pos = src_file_writer.getLength();
                long splitsize = pos - last;
                split_writer.append(new LongWritable(last), 
                    new LongWritable(splitsize));
                ++cnsyncf;
                cbsyncs += chunkLength;
                if(cnsyncf > SYNC_FILE_MAX || cbsyncs > BYTES_PER_MAP) {
                  src_file_writer.sync();
                  split_writer.sync();
                  cnsyncf = 0;
                  cbsyncs = 0L;
                }
                last = pos;
                offset += chunkLength;
                ++chunkIndex;
                chunkLength = 0L;
                acc = 0L;
                restFileLength -= lengthAddedToChunk;
              }                
              else{
                acc += lengthAddedToChunk;
                restFileLength -= lengthAddedToChunk;
                //reach the end of the file.
                if(restFileLength == 0) {
                  src_file_writer.append(fileLengthList.get(i), 
                      new FileChunkPair(fp.input, fp.output, offset,
                          chunkLength, chunkIndex));
                  LOG.info("create chunk, offest " + offset + " chunkLength " + chunkLength
                      + " chunkIndex " + chunkIndex);
                  cbsyncs += chunkLength;
                  if(cbsyncs > BYTES_PER_MAP) {
                    src_file_writer.sync();
                    cbsyncs = 0L;
                  }
                  ++chunkIndex;
                }
              }
            }
            if (++dstsyn > SYNC_FILE_MAX) {
              dstsyn = 0;
              dst_chunk_file_writer.sync();                
            }
          }
          if (!fp.input.isDir()) {
            dst_chunk_file_writer.append(new IntWritable(chunkIndex), 
              new Text(fp.output));
          }
        }
        //add the rest as the last split
        long cbrem = src_file_writer.getLength() - last;
        if (cbrem != 0) {
          split_writer.append(new LongWritable(last), new LongWritable(cbrem));
        }
      }
      finally{
        checkAndClose(src_file_writer);
        checkAndClose(dst_chunk_file_writer);
        checkAndClose(split_writer);
      }    
  }

  /**
   * Check whether the contents of src and dst are the same.
   * 
   * Return false if dstpath does not exist
   * 
   * If the files have different sizes, return false.
   * 
   * If the files have the same sizes, the file checksums will be compared.
   * 
   * When file checksum is not supported in any of file systems,
   * two files are considered as the same if they have the same size.
   */
  static private boolean sameFile(FileSystem srcfs, FileStatus srcstatus,
      FileSystem dstfs, Path dstpath, boolean skipCRCCheck) throws IOException {
    FileStatus dststatus;
    try {
      dststatus = dstfs.getFileStatus(dstpath);
    } catch(FileNotFoundException fnfe) {
      return false;
    }

    //same length?
    if (srcstatus.getLen() != dststatus.getLen()) {
      return false;
    }

    if (skipCRCCheck) {
      LOG.debug("Skipping CRC Check");
      return true;
    }

    //get src checksum
    final FileChecksum srccs;
    try {
      srccs = srcfs.getFileChecksum(srcstatus.getPath());
    } catch(FileNotFoundException fnfe) {
      /*
       * Two possible cases:
       * (1) src existed once but was deleted between the time period that
       *     srcstatus was obtained and the try block above.
       * (2) srcfs does not support file checksum and (incorrectly) throws
       *     FNFE, e.g. some previous versions of HftpFileSystem.
       * For case (1), it is okay to return true since src was already deleted.
       * For case (2), true should be returned.  
       */
      return true;
    }

    //compare checksums
    try {
      final FileChecksum dstcs = dstfs.getFileChecksum(dststatus.getPath());
      //return true if checksum is not supported
      //(i.e. some of the checksums is null)
      return srccs == null || dstcs == null || srccs.equals(dstcs);
    } catch(FileNotFoundException fnfe) {
      return false;
    }
  }
  
  /** Delete the dst files/dirs which do not exist in src */
  static private void deleteNonexisting(
      FileSystem dstfs, FileStatus dstroot, Path dstsorted,
      FileSystem jobfs, Path jobdir, JobConf jobconf, Configuration conf
      ) throws IOException {
    if (!dstroot.isDir()) {
      throw new IOException("dst must be a directory when option "
          + Options.DELETE.cmd + " is set, but dst (= " + dstroot.getPath()
          + ") is not a directory.");
    }

    //write dst lsr results
    final Path dstlsr = new Path(jobdir, "_distcp_dst_lsr");
    final SequenceFile.Writer writer = SequenceFile.createWriter(jobfs, jobconf,
        dstlsr, Text.class, FileStatus.class,
        SequenceFile.CompressionType.NONE);
    try {
      //do lsr to get all file statuses in dstroot
      final Stack<FileStatus> lsrstack = new Stack<FileStatus>();
      for(lsrstack.push(dstroot); !lsrstack.isEmpty(); ) {
        final FileStatus status = lsrstack.pop();
        if (status.isDir()) {
          for(FileStatus child : dstfs.listStatus(status.getPath())) {
            String relative = makeRelative(dstroot.getPath(), child.getPath());
            writer.append(new Text(relative), child);
            lsrstack.push(child);
          }
        }
      }
    } finally {
      checkAndClose(writer);
    }

    //sort lsr results
    final Path sortedlsr = new Path(jobdir, "_distcp_dst_lsr_sorted");
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(jobfs,
        new Text.Comparator(), Text.class, FileStatus.class, jobconf);
    sorter.sort(dstlsr, sortedlsr);

    //compare lsr list and dst list  
    SequenceFile.Reader lsrin = null;
    SequenceFile.Reader dstin = null;
    try {
      lsrin = new SequenceFile.Reader(jobfs, sortedlsr, jobconf);
      dstin = new SequenceFile.Reader(jobfs, dstsorted, jobconf);

      //compare sorted lsr list and sorted dst list
      final Text lsrpath = new Text();
      final FileStatus lsrstatus = new FileStatus();
      final Text dstpath = new Text();
      final Text dstfrom = new Text();
      final FsShell shell = new FsShell(conf);
      final String[] shellargs = {"-rmr", null};

      boolean hasnext = dstin.next(dstpath, dstfrom);
      for(; lsrin.next(lsrpath, lsrstatus); ) {
        int dst_cmp_lsr = dstpath.compareTo(lsrpath);
        for(; hasnext && dst_cmp_lsr < 0; ) {
          hasnext = dstin.next(dstpath, dstfrom);
          dst_cmp_lsr = dstpath.compareTo(lsrpath);
        }
        
        if (dst_cmp_lsr == 0) {
          //lsrpath exists in dst, skip it
          hasnext = dstin.next(dstpath, dstfrom);
        }
        else {
          //lsrpath does not exist, delete it
          String s = new Path(dstroot.getPath(), lsrpath.toString()).toString();
          if (shellargs[1] == null || !isAncestorPath(shellargs[1], s)) {
            shellargs[1] = s;
            int r = 0;
            try {
               r = shell.run(shellargs);
            } catch(Exception e) {
              throw new IOException("Exception from shell.", e);
            }
            if (r != 0) {
              throw new IOException("\"" + shellargs[0] + " " + shellargs[1]
                  + "\" returns non-zero value " + r);
            }
          }
        }
      }
    } finally {
      checkAndClose(lsrin);
      checkAndClose(dstin);
    }
  }

  //is x an ancestor path of y?
  static private boolean isAncestorPath(String x, String y) {
    if (!y.startsWith(x)) {
      return false;
    }
    final int len = x.length();
    return y.length() == len || y.charAt(len) == Path.SEPARATOR_CHAR;  
  }
  
  /** Check whether the file list have duplication. */
  static private void checkDuplication(FileSystem fs, Path file, Path sorted,
    Configuration conf) throws IOException {
    SequenceFile.Reader in = null;
    try {
      SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
        new Text.Comparator(), Text.class, Text.class, conf);
      sorter.sort(file, sorted);
      in = new SequenceFile.Reader(fs, sorted, conf);

      Text prevdst = null, curdst = new Text();
      Text prevsrc = null, cursrc = new Text(); 
      for(; in.next(curdst, cursrc); ) {
        if (prevdst != null && curdst.equals(prevdst)) {
          throw new DuplicationException(
            "Invalid input, there are duplicated files in the sources: "
            + prevsrc + ", " + cursrc);
        }
        prevdst = curdst;
        curdst = new Text();
        prevsrc = cursrc;
        cursrc = new Text();
      }
    }
    finally {
      checkAndClose(in);
    }
  } 

  static void checkAndClose(java.io.Closeable io) throws IOException {
    if (io != null) {
      io.close();
    }
  }
  
  /** An exception class for duplicated source files. */
  public static class DuplicationException extends IOException {
    private static final long serialVersionUID = 1L;
    /** Error code for this exception */
    public static final int ERROR_CODE = -2;
    DuplicationException(String message) {super(message);}
  }
}
