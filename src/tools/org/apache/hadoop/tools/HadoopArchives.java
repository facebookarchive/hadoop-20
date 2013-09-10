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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.HarFileSystem.HarFSDataInputStream;
import org.apache.hadoop.fs.HarProperties;
import org.apache.hadoop.fs.HarReader;
import org.apache.hadoop.fs.HarStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * a archive creation utility.
 * This class provides methods that can be used 
 * to create hadoop archives. For understanding of 
 * Hadoop archives look at {@link HarFileSystem}.
 */
public class HadoopArchives implements Tool {
  public static final int VERSION = 2;
  private static final Log LOG = LogFactory.getLog(HadoopArchives.class);
  
  private static final String NAME = "har"; 
  /** name of the file in hdfs to read the files needed to archive **/
  private static final String SRC_LIST_LABEL = NAME + ".src.list";
  
  /** total size of the files to archive **/
  private static final String TOTAL_SIZE_LABEL = NAME + ".total.size";
  
  /** path to be used as a parent to archive files **/ 
  private static final String SRC_PARENT_LABEL = NAME + ".parent.path";

  /** the size of the blocks that will be created when archiving **/
  private static final String HAR_BLOCKSIZE_LABEL = NAME + ".block.size";
  private static final long HAR_BLOCKSIZE_DEFAULT = 512 * 1024 * 1024l; 

  /**the size of the part files that will be created when archiving **/
  private static final String HAR_PARTSIZE_LABEL = NAME + ".partfile.size";
  private static final long HAR_PARTSIZE_DEFAULT = 4 * 1024 * 1024 * 1024l; 

  private static final String PART_ID_OFFSET = NAME + ".partid.offset";
  
  /** number of lines in each block of _index file
   * see {@link HarFileSystem} for more details
   */
  private static final long NUM_LINES_IN_BLOCK_INDEX = 100l;
  
  private static final String PART_PREFIX = "part-";
  
  private static final String USAGE = 
    "USAGE: java HadoopArchives [options]: \n" +
    "   archive -archiveName   NAME -p <parent path> <src>* <dst>\n" +
    "   archive -append        <archiveName> -p <parent path> <src>* <dstArchive>\n" +
    "   archive -appendFromArchive <srcArchive> <archivePaths>* <dstArchive>\n" +
    "   archive -copyFromLocal <srcDir> <dstArchive>\n" +
    "   archive -copyToLocal   <srcArchive> <dstDir>\n";

  private Path jobDirectory;
  private Path srcFiles;
  
  private JobConf conf;

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf, HadoopArchives.class);
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  /** map of possible run modes **/
  private Map<String, Executor> executors;

  public HadoopArchives(Configuration conf) {
    setConf(conf);
    
    executors = new HashMap<String, HadoopArchives.Executor>();
    executors.put("-archiveName", new ArchiveExecutor(conf));
    executors.put("-append", new AppendExecutor(conf));
    executors.put("-appendFromArchive", new AppendFromArchiveExecutor(conf));
    executors.put("-copyFromLocal", new CopyFromLocalExecutor(conf));
    executors.put("-copyToLocal", new CopyToLocalExecutor(conf));
  }

  // check the src paths
  private static void checkPaths(Configuration conf, List<Path> paths) throws
  IOException {
    for (Path p : paths) {
      FileSystem fs = p.getFileSystem(conf);
      if (!fs.exists(p)) {
        throw new FileNotFoundException("Source " + p + " does not exist.");
      }
    }
  }

  /**
   * this assumes that there are two types of files file/dir
   * @param fs the input filesystem
   * @param p the top level path 
   * @param out the list of paths output of recursive ls
   * @throws IOException
   */
  private void recursivels(FileSystem fs, Path p, List<FileStatus> out) 
  throws IOException {
    FileStatus fstatus = fs.getFileStatus(p);
    if (!fstatus.isDir()) {
      out.add(fstatus);
      return;
    }
    else {
      out.add(fstatus);
      FileStatus[] listStatus = fs.listStatus(p);
      for (FileStatus stat: listStatus) {
        recursivels(fs, stat.getPath(), out);
      }
    }
  }

  private static class HarEntry implements Writable {
    String path;
    String[] children;
    HarProperties properties;

    HarEntry() {}

    public HarEntry(String path, String[] children, HarProperties properties) {
      this.path = path;
      this.children = children;
      this.properties = properties;
    }

    private boolean isDir() {
      return children != null;      
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, path);

      final boolean dir = isDir();
      out.writeBoolean(dir);
      if (dir) {
        out.writeInt(children.length);
        for(String c : children) {
          Text.writeString(out, c);
        }
      }
      properties.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      path = Text.readString(in);

      if (in.readBoolean()) {
        children = new String[in.readInt()];
        for(int i = 0; i < children.length; i++) {
          children[i] = Text.readString(in);
        }
      } else {
        children = null;
      }
      properties = new HarProperties();
      properties.readFields(in);
    }

    public HarProperties getProperties() {
      return properties;
    }

  }

  private boolean checkValidName(String name) {
    Path tmp = new Path(name);
    if (tmp.depth() != 1) {
      return false;
    }
    if (name.endsWith(".har")) 
      return true;
    return false;
  }
  

  private Path largestDepth(List<Path> paths) {
    Path deepest = paths.get(0);
    for (Path p: paths) {
      if (p.depth() > deepest.depth()) {
        deepest = p;
      }
    }
    return deepest;
  }

  /**
   * truncate the prefix root from the full path
   * @param fullPath the full path
   * @param root the prefix root to be truncated
   * @return the relative path
   */
  private Path relPathToRoot(Path fullPath, Path root) {
    // just take some effort to do it 
    // rather than just using substring 
    // so that we do not break sometime later
    Path justRoot = new Path(Path.SEPARATOR);
    if (fullPath.depth() == root.depth()) {
      return justRoot;
    }
    else if (fullPath.depth() > root.depth()) {
      Path retPath = new Path(fullPath.getName());
      Path parent = fullPath.getParent();
      for (int i=0; i < (fullPath.depth() - root.depth() -1); i++) {
        retPath = new Path(parent.getName(), retPath);
        parent = parent.getParent();
      }
      return new Path(justRoot, retPath);
    }
    return null;
  }

  /**
   * this method writes all the valid top level directories into the srcWriter
   * for indexing. This method is a little tricky. example- for an input with
   * parent path /home/user/ and sources as /home/user/source/dir1,
   * /home/user/source/dir2 - this will output {@code <source, dir, dir1, dir2>}
   * (dir means that source is a dir with dir1 and dir2 as children),
   * {@code <source/dir1, file, null>}, {@code <source/dir2, file, null>} and
   * {@code </, dir, source>}
   * 
   * @param srcWriter
   *          the sequence file writer to write the directories to
   * @param paths
   *          the source paths provided by the user. They are glob free and have
   *          full path (not relative paths)
   * @param parentPath
   *          the parent path that you want the archives to be relative to.
   *          example - /home/user/dir1 can be archived with parent as /home or
   *          /home/user.
   * @throws IOException
   */
  private void writeTopLevelDirs(SequenceFile.Writer srcWriter, 
      List<Path> paths, Path parentPath) throws IOException {
    /* find all the common parents of paths that are valid archive
     * paths. The below is done so that we do not add a common path
     * twice and also we need to only add valid child of a path that
     * are specified the user.
     */
    TreeMap<Path, HashSet<String>> allpaths = new TreeMap<Path, 
                                                HashSet<String>>();
    /* the largest depth of paths. the max number of times
     * we need to iterate
     */
    Path deepest = largestDepth(paths);
    Path root = new Path(Path.SEPARATOR);

    List<Path> justDirs = paths;
    for (int i = parentPath.depth(); i < deepest.depth(); i++) {
      List<Path> parents = new ArrayList<Path>();
      for (Path p: justDirs) {
        if (p.compareTo(root) == 0){
          //do nothing
        }
        else {
          Path parent = p.getParent();
          if (allpaths.containsKey(parent)) {
            HashSet<String> children = allpaths.get(parent);
            children.add(p.getName());
          }
          else {
            HashSet<String> children = new HashSet<String>();
            children.add(p.getName());
            allpaths.put(parent, children);
          }
          parents.add(parent);
        }
      }
      justDirs = parents;
    }
    Set<Map.Entry<Path, HashSet<String>>> keyVals = allpaths.entrySet();
    for (Map.Entry<Path, HashSet<String>> entry : keyVals) {
      Path currentPath = entry.getKey();
      Path relPath = relPathToRoot(currentPath, parentPath);
      if (relPath != null) {
        FileSystem fs = currentPath.getFileSystem(getConf());
        HarProperties properties = new HarProperties(fs.getFileStatus(currentPath));
        final String[] children = new String[entry.getValue().size()];
        int i = 0;
        for(String child: entry.getValue()) {
          children[i++] = child;
        }
        HarEntry harEntry = new HarEntry(relPath.toString(), children, properties); 
        srcWriter.append(new LongWritable(0L), harEntry);
      }
    }
  }

  //delete the tmp job directory
  private void cleanJobDirectory() {
    try {
      FileSystem jobfs = jobDirectory.getFileSystem(conf);
      jobfs.delete(jobDirectory, true);
    } catch(IOException ioe) {
      LOG.warn("Unable to clean tmp directory " + jobDirectory, ioe);
    }
  }
  
  private long writeFromArchiveFilesToProcess(Path harSrc, List<Path> relativePaths) throws IOException {
    Set<Path> allowedPaths = new HashSet<Path>(relativePaths);
    
    Map<String, HashSet<String>> tree = new HashMap<String, HashSet<String>>();
    HashSet<String> toTake = new HashSet<String>();
    
    HarReader harReader = new HarReader(harSrc, conf);
    List<HarStatus> allStatuses = new ArrayList<HarStatus>();
    try {
      while (harReader.hasNext()) {
        allStatuses.add(harReader.getNext());
      }
    } finally {
      if (harReader != null) {
        harReader.close();
      }
    }

    Path root = new Path(Path.SEPARATOR);
    // decide which of the har files we need to process
    // and create in-memory tree structure of the files
    for (HarStatus harStatus : allStatuses) {
       Path path = new Path(harStatus.getName());
       Path currentPath = path;
       // decide whether we need to process this har-entry
       boolean allowed = false;
       for (int i = 0; i <= path.depth(); ++i) {
         if (allowedPaths.contains(currentPath)) {
           allowed = true;
           break;
         }
         currentPath = currentPath.getParent();
       }
       if (allowed) {
         currentPath = path;
         // update in-memory structure of har files
         for (int i = 0; i <= path.depth(); ++i) {
           toTake.add(currentPath.toString());
           if (currentPath.equals(root)) {
             break;
           }
           Path parent = currentPath.getParent();
           String parentString = parent.toString();
           HashSet<String> treeEntry = tree.get(parentString);
           if (treeEntry == null) {
             HashSet<String> value = new HashSet<String>();
             value.add(currentPath.getName());
             tree.put(parentString, value);
           } else {
             treeEntry.add(currentPath.getName());
           }
           currentPath = parent;
         }
       }
    }
    
    final String randomId = DistCp.getRandomId();
    jobDirectory = new Path(new JobClient(conf).getSystemDir(), NAME + "_" + randomId);
    
    //get a tmp directory for input splits
    FileSystem jobfs = jobDirectory.getFileSystem(conf);
    jobfs.mkdirs(jobDirectory);
    srcFiles = new Path(jobDirectory, "_har_src_files");
    SequenceFile.Writer srcWriter = SequenceFile.createWriter(jobfs, conf,
        srcFiles, LongWritable.class, HarEntry.class, 
        SequenceFile.CompressionType.NONE);

    long totalSize = 0;
    try {
      for (HarStatus harStatus : allStatuses) {
        String pathString = harStatus.getName();
        // skip items that we don't need
        if (!toTake.contains(pathString)) {
          continue;
        }

        HashSet<String> treeEntry = tree.get(pathString);
        String[] children;
        if (treeEntry == null) {
          children = null;
        } else {
          children = treeEntry.toArray(new String[0]);
        }
        HarEntry harEntry = new HarEntry(harStatus.getName(), children, harStatus.getProperties());
        srcWriter.append(new LongWritable(harStatus.getLength()), harEntry);
        srcWriter.sync();
        totalSize += harStatus.getLength();
      }
    } finally {
      srcWriter.close();
    }
    return totalSize;
  }
  
  private void appendFromArchive(Path harSrc, List<Path> relativePaths, Path harDst) throws IOException {
    Path outputPath = harDst;
    FileOutputFormat.setOutputPath(conf, outputPath);
    FileSystem outFs = outputPath.getFileSystem(conf);
    
    if (!outFs.exists(outputPath)) {
      throw new IOException("Invalid Output. HAR File " + outputPath + "doesn't exist");
    }
    if (outFs.isFile(outputPath)) {
      throw new IOException("Invalid Output. HAR File " + outputPath
          + "must be represented as directory");
    }
    long totalSize = writeFromArchiveFilesToProcess(harSrc, relativePaths);

    //make it a har path
    FileSystem fs1 = harSrc.getFileSystem(conf);
    URI uri = fs1.getUri();
    Path parentPath = new Path("har://" + "hdfs-" + uri.getHost() +":" +
        uri.getPort() + fs1.makeQualified(harSrc).toUri().getPath());
    FileSystem fs = parentPath.getFileSystem(conf);
    
    conf.set(SRC_LIST_LABEL, srcFiles.toString());
    conf.set(SRC_PARENT_LABEL, parentPath.makeQualified(fs).toString());
    conf.setLong(TOTAL_SIZE_LABEL, totalSize);
    long partSize = conf.getLong(HAR_PARTSIZE_LABEL, HAR_PARTSIZE_DEFAULT);
    int numMaps = (int) (totalSize / partSize);
    //run atleast one map.
    conf.setNumMapTasks(numMaps == 0 ? 1 : numMaps);
    conf.setNumReduceTasks(1);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);
    conf.set("hadoop.job.history.user.location", "none");
    //make sure no speculative execution is done
    conf.setSpeculativeExecution(false);

    // set starting offset for mapper
    int partId = findFirstAvailablePartId(outputPath);
    conf.setInt(PART_ID_OFFSET, partId);
    
    Path index = new Path(outputPath, HarFileSystem.INDEX_NAME);
    Path indexDirectory = new Path(outputPath, HarFileSystem.INDEX_NAME + ".copy");
    outFs.mkdirs(indexDirectory);
    Path indexCopy = new Path(indexDirectory, "data");
    outFs.rename(index, indexCopy);

    MultipleInputs.addInputPath(conf, jobDirectory, HArchiveInputFormat.class,
        HArchivesMapper.class);
    MultipleInputs.addInputPath(conf, indexDirectory, TextInputFormat.class,
        HArchivesConvertingMapper.class);
    conf.setReducerClass(HArchivesMergingReducer.class);

    JobClient.runJob(conf);

    cleanJobDirectory();
  }
  
  /**
   * archive the given source paths into the dest
   * 
   * @param parentPath
   *          the parent path of all the source paths
   * @param srcPaths
   *          the src paths to be archived
   * @param dest
   *          the dest dir that will contain the archive
   * @param append
   *          append to existing archive or create new
   * 
   */
  private void archive(Path parentPath, List<Path> srcPaths, Path outputPath, boolean append)
      throws IOException {
    parentPath = parentPath.makeQualified(parentPath.getFileSystem(conf));
    checkPaths(conf, srcPaths);

    Path destinationDir = outputPath.getParent();
    FileOutputFormat.setOutputPath(conf, outputPath);
    FileSystem outFs = outputPath.getFileSystem(conf);
    
    if (append) {
      if (!outFs.exists(outputPath)) {
        throw new IOException("Invalid Output. HAR File " + outputPath + "doesn't exist");
      }
      if (outFs.isFile(outputPath)) {
        throw new IOException("Invalid Output. HAR File " + outputPath
            + "must be represented as directory");
      }
    } else {
      if (outFs.exists(outputPath)) {
        throw new IOException("Invalid Output: " + outputPath + ". File already exists");
      }
      if (outFs.isFile(destinationDir)) {
        throw new IOException("Invalid Output. " + outputPath + " is not a directory");
      }
    }
    long totalSize = writeFilesToProcess(parentPath, srcPaths);

    FileSystem fs = parentPath.getFileSystem(conf);
    
    conf.set(SRC_LIST_LABEL, srcFiles.toString());
    conf.set(SRC_PARENT_LABEL, parentPath.makeQualified(fs).toString());
    conf.setLong(TOTAL_SIZE_LABEL, totalSize);
    long partSize = conf.getLong(HAR_PARTSIZE_LABEL, HAR_PARTSIZE_DEFAULT);
    int numMaps = (int) (totalSize / partSize);
    //run atleast one map.
    conf.setNumMapTasks(numMaps == 0 ? 1 : numMaps);
    conf.setNumReduceTasks(1);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);
    conf.set("hadoop.job.history.user.location", "none");
    //make sure no speculative execution is done
    conf.setSpeculativeExecution(false);

    if (append) {
      // set starting offset for mapper
      int partId = findFirstAvailablePartId(outputPath);
      conf.setInt(PART_ID_OFFSET, partId);
      
      Path index = new Path(outputPath, HarFileSystem.INDEX_NAME);
      Path indexDirectory = new Path(outputPath, HarFileSystem.INDEX_NAME + ".copy");
      outFs.mkdirs(indexDirectory);
      Path indexCopy = new Path(indexDirectory, "data");
      outFs.rename(index, indexCopy);

      MultipleInputs.addInputPath(conf, jobDirectory, HArchiveInputFormat.class,
          HArchivesMapper.class);
      MultipleInputs.addInputPath(conf, indexDirectory, TextInputFormat.class,
          HArchivesConvertingMapper.class);
      conf.setReducerClass(HArchivesMergingReducer.class);
    } else {
      conf.setMapperClass(HArchivesMapper.class);
      conf.setInputFormat(HArchiveInputFormat.class);
      FileInputFormat.addInputPath(conf, jobDirectory);
      conf.setReducerClass(HArchivesReducer.class);
    }

    JobClient.runJob(conf);

    cleanJobDirectory();
  }

  private long writeFilesToProcess(Path parentPath, List<Path> srcPaths) throws IOException {
    final String randomId = DistCp.getRandomId();
    jobDirectory = new Path(new JobClient(conf).getSystemDir(), NAME + "_" + randomId);
    
    //get a tmp directory for input splits
    FileSystem jobfs = jobDirectory.getFileSystem(conf);
    jobfs.mkdirs(jobDirectory);
    srcFiles = new Path(jobDirectory, "_har_src_files");
    SequenceFile.Writer srcWriter = SequenceFile.createWriter(jobfs, conf,
        srcFiles, LongWritable.class, HarEntry.class, 
        SequenceFile.CompressionType.NONE);
    long totalSize = 0;
    // get the list of files 
    // create single list of files and dirs
    FileSystem fs = parentPath.getFileSystem(conf);
    try {
      // write the top level dirs in first 
      writeTopLevelDirs(srcWriter, srcPaths, parentPath);
      srcWriter.sync();

      // these are the input paths passed 
      // from the command line
      // we do a recursive ls on these paths 
      // and then write them to the input file 
      // one at a time
      for (Path src: srcPaths) {
        ArrayList<FileStatus> allFiles = new ArrayList<FileStatus>();
        recursivels(fs, src, allFiles);
        for (FileStatus stat: allFiles) {
          long len = stat.isDir() ? 0 : stat.getLen();
          String path = relPathToRoot(stat.getPath(), parentPath).toString();
          String[] children = null;
          if (stat.isDir()) {
            //get the children 
            FileStatus[] list = fs.listStatus(stat.getPath());
            children = new String[list.length];
            for (int i = 0; i < list.length; i++) {
              children[i] = list[i].getPath().getName();
            }
          }
          HarEntry harEntry = new HarEntry(path, children, new HarProperties(stat));
          srcWriter.append(new LongWritable(len), harEntry);
          srcWriter.sync();
          totalSize += len;
        }
      }
    } finally {
      srcWriter.close();
    }
    jobfs.setReplication(srcFiles, (short) 10);
    return totalSize;
  }

  private int findFirstAvailablePartId(Path archivePath) throws IOException {
    FileSystem fs = archivePath.getFileSystem(conf);
    FileStatus[] fileStatuses = fs.listStatus(archivePath);
    int result = 0;
    for (FileStatus fileStatus : fileStatuses) {
      String name = fileStatus.getPath().getName();
      if (name.startsWith(PART_PREFIX)) {
        int id = Integer.parseInt(name.substring(PART_PREFIX.length()));
        result = Math.max(result, id + 1);
      }
    }
    return result;
  }

  /**
   * Input format of a hadoop archive job responsible for 
   * generating splits of the file list
   */
  private static class HArchiveInputFormat implements InputFormat<LongWritable, HarEntry> {
    //generate input splits from the src file lists
    public InputSplit[] getSplits(JobConf jconf, int numSplits)
    throws IOException {
      String srcfilelist = jconf.get(SRC_LIST_LABEL, "");
      if ("".equals(srcfilelist)) {
          throw new IOException("Unable to get the " +
              "src file for archive generation.");
      }
      long totalSize = jconf.getLong(TOTAL_SIZE_LABEL, -1);
      if (totalSize == -1) {
        throw new IOException("Invalid size of files to archive");
      }
      //we should be safe since this is set by our own code
      Path src = new Path(srcfilelist);
      FileSystem fs = src.getFileSystem(jconf);
      FileStatus fstatus = fs.getFileStatus(src);
      ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
      LongWritable key = new LongWritable();
      HarEntry value = new HarEntry();
      SequenceFile.Reader reader = null;
      // the remaining bytes in the file split
      long remaining = fstatus.getLen();
      // the count of sizes calculated till now
      long currentCount = 0L;
      // the endposition of the split
      long lastPos = 0L;
      // the start position of the split
      long startPos = 0L;
      long targetSize = totalSize / numSplits;
      // create splits of size target size so that all the maps 
      // have equals sized data to read and write to.
      try {
        reader = new SequenceFile.Reader(fs, src, jconf);
        while(reader.next(key, value)) {
          if (currentCount + key.get() > targetSize && currentCount != 0){
            long size = lastPos - startPos;
            splits.add(new FileSplit(src, startPos, size, (String[]) null));
            remaining = remaining - size;
            startPos = lastPos;
            currentCount = 0L;
          }
          currentCount += key.get();
          lastPos = reader.getPosition();
        }
        // the remaining not equal to the target size.
        if (remaining != 0) {
          splits.add(new FileSplit(src, startPos, remaining, (String[])null));
        }
      }
      finally { 
        reader.close();
      }
      return splits.toArray(new FileSplit[splits.size()]);
    }

    public RecordReader<LongWritable, HarEntry> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<LongWritable, HarEntry>(job,
                 (FileSplit)split);
    }
  }

  /**
   * A class that reads lines from _index file and writes
   * them with hash of the path field
   */
  private static class HArchivesConvertingMapper 
  implements Mapper<LongWritable, Text, IntWritable, Text> {

    public void configure(JobConf conf) {
    }

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> out,
        Reporter reporter) throws IOException {
      reporter.setStatus("Passing file " + value + " to archive.");
      reporter.progress();

      HarStatus harStatus = new HarStatus(value.toString());
      int hash = HarFileSystem.getHarHash(harStatus.getName());
      out.collect(new IntWritable(hash), value);
    }

    public void close() throws IOException {
    }
  }
  
  private static class HArchivesMapper 
  implements Mapper<LongWritable, HarEntry, IntWritable, Text> {
    private JobConf conf = null;
    private int partId = -1 ; 
    private Path tmpOutputDir = null;
    Path tmpOutput = null;
    String partName = null;
    Path rootPath = null;
    FSDataOutputStream partStream = null;
    FileSystem destFs = null;
    byte[] buffer;
    final static int BUFFER_SIZE = 128 * 1024;
    long blockSize;

    // configure the mapper and create 
    // the part file.
    // use map reduce framework to write into
    // tmp files. 
    public void configure(JobConf conf) {
      this.conf = conf;
      int partIdOffset = conf.getInt(PART_ID_OFFSET, 0);
      // this is tightly tied to map reduce
      // since it does not expose an api 
      // to get the partition
      partId = conf.getInt("mapred.task.partition", -1) + partIdOffset;
      // create a file name using the partition
      // we need to write to this directory
      tmpOutputDir = FileOutputFormat.getWorkOutputPath(conf);
      blockSize = conf.getLong(HAR_BLOCKSIZE_LABEL, HAR_BLOCKSIZE_DEFAULT);
      // get the output path and write to the tmp 
      // directory 
      partName = PART_PREFIX + partId;
      tmpOutput = new Path(tmpOutputDir, partName);
      String rootPathString = conf.get(SRC_PARENT_LABEL, null); 
      if (rootPathString == null) { 
        throw new RuntimeException("Unable to read parent " +
        		"path for har from config");
      }
      rootPath = new Path(rootPathString);
      try {
        destFs = tmpOutput.getFileSystem(conf);
        //this was a stale copy
        if (destFs.exists(tmpOutput)) {
          destFs.delete(tmpOutput, false);
        } 
        partStream = destFs.create(tmpOutput, false, conf.getInt("io.file.buffer.size", 4096), 
            destFs.getDefaultReplication(), blockSize);
      } catch(IOException ie) {
        throw new RuntimeException("Unable to open output file " + tmpOutput, ie);
      }
      buffer = new byte[BUFFER_SIZE];
    }

    // copy raw data.
    public void copyData(Path input, FSDataInputStream fsin, 
        FSDataOutputStream fout, Reporter reporter) throws IOException {
      try {
        for (int cbread=0; (cbread = fsin.read(buffer))>= 0;) {
          fout.write(buffer, 0,cbread);
          reporter.progress();
        }
      } finally {
        fsin.close();
      }
    }

    /**
     * get rid of / in the beginning of path
     * @param p the path
     * @return return path without /
     */
    private Path realPath(Path p, Path parent) {
      Path rootPath = new Path(Path.SEPARATOR);
      if (rootPath.compareTo(p) == 0) {
        return parent;
      }
      return new Path(parent, new Path(p.toString().substring(1)));
    }

    // read files from the split input 
    // and write it onto the part files.
    // also output hash(name) and string 
    // for reducer to create index 
    // and masterindex files.
    public void map(LongWritable key, HarEntry value,
        OutputCollector<IntWritable, Text> out,
        Reporter reporter) throws IOException {
      Path relativePath = new Path(value.path);
      int hash = HarFileSystem.getHarHash(relativePath.toString());
      String towrite = null;
      Path srcPath = realPath(relativePath, rootPath);
      long startPos = partStream.getPos();
      FileSystem srcFs = srcPath.getFileSystem(conf);
      HarProperties properties = value.getProperties();
      String propStr = properties.serialize();
      if (value.isDir()) { 
        towrite = HarFileSystem.encode(relativePath.toString()) 
                  + " dir " + propStr + " 0 0 ";
        StringBuffer sbuff = new StringBuffer();
        sbuff.append(towrite);
        for (String child: value.children) {
          sbuff.append(HarFileSystem.encode(child) + " ");
        }
        towrite = sbuff.toString();
        //reading directories is also progress
        reporter.progress();
      }
      else {
        FSDataInputStream input = srcFs.open(srcPath);
        reporter.setStatus("Copying file " + srcPath + 
            " to archive.");
        copyData(srcPath, input, partStream, reporter);
        long len = partStream.getPos() - startPos;
        towrite = HarFileSystem.encode(relativePath.toString())
                  + " file " + partName + " " + startPos
                  + " " + len + " " + propStr + " ";
      }
      out.collect(new IntWritable(hash), new Text(towrite));
    }
    
    public void close() throws IOException {
      // close the part files.
      partStream.close();
    }
  }
  
  /**
   * Base reducer for creating the index and the master index
   */
  private static class HArchivesReducer implements Reducer<IntWritable, 
  Text, Text, Text> {
    private JobConf conf = null;
    private long startIndex = 0;
    private long endIndex = 0;
    private long startPos = 0;
    private Path masterIndex = null;
    private Path index = null;
    private FileSystem fs = null;
    private FSDataOutputStream outStream = null;
    private FSDataOutputStream indexStream = null;
    private Path tmpOutputDir = null;
    private int written = 0;
    private int keyVal = 0;
    
    /**
     * Configure the reducer: open the _index and _masterindex files for writing
     */
    public void configure(JobConf conf) {
      this.conf = conf;
      tmpOutputDir = FileOutputFormat.getWorkOutputPath(this.conf);
      masterIndex = new Path(tmpOutputDir, HarFileSystem.MASTER_INDEX_NAME);
      index = new Path(tmpOutputDir, HarFileSystem.INDEX_NAME);
      try {
        fs = masterIndex.getFileSystem(conf);
        if (fs.exists(masterIndex)) {
          fs.delete(masterIndex, false);
        }
        if (fs.exists(index)) {
          fs.delete(index, false);
        }
        indexStream = fs.create(index);
        outStream = fs.create(masterIndex);
        String version = VERSION + " \n";
        outStream.write(version.getBytes());
        
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    // create the index and master index. The input to 
    // the reduce is already sorted by the hash of the 
    // files. SO we just need to write it to the index. 
    // We update the masterindex as soon as we update 
    // numIndex entries.
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<Text, Text> out,
        Reporter reporter) throws IOException {
      keyVal = key.get();
      while(values.hasNext()) {
        Text value = values.next();
        String towrite = value.toString() + "\n";
        indexStream.write(towrite.getBytes());
        written++;
        if (written > HadoopArchives.NUM_LINES_IN_BLOCK_INDEX - 1) {
          // every 1000 indexes we report status
          reporter.setStatus("Creating index for archives");
          reporter.progress();
          endIndex = keyVal;
          writeLineToMasterIndex(outStream, startIndex, endIndex, startPos, indexStream.getPos());
          startPos = indexStream.getPos();
          startIndex = endIndex;
          written = 0;
        }
      }
    }
    
    public void close() throws IOException {
      //write the last part of the master index.
      if (written > 0) {
        writeLineToMasterIndex(outStream, startIndex, keyVal, startPos, indexStream.getPos());
      }
      // close the streams
      outStream.close();
      indexStream.close();
      // try increasing the replication 
      fs.setReplication(index, (short) 5);
      fs.setReplication(masterIndex, (short) 5);
    }
  }

  /**  
   * Reducer that merges entries for _index file
   */
  private static class HArchivesMergingReducer implements Reducer<IntWritable, 
  Text, Text, Text> {
    private JobConf conf = null;
    private long startIndex = 0;
    private long endIndex = 0;
    private long startPos = 0;
    private Path masterIndex = null;
    private Path index = null;
    private FileSystem fs = null;
    private FSDataOutputStream outStream = null;
    private FSDataOutputStream indexStream = null;
    private Path outputDir = null;
    private int written = 0;
    private int keyVal = 0;

    /**
     * Configure the reducer: open the _index and _masterindex files for writing
     */
    public void configure(JobConf conf) {
      this.conf = conf;
      outputDir = FileOutputFormat.getWorkOutputPath(this.conf);
      masterIndex = new Path(outputDir, HarFileSystem.MASTER_INDEX_NAME);
      index = new Path(outputDir, HarFileSystem.INDEX_NAME);
      try {
        fs = masterIndex.getFileSystem(conf);
        if (fs.exists(masterIndex)) {
          fs.delete(masterIndex, false);
        }
        if (fs.exists(index)) {
          fs.delete(index, false);
        }
        indexStream = fs.create(index);
        outStream = fs.create(masterIndex);
        String version = VERSION + " \n";
        outStream.write(version.getBytes());
        
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    /**
     * Write the data to index and master index. The input to the reduce is
     * already sorted by the hash of the files.
     */
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<Text, Text> out,
        Reporter reporter) throws IOException {
      
      // merge the children of the same directories
      Map<String, HarStatus> harItems = new HashMap<String, HarStatus>();
      while(values.hasNext()) {
        Text value = values.next();
        HarStatus harStatus = new HarStatus(value.toString());
        if (harItems.containsKey(harStatus.getName())) {
          if (!harStatus.isDir()) {
            throw new RuntimeException("File " + harStatus.getName() + " already exists in har");
          }
          HarStatus existingHarStatus = harItems.get(harStatus.getName());
          existingHarStatus.getChildren().addAll(harStatus.getChildren());
        } else {
          harItems.put(harStatus.getName(), harStatus);
        }
      }

      // write to _index file and update _masterindex
      keyVal = key.get();
      for (HarStatus harStatus: harItems.values()) {
        String towrite = harStatus.serialize() + "\n";
        indexStream.write(towrite.getBytes());
        written++;
        if (written > HadoopArchives.NUM_LINES_IN_BLOCK_INDEX - 1) {
          // every 1000 indexes we report status
          reporter.setStatus("Creating index for archives");
          reporter.progress();
          endIndex = keyVal;
          writeLineToMasterIndex(outStream, startIndex, endIndex, startPos, indexStream.getPos());
          startPos = indexStream.getPos();
          startIndex = endIndex;
          written = 0;
        }
      }
    }
    
    public void close() throws IOException {
      //write the last part of the master index.
      if (written > 0) {
        writeLineToMasterIndex(outStream, startIndex, keyVal, startPos, indexStream.getPos());
      }
      // close the streams
      outStream.close();
      indexStream.close();
      // try increasing the replication 
      fs.setReplication(index, (short) 5);
      fs.setReplication(masterIndex, (short) 5);
    }
  }

  /**
   * Writes data corresponding to part of _index to master index
   * @param stream stream where to write data
   * @param startHash hash of first entry in block
   * @param endHash hash of last entry in block
   * @param indexStartPos position (in bytes) of the beginning of the block
   * @param indexEndPos position (in bytes) of the end of the block
   * @throws IOException
   */
  private static void writeLineToMasterIndex(FSDataOutputStream stream, long startHash,
      long endHash, long indexStartPos, long indexEndPos) throws IOException {
    String toWrite = startHash + " " + endHash + " " + indexStartPos + " " + indexEndPos + "\n";
    stream.write(toWrite.getBytes());
  }
  
  /**
   * Creates new stream to write actual file data
   * @param dst parent of the part-id file  
   * @param partId id of the part
   * @return the open stream
   * @throws IOException
   */
  private FSDataOutputStream createNewPartStream(Path dst, int partId) throws IOException {
    String partName = PART_PREFIX + partId;
    Path output = new Path(dst, partName);
    FileSystem destFs = output.getFileSystem(conf);
    FSDataOutputStream partStream = destFs.create(output, false,
        conf.getInt("io.file.buffer.size", 4096), destFs.getDefaultReplication(),
        conf.getLong(HAR_BLOCKSIZE_LABEL, HAR_BLOCKSIZE_DEFAULT));
    return partStream;
  }
  
  private static final class LocalAndArchivePaths {
    private final Path localPath;
    private final String archivePath;

    public Path getLocalPath() {
      return localPath;
    }

    public String getArchivePath() {
      return archivePath;
    }

    public LocalAndArchivePaths(Path localPath, String archivePath) {
      super();
      this.localPath = localPath;
      this.archivePath = archivePath;
    }
  }

  /**
   * Uploads local directory as har file
   * @param srcDir path to local directory to upload
   * @param dst path to har archive
   * @throws IOException
   */
  private void copyFromLocal(Path srcDir, Path dst) throws IOException {
    long partSize = conf.getLong(HAR_PARTSIZE_LABEL, HAR_PARTSIZE_DEFAULT);
    
    FileSystem srcFS = FileSystem.getLocal(conf);
    int partId = 0;
    FSDataOutputStream partStream = null;
    
    // index entries will be sorted by hash
    TreeMap<Integer, String> indexEntries = new TreeMap<Integer, String>();
    Queue<LocalAndArchivePaths> queue = new LinkedList<LocalAndArchivePaths>();
 
    try {
      queue.add(new LocalAndArchivePaths(srcDir, Path.SEPARATOR));
      while (!queue.isEmpty()) {
        LocalAndArchivePaths item = queue.remove();
        Path localPath = item.getLocalPath();
        String archiveItem = item.getArchivePath();

        FileStatus currenPathFileStatus = srcFS.getFileStatus(localPath);

        StringBuilder toWrite = new StringBuilder(
            URLEncoder.encode(item.getArchivePath().toString(), "UTF-8"));

        String properties = new HarProperties(currenPathFileStatus).serialize();

        if (currenPathFileStatus.isDir()) {
          FileStatus chlids[] = srcFS.listStatus(localPath);
          toWrite.append(" dir ");
          toWrite.append(properties);
          toWrite.append(" 0 0");
          for (FileStatus child : chlids) {
            Path childPath = child.getPath();
            String nextArchiveItem = new Path(archiveItem, childPath.getName()).toString(); 
            queue.add(new LocalAndArchivePaths(childPath, nextArchiveItem));
            toWrite.append(" ");
            toWrite.append(URLEncoder.encode(childPath.getName().toString(), "UTF-8"));
          }
          toWrite.append("\n");
        } else {
          if (partStream == null) {
            partStream = createNewPartStream(dst, partId);
          }

          toWrite.append(" file ");
          toWrite.append("part-" + partId);
          toWrite.append(" ");
          toWrite.append(partStream.getPos() + " " + currenPathFileStatus.getLen());
          toWrite.append(" " + properties);
          toWrite.append("\n");

          InputStream input = srcFS.open(localPath);
          IOUtils.copyBytes(input, partStream, conf, false);

          // proceed to next part
          if (partStream.getPos() >= partSize) {
            ++partId;
            partStream.close();
            partStream = null;
          }
        }
        int hash = HarFileSystem.getHarHash(archiveItem);
        indexEntries.put(new Integer(hash), toWrite.toString());
      }
    } finally {
      if (partStream != null) {
        partStream.close();
      }
    }

    // Now create master index
    // IndexEntries are already sorted by hash
    Path index = new Path(dst, HarFileSystem.INDEX_NAME);
    Path masterIndex = new Path(dst, HarFileSystem.MASTER_INDEX_NAME);
    
    FSDataOutputStream indexStream = null;
    FSDataOutputStream masterIndexStream = null;
    
    try {
      FileSystem dstFS = index.getFileSystem(conf);
      indexStream = dstFS.create(index);
      masterIndexStream = dstFS.create(masterIndex);
      String version = VERSION + "\n";
      masterIndexStream.write(version.getBytes());

      int startHash = 0;
      int endHash = 0;
      long indexStartPos = 0;
      long indexEndPos = 0;
      long numLines = 0;
      for (Map.Entry<Integer, String> indexEntry : indexEntries.entrySet()) {
        if (numLines == 0) {
          startHash = indexEntry.getKey();
          indexStartPos = indexStream.getPos();
        }
        endHash = indexEntry.getKey();
        indexStream.write(indexEntry.getValue().getBytes());
        ++numLines;
        if (numLines >= HadoopArchives.NUM_LINES_IN_BLOCK_INDEX) {
          numLines = 0;
          indexEndPos = indexStream.getPos();
          writeLineToMasterIndex(masterIndexStream, startHash, endHash, indexStartPos, indexEndPos);
        }
      }

      if (numLines > 0) {
        numLines = 0;
        indexEndPos = indexStream.getPos();
        writeLineToMasterIndex(masterIndexStream, startHash, endHash, indexStartPos, indexEndPos);
      }

    } finally {
      if (indexStream != null) {
        indexStream.close();
      }

      if (masterIndexStream != null) {
        masterIndexStream.close();
      }
    }
  }
  
  private void copyToLocal(Path archivePath, Path local) throws IOException {
    HarReader harReader = new HarReader(archivePath, conf);
    FileSystem localFS = FileSystem.getLocal(conf);
    FileSystem fs = archivePath.getFileSystem(conf);
    if (!localFS.getFileStatus(local).isDir()) {
      throw new IOException("Path " + local + " is not a directory");
    }
    try {
      while (harReader.hasNext()) {
        HarStatus harStatus = harReader.getNext();
        String harPath = harStatus.getName();
        
        // skip top level dir
        if (harPath.equals(Path.SEPARATOR)) {
          continue;
        }
        String relativePath = harPath.substring(1);
        Path output = new Path(local, relativePath);
        if (harStatus.isDir()) {
          localFS.mkdirs(output);
        } else {
          OutputStream outputStream = null;
          FSDataInputStream inputStream = null;
          
          try {
            outputStream = localFS.create(output);
            Path partFile = new Path(archivePath, harStatus.getPartName());
            inputStream = new HarFSDataInputStream(fs, partFile,
                harStatus.getStartIndex(), harStatus.getLength(), conf.getInt("io.file.buffer.size", 4096));
            IOUtils.copyBytes(inputStream, outputStream, conf);
          } finally {
            if (outputStream != null) {
              outputStream.close();
            }
          }
        }
      }
    } finally {
      if (harReader != null) {
        harReader.close();
      }
    }
  }

  /** 
   * General interface to parse command line arguments
   * and then execute needed actions
   */
  private interface Executor {
    public void parse(String[] args) throws Exception;
    public void run() throws Exception;
  }
  
  private class CopyFromLocalExecutor implements Executor {
    private Path sourceDir;
    private Path harDestination;
    Configuration conf;

    public CopyFromLocalExecutor(Configuration conf) {
      this.conf = conf;
    }
    
    @Override
    public void parse(String[] args) throws Exception {
      if (args.length != 2) {
        throw new ParseException("Not enough arguments to parse: expected 2, found "
            + args.length);
      }
      sourceDir = new Path(args[0]);
      harDestination = new Path(args[1]);
    }
    
    @Override
    public void run() throws Exception {
      copyFromLocal(sourceDir, harDestination);
    }
  }

  private class CopyToLocalExecutor implements Executor {
    private Path harArchive;
    private Path localFolder;
    Configuration conf;

    public CopyToLocalExecutor(Configuration conf) {
      this.conf = conf;
    }
    
    @Override
    public void parse(String[] args) throws Exception {
      if (args.length != 2) {
        throw new ParseException("Not enough arguments to parse: expected 2, found "
            + args.length);
      }
      harArchive = new Path(args[0]);
      localFolder = new Path(args[1]);
    }
    
    @Override
    public void run() throws Exception {
      copyToLocal(harArchive, localFolder);
    }
  }
  
  private class AppendFromArchiveExecutor implements Executor {
    private Path harSource;
    private List<Path> pathsInHar;
    private Path harDestination;
    private Configuration conf;

    public AppendFromArchiveExecutor(Configuration conf) {
      this.conf = conf;
    }

    public void parse(String[] args) throws Exception {
      if (args.length < 3) {
        throw new ParseException("Not enough arguments to parse: expected >= 3, found " + args.length);
      }
      harSource = new Path(args[0]);
      harDestination = new Path(args[args.length - 1]);
      pathsInHar = new ArrayList<Path>();
      for (int i = 1; i < args.length - 1; ++i) {
        pathsInHar.add(new Path(args[i]));
      }
    }
    
    public void run() throws Exception {
      appendFromArchive(harSource, pathsInHar, harDestination);
    }
  }
  
  private abstract class ArchiveExecutorBase implements Executor {
    protected Path parentPath;
    protected List<Path> pathsToArchive;
    protected Path harDestination;
    protected Configuration conf;
    
    public ArchiveExecutorBase(Configuration conf) {
      this.conf = conf;
    }
    
    @Override
    public void parse(String[] args) throws Exception {
      Options options = new Options();
      Option parentOption = OptionBuilder.isRequired().hasArg().create("p");
      options.addOption(parentOption);
      
      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(options, args);
      
      parentPath = new Path(cmd.getOptionValue("p"));
      parsePositionalOptions(cmd.getArgs());
    }
    
    private void parsePositionalOptions(String[] args) throws Exception {
      if (args.length < 2) {
        throw new ParseException("Not enough arguments to parse: expected >= 2, found " + args.length);
      }
      String archiveName = args[0];
      if (!checkValidName(archiveName)) {
        throw new ParseException("Invalid archive name: " + archiveName);
        
      }
      harDestination = new Path(args[args.length - 1], archiveName);
      pathsToArchive = new ArrayList<Path>();
      if (args.length == 2) {
        // assuming if the user does not specify path for sources
        // the whole parent directory needs to be archived. 
        pathsToArchive.add(parentPath);
        return;
      }
      
      // process other paths
      for (int i = 1; i < args.length - 1; ++i) {
        Path argPath = new Path(args[i]);
        if (argPath.isAbsolute()) {
          throw new ParseException("source path " + argPath +
              " is not relative  to "+ parentPath);
        }
        Path srcPath = new Path(parentPath, argPath);
        FileSystem fs = srcPath.getFileSystem(conf);
        FileStatus[] statuses = fs.globStatus(srcPath);
        for (FileStatus status : statuses) {
          pathsToArchive.add(fs.makeQualified(status.getPath()));
        }
      }
    }
  }

  private class ArchiveExecutor extends ArchiveExecutorBase {
    public ArchiveExecutor(Configuration conf) {
      super(conf);
    }
    
    @Override
    public void run() throws Exception {
      archive(parentPath, pathsToArchive, harDestination, false);
    }
  }
  
  private class AppendExecutor extends ArchiveExecutorBase {
    public AppendExecutor(Configuration conf) {
      super(conf);
    }
    
    @Override
    public void run() throws Exception {
      archive(parentPath, pathsToArchive, harDestination, true);
    }
  }

  private void doRun(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println(USAGE);
      throw new ParseException("Invalid usage: command was not specified");
    }
    
    String command = args[0];
    Executor executor = executors.get(command);
    if (executor == null) {
      System.err.println("Unknown command: " + command + ". Available commands:");
      for (String cmd : executors.keySet()) {
        System.err.println(cmd);
      }
      throw new ParseException("Unknown command: " + command);
    }

    String[] otherArgs = new String[args.length - 1];
    for (int i = 1; i < args.length; ++i) {
      otherArgs[i-1] = args[i];
    }

    try {
      executor.parse(otherArgs);
    } catch (Exception e) {
      throw new Exception("Error, while parsing args.", e);
    }
    executor.run();
  }
  
  @Override
  public int run(String[] args) throws Exception {
    try {
      doRun(args);
    } catch(Exception e) {
      LOG.debug("Exception in archives  ", e);
      e.printStackTrace();
      System.err.println("Exception in archives");
      System.err.println(e.getLocalizedMessage());
      return 1;
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    JobConf job = new JobConf(HadoopArchives.class);
    HadoopArchives harchives = new HadoopArchives(job);
    int ret = ToolRunner.run(harchives, args);
    System.exit(ret);
  }
}
