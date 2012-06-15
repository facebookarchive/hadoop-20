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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** 
 * A base class for file-based {@link InputFormat}.
 * 
 * <p><code>FileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobConf, int)}.
 * Subclasses of <code>FileInputFormat</code> can also override the 
 * {@link #isSplitable(FileSystem, Path)} method to ensure input-files are
 * not split-up and are processed as a whole by {@link Mapper}s.
 * @deprecated Use {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}
 *  instead.
 */
@Deprecated
public abstract class FileInputFormat<K, V> implements InputFormat<K, V> {

  public static final Log LOG =
    LogFactory.getLog(FileInputFormat.class);

  private static final double SPLIT_SLOP = 1.1;   // 10% slop

  private long minSplitSize = 1;
  protected static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName(); 
        return !name.startsWith("_") && !name.startsWith("."); 
      }
    }; 
  protected void setMinSplitSize(long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  /**
   * Proxy PathFilter that accepts a path only if all filters given in the
   * constructor do. Used by the listPaths() to apply the built-in
   * hiddenFileFilter together with a user provided one (if any).
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Is the given filename splitable? Usually, true, but if the file is
   * stream compressed, it will not be.
   * 
   * <code>FileInputFormat</code> implementations can override this and return
   * <code>false</code> to ensure that individual input files are never split-up
   * so that {@link Mapper}s process entire files.
   * 
   * @param fs the file system that the file is on
   * @param filename the file name to check
   * @return is this file splitable?
   */
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return true;
  }
  
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
                                               JobConf job,
                                               Reporter reporter)
    throws IOException;

  /**
   * Set a PathFilter to be applied to the input paths for the map-reduce job.
   *
   * @param filter the PathFilter class use for filtering the input paths.
   */
  public static void setInputPathFilter(JobConf conf,
                                        Class<? extends PathFilter> filter) {
    conf.setClass("mapred.input.pathFilter.class", filter, PathFilter.class);
  }

  /**
   * Get a PathFilter instance of the filter set for the input paths.
   *
   * @return the PathFilter instance set for the job, NULL if none has been set.
   */
  public static PathFilter getInputPathFilter(JobConf conf) {
    Class<? extends PathFilter> filterClass = conf.getClass(
	"mapred.input.pathFilter.class", null, PathFilter.class);
    return (filterClass != null) ?
        ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /**
   * Add files in the input path recursively into the results.
   * Mark this method as final.
   * if a subclass overrides this method, it should instead override 
   * {@link #addInputPathRecursively(List, FileSystem, Path, PathFilter)}.
   * 
   * @param result
   *          The List to store all files.
   * @param fs
   *          The FileSystem.
   * @param path
   *          The input path.
   * @param inputFilter
   *          The input filter that can be used to filter files/dirs. 
   * @throws IOException
   */
  final static protected void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter) 
      throws IOException {
    for(FileStatus stat: fs.listStatus(path, inputFilter)) {
      if (stat.isDir()) {
        addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
      } else {
        result.add(stat);
      }
    }          
  }
    
  /** List input directories.
   * Mark this method to be final to make sure this method does not
   * get overridden by any subclass.
   * If a subclass historically overrides this method, now it needs to override
   * {@link #listLocatedStatus(JobConf)} instead. 
   * 
   * @param job the job to list input paths for
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  final static protected FileStatus[] listStatus(JobConf job) throws IOException {
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // Whether we need to recursive look into the directory structure
    boolean recursive = job.getBoolean("mapred.input.dir.recursive", false);
    
    List<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();
    
    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    for (Path p: dirs) {
      FileSystem fs = p.getFileSystem(job); 
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDir()) {
            for(FileStatus stat: fs.listStatus(globStat.getPath(),
                inputFilter)) {
              if (recursive && stat.isDir()) {
                addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
              } else {
                result.add(stat);
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    LOG.info("Total input paths to process : " + result.size()); 
    return result.toArray(new FileStatus[result.size()]);
  }
  
  /**
   * Add files in the input path recursively into the results.
   * @param result
   *          The List to store all files together with their block locations
   * @param fs
   *          The FileSystem.
   * @param path
   *          The input path.
   * @param inputFilter
   *          The input filter that can be used to filter files/dirs. 
   * @throws IOException
   */
  protected void addLocatedInputPathRecursively(List<LocatedFileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter) 
      throws IOException {
    for(RemoteIterator<LocatedFileStatus> itor = 
      fs.listLocatedStatus(path, inputFilter); itor.hasNext();) {
      LocatedFileStatus stat = itor.next();
      if (stat.isDir()) {
        addLocatedInputPathRecursively(result, fs, stat.getPath(), inputFilter);
      } else {
        result.add(stat);
      }
    }          
  }
  
  /** List input directories.
   * The file locations are also returned together with the file status.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression. 
   * 
   * @param job the job to list input paths for
   * @return array of LocatedFileStatus objects
   * @throws IOException if zero items.
   */
  protected LocatedFileStatus[] listLocatedStatus(JobConf job)
  throws IOException {
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // Whether we need to recursive look into the directory structure
    final boolean recursive = job.getBoolean("mapred.input.dir.recursive", false);
    
    final List<LocatedFileStatus> result =
      Collections.synchronizedList(new ArrayList<LocatedFileStatus>());
    final List<IOException> errors =
      Collections.synchronizedList(new ArrayList<IOException>());
    final List<IOException> ioexceptions = 
      Collections.synchronizedList(new ArrayList<IOException>());
    
    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    final PathFilter inputFilter = new MultiPathFilter(filters);
    ThreadPoolExecutor executor = null;
    int numExecutors = Math.min(job.getInt("mapred.dfsclient.parallelism.max", 1), dirs.length);

    if (numExecutors > 1) {
      LOG.info("Using " + numExecutors + " threads for listLocatedStatus");
      executor = new ThreadPoolExecutor(numExecutors, numExecutors, 60, TimeUnit.SECONDS,
                                        new LinkedBlockingQueue<Runnable>());
    }

    for (Path one: dirs) {
      final Path p = one;
      final FileSystem fs = p.getFileSystem(job); 

      Runnable r = new Runnable () {
          public void run() {
            try {
              List<Path> plist = new ArrayList<Path> ();

              if (p.isAbsolute() && !FileSystem.hasGlobComponent(p)) {
                plist.add(p);
              } else {
                FileStatus[] matches = fs.globStatus(p, inputFilter);

                if (matches == null) {
                    errors.add(new IOException("Input path does not exist: " + p));
                } else if (matches.length == 0) {
                    errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
                } else {
                  for (FileStatus globStat: matches)
                    plist.add(globStat.getPath());
                }
              }

              for (Path onePath: plist) {
                for(RemoteIterator<LocatedFileStatus> itor = fs.listLocatedStatus
                      (onePath, inputFilter); itor.hasNext();) {
                  LocatedFileStatus stat = itor.next();
                  if (recursive && stat.isDir()) {
                    addLocatedInputPathRecursively
                      (result, fs, stat.getPath(), inputFilter);
                  } else {
                      result.add(stat);
                  }
                }
              }

            } catch (IOException e) {
                ioexceptions.add(e);
            }
          }
        };

      if (executor != null)
        executor.submit(r);
      else
        r.run();
    }

    boolean executorDone = false;
    if (executor != null) {
      executor.shutdown();
      do {
        try {
          executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
          executorDone = true;
        } catch (InterruptedException e) {
        }
      } while (!executorDone);
    }

    if (!ioexceptions.isEmpty()) {
      throw ioexceptions.get(0);
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    LOG.info("Total input paths to process : " + result.size()); 
    verifyLocatedFileStatus(job, result);
    return result.toArray(new LocatedFileStatus[result.size()]);
  }

  private void verifyLocatedFileStatus(
      JobConf conf, List<LocatedFileStatus> stats)
      throws IOException {
    if (!conf.getBoolean("mapred.fileinputformat.verifysplits", true)) {
      return;
    }
    for (LocatedFileStatus stat: stats) {
      long fileLen = stat.getLen();
      long blockLenTotal = 0;
      for (BlockLocation loc: stat.getBlockLocations()) {
        blockLenTotal += loc.getLength();
      }
      if (blockLenTotal != fileLen) {
        throw new IOException("Error while getting located status, " +
          stat.getPath() + " has length " + fileLen + " but blocks total is " +
          blockLenTotal);
      }
    }
  }

  /** Splits files returned by {@link #listStatus(JobConf)} when
   * they're too big.*/ 
  @SuppressWarnings("deprecation")
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    LocatedFileStatus[] files = listLocatedStatus(job);
    
    long totalSize = 0;                           // compute total size
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDir()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      totalSize += file.getLen();
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
                            minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (LocatedFileStatus file: files) {
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(job);
      long length = file.getLen();
      BlockLocation[] blkLocations = file.getBlockLocations();

      if ((length != 0) && isSplitable(fs, path)) { 
        long blockSize = file.getBlockSize();
        long splitSize = computeSplitSize(goalSize, minSize, blockSize);

        long bytesRemaining = length;
        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
          String[] splitHosts = getSplitHosts(blkLocations, 
              length-bytesRemaining, splitSize, clusterMap);
          splits.add(new FileSplit(path, length-bytesRemaining, splitSize, 
              splitHosts));
          bytesRemaining -= splitSize;
        }
        
        if (bytesRemaining != 0) {
          splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, 
                     blkLocations[blkLocations.length-1].getHosts()));
        }
      } else if (length != 0) {
        String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
        splits.add(new FileSplit(path, 0, length, splitHosts));
      } else { 
        //Create empty hosts array for zero length files
        splits.add(new FileSplit(path, 0, length, new String[0]));
      }
    }
    LOG.debug("Total # of splits: " + splits.size());
    return splits.toArray(new FileSplit[splits.size()]);
  }

  protected long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));
  }

  protected int getBlockIndex(BlockLocation[] blkLocations, 
                              long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length -1];
    long fileLength = last.getOffset() + last.getLength() -1;
    throw new IllegalArgumentException("Offset " + offset + 
                                       " is outside of file (0.." +
                                       fileLength + ")");
  }

  /**
   * Sets the given comma separated paths as the list of inputs 
   * for the map-reduce job.
   * 
   * @param conf Configuration of the job
   * @param commaSeparatedPaths Comma separated paths to be set as 
   *        the list of inputs for the map-reduce job.
   */
  public static void setInputPaths(JobConf conf, String commaSeparatedPaths) {
    setInputPaths(conf, StringUtils.stringToPath(
                        getPathStrings(commaSeparatedPaths)));
  }

  /**
   * Add the given comma separated paths to the list of inputs for
   *  the map-reduce job.
   * 
   * @param conf The configuration of the job 
   * @param commaSeparatedPaths Comma separated paths to be added to
   *        the list of inputs for the map-reduce job.
   */
  public static void addInputPaths(JobConf conf, String commaSeparatedPaths) {
    for (String str : getPathStrings(commaSeparatedPaths)) {
      addInputPath(conf, new Path(str));
    }
  }

  /**
   * Set the array of {@link Path}s as the list of inputs
   * for the map-reduce job.
   * 
   * @param conf Configuration of the job. 
   * @param inputPaths the {@link Path}s of the input directories/files 
   * for the map-reduce job.
   */ 
  public static void setInputPaths(JobConf conf, Path... inputPaths) {
    if (!inputPaths[0].isAbsolute()) {
      FileSystem.LogForCollect.info("set relative path to non absolute path: "
          + inputPaths[0]+ " working directory: " + conf.getWorkingDirectory());
    }
    Path path = new Path(conf.getWorkingDirectory(), inputPaths[0]);
    StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < inputPaths.length;i++) {
      str.append(StringUtils.COMMA_STR);
      if (!inputPaths[i].isAbsolute()) {
        FileSystem.LogForCollect.info("set input path to non absolute path: "
            + inputPaths[i] + " working directory: "
            + conf.getWorkingDirectory());
      }
      path = new Path(conf.getWorkingDirectory(), inputPaths[i]);
      str.append(StringUtils.escapeString(path.toString()));
    }
    conf.set("mapred.input.dir", str.toString());
  }

  /**
   * Add a {@link Path} to the list of inputs for the map-reduce job.
   * 
   * @param conf The configuration of the job 
   * @param path {@link Path} to be added to the list of inputs for 
   *            the map-reduce job.
   */
  public static void addInputPath(JobConf conf, Path path ) {
    if (!path.isAbsolute()) {
      FileSystem.LogForCollect.info("set input path to relative path: " + path
          + " working directory: " + conf.getWorkingDirectory());
    }
    path = new Path(conf.getWorkingDirectory(), path);
    String dirStr = StringUtils.escapeString(path.toString());
    String dirs = conf.get("mapred.input.dir");
    conf.set("mapred.input.dir", dirs == null ? dirStr :
      dirs + StringUtils.COMMA_STR + dirStr);
  }
         
  // This method escapes commas in the glob pattern of the given paths.
  private static String[] getPathStrings(String commaSeparatedPaths) {
    int length = commaSeparatedPaths.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();
    
    for (int i=0; i<length; i++) {
      char ch = commaSeparatedPaths.charAt(i);
      switch(ch) {
        case '{' : {
          curlyOpen++;
          if (!globPattern) {
            globPattern = true;
          }
          break;
        }
        case '}' : {
          curlyOpen--;
          if (curlyOpen == 0 && globPattern) {
            globPattern = false;
          }
          break;
        }
        case ',' : {
          if (!globPattern) {
            pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
            pathStart = i + 1 ;
          }
          break;
        }
      }
    }
    pathStrings.add(commaSeparatedPaths.substring(pathStart, length));
    
    return pathStrings.toArray(new String[0]);
  }
  
  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   * 
   * @param conf The configuration of the job 
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  public static Path[] getInputPaths(JobConf conf) {
    String dirs = conf.get("mapred.input.dir", "");
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }
  

  private void sortInDescendingOrder(List<NodeInfo> mylist) {
    Collections.sort(mylist, new Comparator<NodeInfo> () {
      public int compare(NodeInfo obj1, NodeInfo obj2) {

        if (obj1 == null || obj2 == null)
          return -1;

        if (obj1.getValue() == obj2.getValue()) {
          return 0;
        }
        else {
          return ((obj1.getValue() < obj2.getValue()) ? 1 : -1);
        }
      }
    }
    );
  }

  /** 
   * This function identifies and returns the hosts that contribute 
   * most for a given split. For calculating the contribution, rack
   * locality is treated on par with host locality, so hosts from racks
   * that contribute the most are preferred over hosts on racks that 
   * contribute less
   * @param blkLocations The list of block locations
   * @param offset 
   * @param splitSize 
   * @return array of hosts that contribute most to this split
   * @throws IOException
   */
  protected String[] getSplitHosts(BlockLocation[] blkLocations, 
      long offset, long splitSize, NetworkTopology clusterMap)
  throws IOException {

    int startIndex = getBlockIndex(blkLocations, offset);

    long bytesInThisBlock = blkLocations[startIndex].getOffset() + 
                          blkLocations[startIndex].getLength() - offset;

    //If this is the only block, just return
    if (bytesInThisBlock >= splitSize) {
      return blkLocations[startIndex].getHosts();
    }

    long bytesInFirstBlock = bytesInThisBlock;
    int index = startIndex + 1;
    splitSize -= bytesInThisBlock;

    while (splitSize > 0) {
      bytesInThisBlock =
        Math.min(splitSize, blkLocations[index++].getLength());
      splitSize -= bytesInThisBlock;
    }

    long bytesInLastBlock = bytesInThisBlock;
    int endIndex = index - 1;
    
    Map <Node,NodeInfo> hostsMap = new IdentityHashMap<Node,NodeInfo>();
    Map <Node,NodeInfo> racksMap = new IdentityHashMap<Node,NodeInfo>();
    String [] allTopos = new String[0];

    // Build the hierarchy and aggregate the contribution of 
    // bytes at each level. See TestGetSplitHosts.java 

    for (index = startIndex; index <= endIndex; index++) {

      // Establish the bytes in this block
      if (index == startIndex) {
        bytesInThisBlock = bytesInFirstBlock;
      }
      else if (index == endIndex) {
        bytesInThisBlock = bytesInLastBlock;
      }
      else {
        bytesInThisBlock = blkLocations[index].getLength();
      }
      
      allTopos = blkLocations[index].getTopologyPaths();

      // If no topology information is available, just
      // prefix a fakeRack
      if (allTopos.length == 0) {
        allTopos = fakeRacks(blkLocations, index);
      }

      // NOTE: This code currently works only for one level of
      // hierarchy (rack/host). However, it is relatively easy
      // to extend this to support aggregation at different
      // levels 
      
      for (String topo: allTopos) {

        Node node, parentNode;
        NodeInfo nodeInfo, parentNodeInfo;

        node = clusterMap.getNode(topo);

        if (node == null) {
          node = new NodeBase(topo);
          clusterMap.add(node);
        }
        
        nodeInfo = hostsMap.get(node);
        
        if (nodeInfo == null) {
          nodeInfo = new NodeInfo(node);
          hostsMap.put(node,nodeInfo);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
          if (parentNodeInfo == null) {
            parentNodeInfo = new NodeInfo(parentNode);
            racksMap.put(parentNode,parentNodeInfo);
          }
          parentNodeInfo.addLeaf(nodeInfo);
        }
        else {
          nodeInfo = hostsMap.get(node);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
        }

        nodeInfo.addValue(index, bytesInThisBlock);
        parentNodeInfo.addValue(index, bytesInThisBlock);

      } // for all topos
    
    } // for all indices

    return identifyHosts(allTopos.length, racksMap);
  }
  
  private String[] identifyHosts(int replicationFactor, 
                                 Map<Node,NodeInfo> racksMap) {
    
    String [] retVal = new String[replicationFactor];
    // The replicationFactor passed in can be zero when there are missing blocks
    // in a file. In that case, we want to return an empty array of locations.
    if (replicationFactor == 0) {
      return retVal;
    }
   
    List <NodeInfo> rackList = new LinkedList<NodeInfo>(); 

    rackList.addAll(racksMap.values());
    
    // Sort the racks based on their contribution to this split
    sortInDescendingOrder(rackList);
    
    boolean done = false;
    int index = 0;
    
    // Get the host list for all our aggregated items, sort
    // them and return the top entries
    for (NodeInfo ni: rackList) {

      Set<NodeInfo> hostSet = ni.getLeaves();

      List<NodeInfo>hostList = new LinkedList<NodeInfo>();
      hostList.addAll(hostSet);
    
      // Sort the hosts in this rack based on their contribution
      sortInDescendingOrder(hostList);

      for (NodeInfo host: hostList) {
        // Strip out the port number from the host name
        retVal[index++] = host.node.getName().split(":")[0];
        if (index == replicationFactor) {
          done = true;
          break;
        }
      }
      
      if (done == true) {
        break;
      }
    }
    return retVal;
  }
  
  private String[] fakeRacks(BlockLocation[] blkLocations, int index) 
  throws IOException {
    String[] allHosts = blkLocations[index].getHosts();
    String[] allTopos = new String[allHosts.length];
    for (int i = 0; i < allHosts.length; i++) {
      allTopos[i] = NetworkTopology.DEFAULT_RACK + "/" + allHosts[i];
    }
    return allTopos;
  }


  private static class NodeInfo {
    final Node node;
    final Set<Integer> blockIds;
    final Set<NodeInfo> leaves;

    private long value;
    
    NodeInfo(Node node) {
      this.node = node;
      blockIds = new HashSet<Integer>();
      leaves = new HashSet<NodeInfo>();
    }

    long getValue() {return value;}

    void addValue(int blockIndex, long value) {
      if (blockIds.add(blockIndex) == true) {
        this.value += value;
      }
    }

    Set<NodeInfo> getLeaves() { return leaves;}

    void addLeaf(NodeInfo nodeInfo) {
      leaves.add(nodeInfo);
    }
  }
}
