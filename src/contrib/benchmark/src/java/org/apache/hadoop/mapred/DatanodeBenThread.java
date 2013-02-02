package org.apache.hadoop.mapred;
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

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.GeneralConstant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Random;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapred.GenWriterThread.TokenBucket;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

@SuppressWarnings("deprecation")
  /**
   * DatanodeBenThread is used for benchmark datanode. Its test type is dnben
   * There are two steps in the dnben:
   * 1. prepare step:
   *   Has multiple mappers, each mapper is running on one datanode.
   *   Each mapper writes N files that has only 1 replia.
   *   Because the first replica is written locally, it ends up with
   *   N 1-replica files for each datanode  
   * 2. Stress test step
   *   It will first randomly pick N (defined by -dn) victim datanodes
   *   from the cluster, these victim datanodes need to satisfy two conditions:
   *   a. it's alive in all namespaces
   *   b. it has at least M (defined by -minfile) files under workdir 
   *   Then in each mapper, we will spawn T threads and T/X threads per 
   *   namespace where X is the number of namespaces
   *   Each thread will have P (defined by pread) probability to become
   *   a read thread and 1-P probability to become a write thread.
   *   For read thread:
   *     it will keep picking a random file belongs to one victim node and 
   *     read one buffer data from the file. Because the victim node contains
   *     the only replica of the file. All the reads will go to victim nodes.
   *   For write thread:
   *     It will pass victim nodes as favornodes to the DFSClient and namenode
   *     will allocate victim nodes to the new files. Most of the writes will 
   *     go to victim nodes
   * @author weiyan
   */
  public class DatanodeBenThread extends GenThread implements
    GeneralConstant {
    private static final Log LOG = LogFactory.getLog(DatanodeBenThread.class);
    public static final String TEST_TYPE = "dnben"; 
    public static final long DEFAULT_MAX_TIME_SEC = 60;
    public static final long DEFAULT_MIN_NUMBER_OF_FILES_PER_DATANODE = 60; 
    public static final long DEFAULT_FILE_SIZE = 10; //10MB
    public static final long DEFAULT_DATANODE_NUMBER = 1; 
    public static final float DEFAULT_READ_PERCENT = 1.0f;
    public static final long DEFAULT_MAX_NUMBER_OF_FILES_PER_THREAD = 
        Long.MAX_VALUE;
    public static final short DEFAULT_REPLICATION_NUM = 3;
    
    public static final String MAX_TIME_SEC_KEY = "max.time.sec";
    public static final String FILE_SIZE_KEY = "dfs.MB.file.size";
    public static final String DATANODE_NUMBER_KEY = 
        "dfs.stress.test.datanode.num";
    public static final String READ_PERCENT_KEY = "dfs.read.percent";
    public static final String MIN_FILE_PER_DATANODE_KEY = 
        "dfs.min.file.per.datanode";
    public static final String RUNNING_TYPE_KEY = "dfs.running.type";
    public static final String MAX_NUMBER_OF_FILES_PER_THREAD_KEY = 
        "dfs.max.nfile.per.thread";
    public static final String VICTIM_DATANODE_KEY = 
        "dfs.victim.datanodes";
    public static enum RUNNING_TYPE {
      PREPARE, GENERAL, READ, WRITE
    }
    
    public class DatanodeBenRunTimeConstants extends RunTimeConstants {
      long max_time = DEFAULT_MAX_TIME_SEC * 1000;
      long data_rate = DEFAULT_DATA_RATE * 1024;
      long min_file = DEFAULT_MIN_NUMBER_OF_FILES_PER_DATANODE;
      long file_size = DEFAULT_FILE_SIZE * 1024 * 1024;
      long max_files = DEFAULT_MAX_NUMBER_OF_FILES_PER_THREAD;
      String task_name = null;
      //Used by prepare 
      String cur_datanode = null;
      //Used by General
      InetSocketAddress[] victims = null;
      Set<String> victimSet = null;
      // namespace default uri -> {datanode host name -> list of files} 
      HashMap<String, HashMap<String, ArrayList<Path>>> pickLists = null;
    }
    
    private DatanodeBenRunTimeConstants rtc = null;
    private int id;
    private String file_prefix = "";
    private RUNNING_TYPE running_type = RUNNING_TYPE.GENERAL;
    private Random rb = new Random();
    private HashMap<String, ArrayList<Path>> nsPickLists = null;
    private DistributedFileSystem dfs = null;
    public TokenBucket tb = null;
    public short replication = 3;
    public float pread = DEFAULT_READ_PERCENT;
    public long max_size = DEFAULT_FILE_SIZE * 1024 * 1024;
    public String thread_name = null;
    // Counters
    private long read_size = 0;
    private long write_size = 0;
    private float average_read_rate = 0;
    private float average_write_rate = 0;
    private long total_num = 0;
    
    
    public DatanodeBenThread() {
      
    }
    
    public DatanodeBenThread(Configuration conf, Path input, Path output, int id,
        RUNNING_TYPE init_type, DatanodeBenRunTimeConstants rtc) throws IOException{
      super(conf, input, output, rtc);
      this.rtc = rtc;
      this.replication = (short)conf.getInt(REPLICATION_KEY, DEFAULT_REPLICATION_NUM);
      this.max_size = conf.getLong(FILE_SIZE_KEY, DEFAULT_FILE_SIZE) * 1024 * 1024;
      this.pread = conf.getFloat(READ_PERCENT_KEY, DEFAULT_READ_PERCENT);
      this.tb = new TokenBucket(rtc.data_rate);
      this.id = id;
      this.thread_name = rtc.task_name + "_" + id;
      this.running_type = init_type;
      if (running_type.equals(RUNNING_TYPE.PREPARE)) {
        this.file_prefix = rtc.cur_datanode + thread_name +  "_part";
      } else {
        this.file_prefix = thread_name + "_part";
        this.nsPickLists = rtc.pickLists.get(conf.get(FileSystem.FS_DEFAULT_NAME_KEY));
        this.dfs = (DistributedFileSystem)fs;
        float f = rb.nextFloat();
        if (f < pread + 1e-9) {
          this.running_type = RUNNING_TYPE.READ;
        } else {
          this.outputPath = new Path(outputPath, thread_name);
          this.running_type = RUNNING_TYPE.WRITE;
        }
      }
      fs.mkdirs(this.outputPath); 
    }
    
    public DatanodeBenThread(Configuration conf, Path output,  int id,
        RUNNING_TYPE running_type, DatanodeBenRunTimeConstants rtc) 
            throws IOException{
      this(conf, null, output, id, running_type, rtc);
    }
    
    public DatanodeBenThread(JobConf conf) {
      super(conf);
    }
    
    /*
     * Look at the output directory to see how many files belong to 
     * the current datanode
     */
    public int getNumberOfFiles() throws IOException {
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      RemoteIterator<LocatedFileStatus> iter = dfs.listLocatedStatus(outputPath);
      int fn = 0;
      while (iter.hasNext()) {
        LocatedFileStatus lfs = iter.next();
        if (lfs.isDir()) 
          continue;
        if (lfs.getBlockLocations().length != 1) 
          continue;
        String curHost = rtc.cur_datanode;
        for (String host: lfs.getBlockLocations()[0].getHosts()) {
          if (curHost.equals(host)){
            fn++;
            break;
          }
        }
      }
      LOG.info(" Found " + fn + " files in " + dfs.getUri());
      return fn;
    }
    
    public void write() throws Exception {
      long endTime = System.currentTimeMillis() + rtc.max_time;
      long currentId = 0;
      FSDataOutputStream out = null;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      while (System.currentTimeMillis() < endTime
          && currentId < rtc.max_files) {
        if (running_type == RUNNING_TYPE.PREPARE) {
          //The number of files reach the minimum limit, exit
          if (getNumberOfFiles() > rtc.min_file) 
            break;
        }
        Path fileName = new Path(outputPath, file_prefix + currentId);
        try { 
          out = dfs.create(fileName,
                           FsPermission.getDefault(),
                           false,
                           dfs.getConf().getInt("io.file.buffer.size", 4096),
                           (short)replication,
                           dfs.getDefaultBlockSize(),
                           dfs.getConf().getInt("io.bytes.per.checksum", 512),
                           null,
                           rtc.victims);
          long size = 0;
          while (true) {
            rb.nextBytes(buffer);
            tb.getTokens(rtc.buffer_size);
            out.write(buffer, 0, rtc.buffer_size);
            size += rtc.buffer_size;
            if (System.currentTimeMillis() > endTime 
                || size + rtc.buffer_size > max_size) {
              // Roll the file
              out.close();
              out = null;
              currentId++;
              files_processed++;
              processed_size += size;
              write_size += size;
              Path fullName = fs.makeQualified(fileName);
              BlockLocation bl = dfs.getClient().getBlockLocations(
                  fullName.toUri().getPath(), 0L, 1L)[0];
              String hosts = "";
              for (String host: bl.getHosts()) {
                hosts += host + " ";
              }
              LOG.info("[close (" + size + "B)] " + hosts + " file " + fullName);
              break;
            }
          }
        } catch (Exception e) {
          LOG.error("Error in writing file:" + fileName, e);
          this.errors.add(e);
        } finally {
          IOUtils.closeStream(out);
        }
      }
    }
    
    public void read() throws Exception {
      long endTime = System.currentTimeMillis() + rtc.max_time;
      while (System.currentTimeMillis() < endTime) {
        // Randomly pick a datanode from victims
        int idx = rb.nextInt(rtc.victims.length);
        // Randomly pick a file to read
        ArrayList<Path> fileList = nsPickLists.get(rtc.victims[idx].getHostName()); 
        int fid = rb.nextInt(fileList.size());
        Path readFile = fileList.get(fid); 
        FSDataInputStream in = null;
        try {
          in = fs.open(readFile);
          if (in.isUnderConstruction()) {
            LOG.info("file " + readFile + " is still open");
          }
          FileStatus fileStatus = fs.getFileStatus(readFile);
          long offset = rb.nextInt((int)Math.max(
              fileStatus.getLen() - rtc.buffer_size, 0) + 1);
          int size = 0;
          in.seek(offset);
          size = in.read(buffer, 0, rtc.buffer_size);
          if (size < 0) {
            continue;
          }
          processed_size += size;
          read_size += size;
          LOG.info("Read file " + readFile + " from " + 
              offset + " to " + (offset + size));
        } catch (Exception e) { 
          LOG.error("Error in read file: " + readFile, e);
          this.errors.add(e);
        } finally {
          IOUtils.closeStream(in);
        }
        files_processed++;
        Thread.sleep(5);
      }
    }
    
    public void run() {
      try {
        switch (running_type) {
        case PREPARE: 
          write();
          break;
        case READ:
          LOG.info("Read Thread: " + thread_name);
          read();
          break;
        case WRITE:
          LOG.info("Write Thread:" + thread_name);
          write();
          break;
        }
        LOG.info("Thread " + thread_name + " is done.");
      } catch (Exception ioe) {
        LOG.error("Error: ", ioe);
        this.errors.add(ioe);
      }
    } 
    
    private static HashMap<String, ArrayList<Path>> getNSPickLists(
        DistributedFileSystem dfs, String workdir, long minFile, Set<String> victims) 
            throws IOException {
      HashMap<String, ArrayList<Path>> nsPickLists = 
          new HashMap<String, ArrayList<Path>>();
      RemoteIterator<LocatedFileStatus> iter = 
          dfs.listLocatedStatus(new Path(workdir));
      while (iter.hasNext()) {
        LocatedFileStatus lfs = iter.next();
        if (lfs.isDir()) 
          continue;
        if (lfs.getBlockLocations().length != 1) 
          continue;
        for (String hostname: lfs.getBlockLocations()[0].getHosts()) {
          // Skip the uninterested datanodes
          if (victims != null && !victims.contains(hostname))
            continue;
          ArrayList<Path> value = null;
          if (!nsPickLists.containsKey(hostname)) {
            value = new ArrayList<Path>();
            nsPickLists.put(hostname, value);
          } else {
            value = nsPickLists.get(hostname);
          }
          value.add(lfs.getPath());
        }
      }
      if (victims == null) {
        String[] hostnames = nsPickLists.keySet().toArray(new String[0]);
        //Remove the datanodes with not enough files
        for (String hostname : hostnames) {
          if (nsPickLists.get(hostname).size() < minFile) {
            nsPickLists.remove(hostname);
          }
        }
      } 
      return nsPickLists;
    }

    private static DistributedFileSystem getDFS(FileSystem fs)
      throws IOException {
      if (fs instanceof DistributedRaidFileSystem)
        fs = ((DistributedRaidFileSystem)fs).getFileSystem();
      return (DistributedFileSystem)fs;
    }

    private static HashMap<String, HashMap<String, ArrayList<Path>>> 
        getPickLists(List<JobConf> nameNodeConfs, String workdir, long minFile, 
          Set<String> victims) throws IOException {
      HashMap<String, HashMap<String, ArrayList<Path>>> allList = new 
          HashMap<String, HashMap<String, ArrayList<Path>>>();
      for (JobConf nameNodeConf : nameNodeConfs) {
        FileSystem fs = FileSystem.get(nameNodeConf);
        DistributedFileSystem dfs = getDFS(fs);
        HashMap<String, ArrayList<Path>> nsPickLists = 
            getNSPickLists(dfs, workdir, minFile, victims);
        allList.put(nameNodeConf.get(FileSystem.FS_DEFAULT_NAME_KEY),
                      nsPickLists);
      }
      return allList;
    }
    
    private static Set<DatanodeInfo> getValidDatanodes(JobConf nameNodeConf,
        DatanodeBenRunTimeConstants rtc) throws IOException {
      FileSystem fs = FileSystem.get(nameNodeConf);
      DistributedFileSystem dfs = getDFS(fs);
      HashMap<String, ArrayList<Path>> nsPickLists = 
          rtc.pickLists.get(nameNodeConf.get(FileSystem.FS_DEFAULT_NAME_KEY));
      DatanodeInfo[] dnStats = dfs.getLiveDataNodeStats();
      Set<DatanodeInfo> validDatanodes = new HashSet<DatanodeInfo>();
      for (DatanodeInfo dn: dnStats) {
        if (dn.isDecommissioned() || dn.isDecommissionInProgress()) {
          continue;
        }
        if (!nsPickLists.containsKey(dn.getHostName())) {
          continue;
        }
        validDatanodes.add(dn);
      }
      return validDatanodes;
    }
    
    /**
     * We randomly pick nDatanode datanodes to do stress tests. 
     * Valid datanodes should satisfy two conditions:
     * 1. alive in all namespaces
     * 2. have at lease min_file files under the working directory.
     */
    public List<DatanodeInfo> getTestDatanodes(List<JobConf> nameNodeConfs, 
        String workdir, long nDatanode, long minFile) throws IOException {
      this.rtc = new DatanodeBenRunTimeConstants();
      rtc.pickLists = getPickLists(nameNodeConfs, workdir, minFile, null);
      Set<DatanodeInfo> validDatanodes = getValidDatanodes(nameNodeConfs.get(0), rtc);
      for (int i = 1; i < nameNodeConfs.size(); i++) {
        Set<DatanodeInfo> tmpDatanodes = getValidDatanodes(nameNodeConfs.get(i), rtc);
        Set<DatanodeInfo> mergeDatanodes = new HashSet<DatanodeInfo>();
        for (DatanodeInfo dn: tmpDatanodes) 
          if (validDatanodes.contains(dn)) {
            mergeDatanodes.add(dn);
          }
        validDatanodes = mergeDatanodes;
      }
      LOG.info("There are " + validDatanodes.size() + " valid datanodes.");
      String logInfo = "";
      List<DatanodeInfo> dnList = new ArrayList<DatanodeInfo>();
      for (DatanodeInfo dn : validDatanodes) {
        logInfo += ' ' + dn.getHostName();
        dnList.add(dn);
      }
      LOG.info(logInfo);
      if (dnList.size() < nDatanode) 
        return dnList;
      Collections.shuffle(dnList);
      return dnList.subList(0, (int)nDatanode);
    }
    
    /**
     * Write a small file to figure out which datanode we are running
     */
    private String getRunningDatanode(Configuration conf)
        throws IOException {
      FileSystem fs = FileSystem.newInstance(conf);
      fs.mkdirs(new Path("/tmp"));
      Path fileName = new Path("/tmp", rtc.task_name + System.currentTimeMillis()
          + rb.nextInt());
      if (fs.exists(fileName)) {
        fs.delete(fileName);
      }
      FSDataOutputStream out = null;
      byte[] buffer= new byte[1];
      buffer[0] = '0';
      try {
        out = fs.create(fileName, (short)1);
        out.write(buffer, 0, 1);
      } finally {
        IOUtils.closeStream(out);
      }
      fs = getDFS(fs);
      assert fs instanceof DistributedFileSystem;
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      BlockLocation[] lbs = dfs.getClient().getBlockLocations(
          fileName.toUri().getPath(), 0, 1);
      fs.delete(fileName);
      return lbs[0].getHosts()[0];
    }
    
    public static List<JobConf> getNameNodeConfs(JobConf conf) 
        throws IOException {
      List<InetSocketAddress> nameNodeAddrs = 
          DFSUtil.getClientRpcAddresses(conf, null);
      List<JobConf> nameNodeConfs = 
          new ArrayList<JobConf>(nameNodeAddrs.size());
      for (InetSocketAddress nnAddr : nameNodeAddrs) {
        JobConf newConf = new JobConf(conf);
        newConf.set(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY,
            nnAddr.getHostName() + ":" + nnAddr.getPort());
        NameNode.setupDefaultURI(newConf);
        nameNodeConfs.add(newConf);
      }
      return nameNodeConfs;
    }
    
    @Override
    public GenThread[] prepare(JobConf conf, Text key, Text value)
        throws IOException {
      this.rtc = new DatanodeBenRunTimeConstants();
      super.prepare(conf, key, value, rtc);
      rtc.task_name = key.toString() + rtc.taskID;
      rtc.min_file = conf.getLong(MIN_FILE_PER_DATANODE_KEY, 
          DEFAULT_MIN_NUMBER_OF_FILES_PER_DATANODE);
      rtc.max_time = conf.getLong(MAX_TIME_SEC_KEY, DEFAULT_MAX_TIME_SEC) * 1000;
      rtc.data_rate = conf.getLong(WRITER_DATARATE_KEY, DEFAULT_DATA_RATE) * 1024;
      rtc.file_size = conf.getLong(FILE_SIZE_KEY, DEFAULT_FILE_SIZE) * 1024*1024;
      LOG.info("data rate: " + rtc.data_rate);
      String working_dir = value.toString();
      int run_type = conf.getInt(RUNNING_TYPE_KEY, 
          RUNNING_TYPE.GENERAL.ordinal());
      List<JobConf> nameNodeConfs = getNameNodeConfs(conf);
      if (run_type == RUNNING_TYPE.PREPARE.ordinal()) {
        rtc.cur_datanode = getRunningDatanode(conf);
        LOG.info("Current datanode is " + rtc.cur_datanode);
        // Make sure each namespace has same number of threads
        long nthread_per_namespace = ((rtc.nthreads-1) / nameNodeConfs.size() + 1);
        rtc.max_files = (rtc.min_file-1) / nthread_per_namespace + 1;
        rtc.nthreads =  nthread_per_namespace * nameNodeConfs.size();
        LOG.info("Number of threads: " + rtc.nthreads + " max_file:" + rtc.max_files); 
        DatanodeBenThread[] threads = new DatanodeBenThread[(int)rtc.nthreads];
        int nsIdx = 0;
        for (int i=0; i < rtc.nthreads; i++) {
          threads[i] = new DatanodeBenThread(nameNodeConfs.get(nsIdx), 
                                             new Path(working_dir), i,
                                             RUNNING_TYPE.PREPARE, rtc);
          nsIdx++;
          if (nsIdx == nameNodeConfs.size()) {
            nsIdx = 0;
          }
        }
        return threads;
      } else {
        String[] victimStrs = conf.getStrings(VICTIM_DATANODE_KEY);
        LOG.info("Victim datanodes are :" + conf.get(VICTIM_DATANODE_KEY));
        rtc.victims = new InetSocketAddress[victimStrs.length];
        rtc.victimSet = new HashSet<String>();
        for (int i = 0; i < victimStrs.length; i++) {
          //hostname:port
          String[] values = victimStrs[i].split(":");
          rtc.victims[i] = new InetSocketAddress(values[0],
                                                 Integer.parseInt(values[1]));
          rtc.victimSet.add(values[0]);
        }
        rtc.pickLists = getPickLists(nameNodeConfs, working_dir, 
            Long.MAX_VALUE, rtc.victimSet);
        DatanodeBenThread[] threads = new DatanodeBenThread[(int)rtc.nthreads];
        int nsIdx = 0;
        for (int i=0; i < rtc.nthreads; i++) {
          threads[i] = new DatanodeBenThread(nameNodeConfs.get(nsIdx), 
                                             new Path(working_dir), 
                                             new Path(rtc.output_dir), 
                                             i, RUNNING_TYPE.GENERAL, rtc);
          nsIdx++;
          if (nsIdx == nameNodeConfs.size()) {
            nsIdx = 0;
          }
        }
        return threads;
      }
    }
    
    @Override
    public Map<String, String> collectStats(JobConf conf, 
        GenThread[] threads, long execTime) throws IOException { 
      long total_read_size = 0;
      long total_write_size = 0;
      for (GenThread t: threads) {
        DatanodeBenThread dbt = (DatanodeBenThread)t;
        total_read_size += dbt.read_size;
        total_write_size += dbt.write_size;
      }
      float readRateMbSec = (float)total_read_size * 1000 / (execTime * MEGA);
      float writeRateMbSec = (float)total_write_size * 1000 / (execTime * MEGA);
      LOG.info("Read IO rate = " + readRateMbSec);
      LOG.info("Write IO rate = " + writeRateMbSec);
      Map<String, String> stat = super.collectStats(conf, threads, execTime);
      stat.put("readrate", String.valueOf(readRateMbSec));
      stat.put("writerate", String.valueOf(writeRateMbSec));
      return stat;
    }

    @Override
    public void reset() {
      average_read_rate = 0;
      average_write_rate = 0;
      total_num = 0;
    }

    @Override
    public void analyze(Map<String, String> stat) throws IOException {
      average_read_rate += Float.parseFloat(stat.get("readrate"));
      average_write_rate += Float.parseFloat(stat.get("writerate"));
      total_num++;
    }
    
    @Override
    public void output(FSDataOutputStream out) throws IOException {
      out.writeChars("Average Read (MB/sec): \t\t\t" +
        average_read_rate/total_num + "\n");
      out.writeChars("Average Write (MB/sec): \t\t" +
        average_write_rate/total_num + "\n");
    }
  }
