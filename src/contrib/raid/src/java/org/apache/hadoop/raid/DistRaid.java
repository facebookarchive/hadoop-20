package org.apache.hadoop.raid;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.raid.RaidNode.Statistics;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.StringUtils;

public class DistRaid {

  protected static final Log LOG = LogFactory.getLog(DistRaid.class);

  static final String NAME = "distRaid";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String OP_LIST_LABEL = NAME + ".op.list";
  static final String OP_COUNT_LABEL = NAME + ".op.count";
  static final String SCHEDULER_OPTION_LABEL = NAME + ".scheduleroption";
  static final String IGNORE_FAILURES_OPTION_LABEL = NAME + ".ignore.failures";
  static final int   OP_LIST_BLOCK_SIZE = 32 * 1024 * 1024; // block size of control file
  static final short OP_LIST_REPLICATION = 10; // replication factor of control file

  private static final long DEFAULT_OP_PER_MAP = 50;
  public static final String OP_PER_MAP_KEY = "hdfs.raid.op.per.map";
  private static final int DEFAULT_MAX_MAPS_PER_NODE = 20;
  public static final String MAX_MAPS_PER_NODE_KEY = "hdfs.raid.max.maps.per.node";
  public static final int DEFAULT_MAX_FAILURE_RETRY = 3;
  public static final String MAX_FAILURE_RETRY_KEY = "hdfs.raid.max.failure.retry";
  public static final String SLEEP_TIME_BETWEEN_RETRY_KEY =
      "hdfs.raid.sleep.time.between.retry";
  public static final long DEFAULT_SLEEP_TIME_BETWEEN_RETRY = 1000L;
  private static long opPerMap = DEFAULT_OP_PER_MAP;
  private static int maxMapsPerNode = DEFAULT_MAX_MAPS_PER_NODE;
  
  private static final int SYNC_FILE_MAX = 10;
  private static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  private static String jobName = NAME;

  public static enum Counter {
    FILES_SUCCEEDED, FILES_FAILED, PROCESSED_BLOCKS, PROCESSED_SIZE, META_BLOCKS, META_SIZE,
    SAVING_SIZE
  }

  protected JobConf jobconf;

  /** {@inheritDoc} */
  public void setConf(Configuration conf) {
    if (jobconf != conf) {
      jobconf = conf instanceof JobConf ? (JobConf) conf : new JobConf(conf);
    }
  }

  /** {@inheritDoc} */
  public JobConf getConf() {
    return jobconf;
  }

  public DistRaid(Configuration conf) {
    setConf(createJobConf(conf));
    opPerMap = conf.getLong(OP_PER_MAP_KEY, DEFAULT_OP_PER_MAP);
    maxMapsPerNode = conf.getInt(MAX_MAPS_PER_NODE_KEY,
        DEFAULT_MAX_MAPS_PER_NODE);
  }

  private static final Random RANDOM = new Random();

  protected static String getRandomId() {
    return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
  }
  
  /*
   * This class denotes the unit of encoding for a mapper task
   * The key in the map function is formated as:
   * "startStripeId encodingId encodingUnit path"
   * Given a file path /user/dhruba/1 with 13 blocks
   * Suppose codec's stripe length is 3 and encoding unit is 2
   * RaidNode.splitPaths will split the raiding task into 3 tasks
   * "0 1 2 /user/dhruba/1": will raid the stripe 0 and 1 (blocks 0-5)
   * "2 2 2 /user/dhruba/1": will raid the stripe 2 and 3 (blocks 6-11)
   * "4 3 2 /user/dhruba/1": will raid the stripe 4 (blocks 12-13)
   * encodingId is unique for each task and it's used to construct a temporary
   * directory to store the partial parity file
   * modificationTime is the modification time of candidate when job is submitted
   * it's used for checking if candidate changes after the job submit.  
   */
  
  public static class EncodingCandidate {
    public final static int DEFAULT_GET_SRC_STAT_RETRY = 5;
    public FileStatus srcStat;
    public long startStripe = 0;
    public long encodingUnit = 0;
    public String encodingId = null;
    final static public String delim = " ";
    public long modificationTime = 0L;
    public boolean isEncoded = false;
    public boolean isRenamed = false;
    public boolean isConcated = false;
    public List<List<Block>> srcStripes = null;
    
    EncodingCandidate(FileStatus newStat, long newStartStripe,
        String newEncodingId, long newEncodingUnit, long newModificationTime) {
      this.srcStat = newStat;
      this.startStripe = newStartStripe;
      this.encodingId = newEncodingId;
      this.encodingUnit = newEncodingUnit;
      this.modificationTime = newModificationTime;
    }
    
    public String toString() {
      return startStripe + delim + encodingId + delim + encodingUnit 
          + delim + modificationTime + delim + this.srcStat.getPath().toString();
    }
    
    public static EncodingCandidate getEncodingCandidate(String key,
        Configuration jobconf) throws IOException {
      String[] keys = key.split(delim, 5);
      Path p = new Path(keys[4]);
      long startStripe = Long.parseLong(keys[0]);
      long modificationTime = Long.parseLong(keys[3]);
      FileStatus srcStat = getSrcStatus(jobconf, p);
      long encodingUnit = Long.parseLong(keys[2]);
      return new EncodingCandidate(srcStat, startStripe, keys[1],
          encodingUnit, modificationTime);
    }
    
    public static FileStatus getSrcStatus(Configuration jobconf, Path p)
        throws IOException {
      for (int i = 0; i < DEFAULT_GET_SRC_STAT_RETRY; i++) {
        try {
          return p.getFileSystem(jobconf).getFileStatus(p); 
        } catch (FileNotFoundException fnfe) {
          return null;
        } catch (IOException ioe) {
          LOG.warn("Get exception ", ioe);
          if (i == DEFAULT_GET_SRC_STAT_RETRY - 1) {
            throw ioe;
          }
          try {
            Thread.sleep(3000);
          } catch (InterruptedException ignore) {
          }
        }
      }
      throw new IOException("couldn't getFileStatus " + p);
    }
    
    public void refreshFile(Configuration jobconf) throws IOException {
      srcStat = getSrcStatus(jobconf, srcStat.getPath());
    }
  }

  /**
   * 
   * helper class which holds the policy and paths
   * 
   */
  public static class RaidPolicyPathPair {
    public PolicyInfo policy;
    public List<EncodingCandidate> srcPaths;

    RaidPolicyPathPair(PolicyInfo policy, List<EncodingCandidate> srcPaths) {
      this.policy = policy;
      this.srcPaths = srcPaths;
    }
  }

  List<RaidPolicyPathPair> raidPolicyPathPairList = new ArrayList<RaidPolicyPathPair>();

  private JobClient jobClient;
  private RunningJob runningJob;
  private int jobEventCounter = 0;
  private String lastReport = null;
  private long startTime = System.currentTimeMillis();

  private long totalSaving;

  /** Responsible for generating splits of the src file list. */
  static class DistRaidInputFormat implements InputFormat<Text, PolicyInfo> {
    /** Do nothing. */
    public void validateInput(JobConf job) {
    }

    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     * 
     * @param job
     *          The handle to the JobConf object
     * @param numSplits
     *          Number of splits requested
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      final int srcCount = job.getInt(OP_COUNT_LABEL, -1);
      final int targetcount = srcCount / numSplits;
      String srclist = job.get(OP_LIST_LABEL, "");
      if (srcCount < 0 || "".equals(srclist)) {
        throw new RuntimeException("Invalid metadata: #files(" + srcCount
            + ") listuri(" + srclist + ")");
      }
      Path srcs = new Path(srclist);
      FileSystem fs = srcs.getFileSystem(job);

      List<FileSplit> splits = new ArrayList<FileSplit>(numSplits);

      Text key = new Text();
      PolicyInfo value = new PolicyInfo();
      SequenceFile.Reader in = null;
      long prev = 0L;
      int count = 0; // count src
      try {
        for (in = new SequenceFile.Reader(fs, srcs, job); in.next(key, value);) {
          long curr = in.getPosition();
          long delta = curr - prev;
          if (++count > targetcount) {
            count = 0;
            splits.add(new FileSplit(srcs, prev, delta, (String[]) null));
            prev = curr;
          }
        }
      } finally {
        in.close();
      }
      long remaining = fs.getFileStatus(srcs).getLen() - prev;
      if (remaining != 0) {
        splits.add(new FileSplit(srcs, prev, remaining, (String[]) null));
      }
      LOG.info("jobname= " + jobName + " numSplits=" + numSplits + 
               ", splits.size()=" + splits.size());
      return splits.toArray(new FileSplit[splits.size()]);
    }

    /** {@inheritDoc} */
    public RecordReader<Text, PolicyInfo> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, PolicyInfo>(job,
          (FileSplit) split);
    }
  }

  /** The mapper for raiding files. */
  static class DistRaidMapper implements
      Mapper<Text, PolicyInfo, WritableComparable, Text> {
    private JobConf jobconf;
    private boolean ignoreFailures;
    private int retryNum;

    private int failcount = 0;
    private int succeedcount = 0;
    private Statistics st = null;
    private Reporter reporter = null;

    private String getCountString() {
      return "Succeeded: " + succeedcount + " Failed: " + failcount;
    }

    /** {@inheritDoc} */
    public void configure(JobConf job) {
      this.jobconf = job;
      ignoreFailures = jobconf.getBoolean(IGNORE_FAILURES_OPTION_LABEL, true);
      retryNum = jobconf.getInt(DistRaid.MAX_FAILURE_RETRY_KEY,
          DistRaid.DEFAULT_MAX_FAILURE_RETRY);
      st = new Statistics();
    }
    
    public static boolean doRaid(int retryNum, String key, Configuration jobconf,
        PolicyInfo policy, Statistics st, Reporter reporter)
            throws IOException {
      String s = "FAIL: " + policy + ", " + key + " ";
      long sleepTimeBetwRetry = jobconf.getLong(SLEEP_TIME_BETWEEN_RETRY_KEY,
                                            DEFAULT_SLEEP_TIME_BETWEEN_RETRY);
      EncodingCandidate ec = EncodingCandidate.getEncodingCandidate(key, jobconf);
      for (int i = 0; i < retryNum; i++) {
        LOG.info("The " + i + "th attempt: " + s);
        if (ec.srcStat == null) {
          LOG.info("Raiding Candidate doesn't exist, NO_ACTION");
          return false;
        }
        if (ec.modificationTime != ec.srcStat.getModificationTime()) {
          LOG.info("Raiding Candidate was changed, NO_ACTION");
          return false;
        }
        try {
          return RaidNode.doRaid(jobconf, policy, ec, st, reporter);
        } catch (IOException e) {
          LOG.info(s, e);
          if (i == retryNum - 1) {
            throw new IOException(s, e);
          }
          ec.refreshFile(jobconf);
          try {
            Thread.sleep(sleepTimeBetwRetry);
          } catch (InterruptedException ie) {
            throw new IOException(ie);
          }
        }
      }
      throw new IOException(s);
    }

    /** Run a FileOperation */
    public void map(Text key, PolicyInfo policy,
        OutputCollector<WritableComparable, Text> out, Reporter reporter)
        throws IOException {
      this.reporter = reporter;
      try {
        Codec.initializeCodecs(jobconf);
        LOG.info("Raiding file=" + key.toString() + " policy=" + policy);
        boolean result = doRaid(retryNum, key.toString(), jobconf, policy, st, reporter);
        if (result) {
          ++succeedcount;
          
          reporter.incrCounter(Counter.PROCESSED_BLOCKS, st.numProcessedBlocks);
          reporter.incrCounter(Counter.PROCESSED_SIZE, st.processedSize);
          reporter.incrCounter(Counter.META_BLOCKS, st.numMetaBlocks);
          reporter.incrCounter(Counter.META_SIZE, st.metaSize);
          reporter.incrCounter(Counter.SAVING_SIZE,
              st.processedSize - st.remainingSize - st.metaSize);
          reporter.incrCounter(Counter.FILES_SUCCEEDED, 1);
        }
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FILES_FAILED, 1);
        out.collect(null, new Text(e.getMessage()));
      } finally {
        reporter.setStatus(getCountString());
      }
    }

    /** {@inheritDoc} */
    public void close() throws IOException {
      if (failcount == 0 || ignoreFailures) {
        return;
      }
      throw new IOException(getCountString());
    }
  }

  /**
   * create new job conf based on configuration passed.
   * 
   * @param conf
   * @return
   */
  private static JobConf createJobConf(Configuration conf) {
    JobConf jobconf = new JobConf(conf, DistRaid.class);
    jobName = NAME + " " + dateForm.format(new Date(RaidNode.now()));
    jobconf.setUser(RaidNode.JOBUSER);
    jobconf.setJobName(jobName);
    jobconf.setMapSpeculativeExecution(false);
    RaidUtils.parseAndSetOptions(jobconf, SCHEDULER_OPTION_LABEL);

    jobconf.setJarByClass(DistRaid.class);
    jobconf.setInputFormat(DistRaidInputFormat.class);
    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setMapperClass(DistRaidMapper.class);
    jobconf.setNumReduceTasks(0);
    return jobconf;
  }

  /** Add paths to be raided */
  public void addRaidPaths(PolicyInfo info, List<EncodingCandidate> paths) {
    raidPolicyPathPairList.add(new RaidPolicyPathPair(info, paths));
  }

  /** Calculate how many maps to run. */
  private static int getMapCount(int srcCount) {
    int numMaps = (int) (srcCount / opPerMap);
    return Math.max(numMaps, maxMapsPerNode);
  }

  /** Invokes a map-reduce job do parallel raiding.
   *  @return true if the job was started, false otherwise
   */
  public boolean startDistRaid() throws IOException {
    assert(raidPolicyPathPairList.size() > 0);
    if (setup()) {
      this.jobClient = new JobClient(jobconf);
      this.runningJob = this.jobClient.submitJob(jobconf);
      LOG.info("Job Started: " + runningJob.getID());
      this.startTime = System.currentTimeMillis();
      return true;
    }
    return false;
  }

  /**
   * Get the URL of the current running job
   * @return the tracking URL
   */
  public String getJobTrackingURL() {
    if (runningJob == null)
      return null;
    return runningJob.getTrackingURL();
  }
  
   /** Checks if the map-reduce job has completed.
    *
    * @return true if the job completed, false otherwise.
    * @throws IOException
    */
   public boolean checkComplete() throws IOException {
     JobID jobID = runningJob.getID();
     if (runningJob.isComplete()) {
       // delete job directory
       final String jobdir = jobconf.get(JOB_DIR_LABEL);
       if (jobdir != null) {
         final Path jobpath = new Path(jobdir);
         jobpath.getFileSystem(jobconf).delete(jobpath, true);
       }
       if (runningJob.isSuccessful()) {
         LOG.info("Job Complete(Succeeded): " + jobID);
       } else {
         LOG.info("Job Complete(Failed): " + jobID);
       }
       raidPolicyPathPairList.clear();
       Counters ctrs = runningJob.getCounters();
       if (ctrs != null) {
         RaidNodeMetrics metrics = RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
         if (ctrs.findCounter(Counter.FILES_FAILED) != null) {
           long filesFailed = ctrs.findCounter(Counter.FILES_FAILED).getValue();
           metrics.raidFailures.inc(filesFailed);
         }
         long slotSeconds = ctrs.findCounter(
          JobInProgress.Counter.SLOTS_MILLIS_MAPS).getValue() / 1000;
         metrics.raidSlotSeconds.inc(slotSeconds);
       }
       return true;
     } else {
       String report =  (" job " + jobID +
         " map " + StringUtils.formatPercent(runningJob.mapProgress(), 0)+
         " reduce " + StringUtils.formatPercent(runningJob.reduceProgress(), 0));
       if (!report.equals(lastReport)) {
         LOG.info(report);
         lastReport = report;
       }
       TaskCompletionEvent[] events =
         runningJob.getTaskCompletionEvents(jobEventCounter);
       jobEventCounter += events.length;
       for(TaskCompletionEvent event : events) {
         if (event.getTaskStatus() ==  TaskCompletionEvent.Status.FAILED) {
           LOG.info(" Job " + jobID + " " + event.toString());
         }
       }
       return false;
     }
   }

   public void killJob() throws IOException {
     runningJob.killJob();
   }
   
   public void cleanUp() {
     for (Codec codec: Codec.getCodecs()) {
       Path tmpdir = new Path(codec.tmpParityDirectory, this.getJobID());
       try {
         FileSystem fs = tmpdir.getFileSystem(jobconf);
         if (fs.exists(tmpdir)) {
           fs.delete(tmpdir, true);
         }
       } catch (IOException ioe) {
         LOG.error("Fail to delete " + tmpdir, ioe);
       }
     }
   }

   public boolean successful() throws IOException {
     return runningJob.isSuccessful();
   }

   private void estimateSavings() {
     for (RaidPolicyPathPair p : raidPolicyPathPairList) {
       Codec codec = Codec.getCodec(p.policy.getCodecId());
       int stripeSize = codec.stripeLength;
       int paritySize = codec.parityLength;
       int targetRepl = Integer.parseInt(p.policy.getProperty("targetReplication"));
       int parityRepl = Integer.parseInt(p.policy.getProperty("metaReplication"));
       for (EncodingCandidate st : p.srcPaths) {
         long saving = RaidNode.savingFromRaidingFile(
             st, stripeSize, paritySize, targetRepl, parityRepl);
         totalSaving += saving;
       }
     }
   }

  /**
   * set up input file which has the list of input files.
   * 
   * @return boolean
   * @throws IOException
   */
  private boolean setup() throws IOException {
    estimateSavings();

    final String randomId = getRandomId();
    JobClient jClient = new JobClient(jobconf);
    Path jobdir = new Path(jClient.getSystemDir(), NAME + "_" + randomId);

    LOG.info(JOB_DIR_LABEL + "=" + jobdir);
    jobconf.set(JOB_DIR_LABEL, jobdir.toString());
    Path log = new Path(jobdir, "_logs");

    // The control file should have small size blocks. This helps
    // in spreading out the load from mappers that will be spawned.
    jobconf.setInt("dfs.blocks.size",  OP_LIST_BLOCK_SIZE);

    FileOutputFormat.setOutputPath(jobconf, log);
    LOG.info("log=" + log);

    // create operation list
    FileSystem fs = jobdir.getFileSystem(jobconf);
    Path opList = new Path(jobdir, "_" + OP_LIST_LABEL);
    jobconf.set(OP_LIST_LABEL, opList.toString());
    int opCount = 0, synCount = 0;
    SequenceFile.Writer opWriter = null;

    try {
      opWriter = SequenceFile.createWriter(fs, jobconf, opList, Text.class,
          PolicyInfo.class, SequenceFile.CompressionType.NONE);
      for (RaidPolicyPathPair p : raidPolicyPathPairList) {
        // If a large set of files are Raided for the first time, files
        // in the same directory that tend to have the same size will end up
        // with the same map. This shuffle mixes things up, allowing a better
        // mix of files.
        java.util.Collections.shuffle(p.srcPaths);
        for (EncodingCandidate ec : p.srcPaths) {
          opWriter.append(new Text(ec.toString()), p.policy);
          opCount++;
          if (++synCount > SYNC_FILE_MAX) {
            opWriter.sync();
            synCount = 0;
          }
        }
      }

    } finally {
      if (opWriter != null) {
        opWriter.close();
      }
      fs.setReplication(opList, OP_LIST_REPLICATION); // increase replication for control file
    }
    raidPolicyPathPairList.clear();
    
    jobconf.setInt(OP_COUNT_LABEL, opCount);
    LOG.info("Number of files=" + opCount);
    jobconf.setNumMapTasks(getMapCount(opCount));
    LOG.info("jobName= " + jobName + " numMapTasks=" + jobconf.getNumMapTasks());
    return opCount != 0;

  } 
  
  public long getStartTime() {
    return this.startTime;
  }

  public String toHtmlRow() {
    return JspUtils.tr(
        JspUtils.td(
            JspUtils.link(
                runningJob.getID().toString(), runningJob.getTrackingURL())) +
        JspUtils.td(runningJob.getJobName()) +
        JspUtils.td(StringUtils.humanReadableInt(totalSaving)));
  }
  
  public static String htmlRowHeader() {
    return JspUtils.tr(
        JspUtils.td("Job ID") +
        JspUtils.td("Name") +
        JspUtils.td("Estimated Saving"));
  }
  
  public Counters getCounters() throws IOException{
    return this.runningJob.getCounters();
  }

  public String getJobID() {
    return this.runningJob.getID().toString();
  }
}
