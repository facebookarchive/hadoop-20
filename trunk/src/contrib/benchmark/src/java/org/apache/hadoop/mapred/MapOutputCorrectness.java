package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * This class will test the correctness of whether (key, value) pairs that are
 * sent to reduce tasks are actually received.  It will additionally try to
 * help identify which mapper caused the problem and can simulate failures.
 */
@SuppressWarnings("deprecation")
public class MapOutputCorrectness extends Configured implements Tool,
    Partitioner<LongWritable, LongWritable> {
  /** Numbers of keys per mapper */
  public static final String NUM_KEYS_PER_MAPPER = "correct.numKeysPerMapper";
  /** Default number of keys per mapper */
  public static final int DEFAULT_NUM_KEYS_PER_MAPPER = 1000;
  /** Numbers of values per key */
  public static final String NUM_VALUES_PER_KEY = "correct.numValuesPerKey";
  /** Default number of values per key */
  public static final int DEFAULT_NUM_VALUES_PER_KEY = 1000;
  /** Maximum key space */
  public static final String MAX_KEY_SPACE = "correct.maxKeySpace";
  /** Default maximum key space */
  public static final int DEFAULT_MAX_KEY_SPACE = 100000;
  /** Starting seed */
  public static final String SEED = "correct.seed";
  /** Default starting seed */
  public static int DEFAULT_SEED = 1;
  /** Chance of failure per key-value collected [0.0,1.0] */
  public static final String CHANCE_FAILURE =
      "correct.chanceFailure";
  /** Default starting seed */
  public static float DEFAULT_CHANCE_FAILURE = 0.0f;
  /** Maximum number of map tasks */
  public static int MAX_MAP_TASKS = 100000;
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(MapOutputCorrectness.class);

  /**
   * Empty split that does nothing.
   */
  public static class MapOutputCorrectnessSplit implements InputSplit {
    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public long getLength() throws IOException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[]{};
    }
  }

  /**
   * Create the number of splits as desired map tasks.
   */
  public static class MapOutputCorrectnessInputFormat extends Configured
      implements InputFormat<LongWritable, LongWritable> {

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      InputSplit[] splitArray =
          new InputSplit[numSplits];
      for (int i = 0; i < numSplits; ++i) {
        splitArray[i] = new MapOutputCorrectnessSplit();
      }
      return splitArray;
    }

    @Override
    public RecordReader<LongWritable, LongWritable> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new MapOutputCorrectnessRecordReader();
    }
  }

  /**
   * Ensure that the record reader provides exactly one record
   */
  public static class MapOutputCorrectnessRecordReader implements
      RecordReader<LongWritable, LongWritable> {
    private boolean seenFirstRecord = false;

    @Override
    public boolean next(LongWritable key, LongWritable value) throws IOException {
      if (!seenFirstRecord) {
        seenFirstRecord = true;
        return true;
      }
      return false;
    }

    @Override
    public LongWritable createKey() {
      return new LongWritable(0);
    }

    @Override
    public LongWritable createValue() {
      return new LongWritable(0);
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  @Override
  public final int run(final String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("m",
        "maps",
        true,
        "Number of map tasks (must be less than " + MAX_MAP_TASKS + ")");
    options.addOption("M",
        "randomMaps",
        true,
        "Upper bound of random map tasks (must be less than " +
        MAX_MAP_TASKS + ")");
    options.addOption("r",
        "reduces",
        true,
        "Number of reduce tasks");
    options.addOption("R",
        "randomReduces",
        true,
        "Upper bound of random reduce tasks");
    options.addOption("k",
        "keys",
        true,
        "Number of keys per mapper");
    options.addOption("K",
        "keys",
        true,
        "Upper bound of random keys per mapper");
    options.addOption("v",
        "values",
        true,
        "Number of values per mapper");
    options.addOption("V",
        "values",
        true,
        "Upper bound of random values per mapper");
    options.addOption("s",
        "seed",
        true,
        "Starting seed for random values");
    options.addOption("f",
        "failure",
        true,
        "Failure chance per emitted key-value [0.0,1.0]");
    options.addOption("F",
        "randomFailure",
        true,
        "Upper bound of random failure chance per emitted key-value [0.0,1.0]");
    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    if (cmd.hasOption('h')) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }

    Random random = new Random();
    int mapTasks = 0;
    if (cmd.hasOption('m')) {
      mapTasks = Integer.parseInt(cmd.getOptionValue('m'));
    } else if (cmd.hasOption('M')) {
      mapTasks = Math.max(1,
          random.nextInt(Integer.parseInt(cmd.getOptionValue('M'))));
    } else {
        LOG.info("Need to choose the number of map tasks (-m) or " +
        		     "random map tasks (-M");
        return -1;
    }
    int reduceTasks = 0;
    if (cmd.hasOption('r')) {
      reduceTasks = Integer.parseInt(cmd.getOptionValue('r'));
    } else if (cmd.hasOption('R')) {
      reduceTasks = Math.max(1,
          random.nextInt(Integer.parseInt(cmd.getOptionValue('R'))));
    } else {
        LOG.info("Need to choose the number of reduce tasks (-r) or " +
                 "random reduce tasks (-R");
        return -1;
    }

    int numKeysPerMapper = DEFAULT_NUM_KEYS_PER_MAPPER;
    if (cmd.hasOption('k')) {
      numKeysPerMapper = Integer.parseInt(cmd.getOptionValue('k'));
    } else if (cmd.hasOption('K')) {
      numKeysPerMapper =
          random.nextInt(Integer.parseInt(cmd.getOptionValue('K')));
    }
    int numValuesPerKey = DEFAULT_NUM_VALUES_PER_KEY;
    if (cmd.hasOption('v')) {
      numValuesPerKey = Integer.parseInt(cmd.getOptionValue('v'));
    } else if (cmd.hasOption('V')) {
      numValuesPerKey =
          random.nextInt(Integer.parseInt(cmd.getOptionValue('V')));
    }
    float chanceFailure = DEFAULT_CHANCE_FAILURE;
    if (cmd.hasOption('f')) {
      chanceFailure = Float.parseFloat(cmd.getOptionValue('f'));
    } else if (cmd.hasOption('F')) {
      chanceFailure =
          random.nextFloat() * (Float.parseFloat(cmd.getOptionValue('F')));
    }
    int seed = DEFAULT_SEED;
    if (cmd.hasOption('s')) {
      seed = Integer.parseInt(cmd.getOptionValue('s'));
    }

    LOG.info(new Date() + " : Running with the following arguments: " +
             Arrays.toString(args));

    // Any adjustments
    if (mapTasks > MAX_MAP_TASKS) {
      LOG.warn("Too many map tasks specified, revising down to " +
               MAX_MAP_TASKS);
      mapTasks = MAX_MAP_TASKS;
    }

    // What are the actual values and how to replicate this case
    LOG.info("Using " + mapTasks + " map tasks");
    LOG.info("Using " + reduceTasks + " reduce tasks");
    LOG.info("Using " + numKeysPerMapper + " keys per mapper");
    LOG.info("Using " + numValuesPerKey + " values per key");
    LOG.info("Using " + chanceFailure + " as the chance of failure");
    LOG.info("Using " + seed + " as the starting seed");
    LOG.info("Replicate with the following arguments: -m " + mapTasks +
        " -r " + reduceTasks + " -k " + numKeysPerMapper +
        " -v " + numValuesPerKey + " -f " + chanceFailure + " -s " + seed);

    JobConf job = new JobConf(getConf(), MapOutputCorrectness.class);
    job.setNumMapTasks(mapTasks);
    job.setNumReduceTasks(reduceTasks);
    job.setMapperClass(MapOutputCorrectness.MapOutputCorrectnessMapper.class);
    job.setReducerClass(MapOutputCorrectness.MapOutputCorrectnessReducer.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setInputFormat(MapOutputCorrectnessInputFormat.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setPartitionerClass(MapOutputCorrectness.class);
    job.setJobName(this.getClass().getName());
    job.setInt(NUM_KEYS_PER_MAPPER, numKeysPerMapper);
    job.setInt(NUM_VALUES_PER_KEY, numValuesPerKey);
    job.setInt(SEED, seed);
    job.setFloat(CHANCE_FAILURE, chanceFailure);

    RunningJob runningJob = JobClient.runJob(job);
    runningJob.waitForCompletion();
    return runningJob.isSuccessful() ? 0 : -1;
  }

  @Override
  public int getPartition(LongWritable key, LongWritable value,
      int numPartitions) {
    return getPartitionStatic(key, value, numPartitions);
  }

  /**
   * Get a partition from the key only
   * @param key Key to partition
   * @param value Value (not used)
   * @param numPartitions Number of partitions
   * @return Destination partition
   */
  public static int getPartitionStatic(LongWritable key, LongWritable value,
      int numPartitions) {
    return (int) (Math.abs(key.get()) % numPartitions);
  }

  /**
   * Is this key a special sum key (for reducers)
   * @param key Key to check
   * @param numReducers Number of reducers
   * @param maxKeySpace Max key space
   * @return True if a special sum key, false otherwise
   */
  private static boolean isSumKey(long key, int numReducers, int maxKeySpace) {
    return key >= getFirstSumKey(numReducers, maxKeySpace);
  }

  /**
   * Which mapper sent this key sum?
   * @param key Key to check
   * @param numReducers Number of reducers
   * @param maxKeySpace Max key space
   * @return Mapper that send this key sum
   */
  private static int getMapperId(long key, int numReducers, int maxKeySpace) {
    key = key - getFirstSumKey(numReducers, maxKeySpace);
    return (int) (key / numReducers);
  }

  /**
   * Get the first sum key (all other random keys are less than this one)
   * @param numReducers Number of reducers
   * @param maxKeySpace Max key space
   * @return Value of the first sum key
   */
  private static long getFirstSumKey(
      int numReducers, int maxKeySpace) {
    return (maxKeySpace / numReducers) * (numReducers + 1);
  }

  /**
   * Get the first sum key for a given mapper
   * @param taskPartition Task id of the mapper
   * @param numReducers Number of reducers
   * @param maxKeySpace Max key space
   * @return
   */
  private static long getFirstReducerSumKey(
      int taskPartition, int numReducers, int maxKeySpace) {
    return getFirstSumKey(numReducers, maxKeySpace) +
        (taskPartition * numReducers);
  }

  /**
   * Should fail?
   * @param chanceFailure Chances of failure [0.0,1.0]
   * @param random Pseudo-random variable
   */
  private static void possiblyFail(float chanceFailure, Random random) {
    float value = random.nextFloat();
    if (value < chanceFailure) {
      LOG.fatal("shouldFail: Failing with value " + value +
          " < " + chanceFailure);
      System.exit(-1);
    }
  }

  /**
   * Get the attempt number
   * @param conf Configuration
   * @return Attempt number
   * @throws IllegalArgumentException
   */
  public static int getAttemptId(Configuration conf)
      throws IllegalArgumentException {
    if (conf == null) {
      throw new NullPointerException("Conf is null");
    }

    String taskId = conf.get("mapred.task.id");
    if (taskId == null) {
      throw new IllegalArgumentException(
          "Configuration does not contain the property mapred.task.id");
    }

    String[] parts = taskId.split("_");
    if (parts.length != 6 ||
        !parts[0].equals("attempt") ||
        (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
      throw new IllegalArgumentException(
          "TaskAttemptId string : " + taskId + " is not properly formed");
    }

    return Integer.parseInt(parts[5]);
  }

  /**
   * Send a pseudo-random number of keys and values.
   */
  static class MapOutputCorrectnessMapper implements
  Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
    /** Job conf */
    private JobConf conf;
    /** Sum of values sent to each of the reducers */
    private List<LongWritable> reducerSumList = new ArrayList<LongWritable>();
    /** My partition id */
    private int taskPartition;
    /** Pseudo-random variable */
    private Random random;
    /** Number of keys per mapper */
    private int numKeysPerMapper;
    /** Number of values per key */
    private int numValuesPerKey;
    /** Number of reducers */
    private int numReducers;
    /** All keys will be smaller than this */
    private int maxKeySpace;
    /** Chance of failure */
    private float chanceFailure;
    /** How many values were sent */
    private long valueCount = 0;

    @Override
    public void map(LongWritable key, LongWritable value,
        OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
            throws IOException {
      // Ignore the key and value
      // Instead deterministically send based on the map partition id
      for (int i = 0; i < numKeysPerMapper; ++i) {
        for (int j = 0; j < numValuesPerKey; ++j) {
          LongWritable randomKey =
              new LongWritable(random.nextInt() % maxKeySpace);
          LongWritable uniqueValue =
              new LongWritable((valueCount * MAX_MAP_TASKS) + taskPartition);
          ++valueCount;
          output.collect(randomKey, uniqueValue);
          int reducerSumIndex =
              getPartitionStatic(randomKey, null, numReducers);
          LongWritable reducerSumValue = reducerSumList.get(reducerSumIndex);
          reducerSumValue.set(reducerSumValue.get() + uniqueValue.get());
          if (getAttemptId(conf) == 0) {
            possiblyFail(chanceFailure, random);
          }
        }
      }

      // Send each reducer a summation of the values sent to it.
      long firstReducerSumKey =
          getFirstReducerSumKey(taskPartition, numReducers, maxKeySpace);
      for (int i = 0; i < numReducers; ++i) {
        output.collect(new LongWritable(firstReducerSumKey + i),
            reducerSumList.get(i));
        LOG.info("Sent reducer " + i + " a sum of " + reducerSumList.get(i));
      }
    }

    @Override
    public void configure(JobConf job) {
      this.conf = job;
      taskPartition = conf.getInt("mapred.task.partition", -1);
      int startingSeed = conf.getInt(SEED, -1) + taskPartition;
      random = new Random(startingSeed);
      LOG.info("Starting with seed " + startingSeed +
          " on partition " + taskPartition);
      numKeysPerMapper = conf.getInt(NUM_KEYS_PER_MAPPER, -1);
      numValuesPerKey = conf.getInt(NUM_VALUES_PER_KEY, -1);
      numReducers = conf.getInt("mapred.reduce.tasks", -1);
      maxKeySpace = conf.getInt(MAX_KEY_SPACE, DEFAULT_MAX_KEY_SPACE);
      chanceFailure = conf.getFloat(CHANCE_FAILURE, 0.0f);
      if (numKeysPerMapper == -1 || numValuesPerKey == -1 || numReducers == -1
          || maxKeySpace == -1) {
        throw new IllegalArgumentException(
            "Illegal values " + numKeysPerMapper + " " + numValuesPerKey +
            " " + numReducers + " " + maxKeySpace);
      }
      for (int i = 0; i < numReducers; ++i) {
        reducerSumList.add(new LongWritable(0));
      }
    }

    @Override
    public void close() throws IOException {
    }
  }

  /**
   * Collect the key-value pairs from the mappers and check that the sums
   * match what is expected.
   */
  static class MapOutputCorrectnessReducer
      implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    /** Job conf */
    private JobConf conf;
    /** My partition id */
    private int taskPartition;
    /** Pseudo-random variable */
    private Random random;
    /** Number of keys per mapper */
    private int numKeysPerMapper;
    /** Number of values per key */
    private int numValuesPerKey;
    /** Number of mappers */
    private int numMappers;
    /** Number of reducers */
    private int numReducers;
    /** All keys will be smaller than this */
    private int maxKeySpace;
    /** Chance of failure */
    private float chanceFailure;
    /** Sum of the values from each mapper */
    private List<LongWritable> mapperSumList =
        new ArrayList<LongWritable>();
    /** Expected sum of the values from each mapper */
    private List<LongWritable> expectedMapperSumList =
        new ArrayList<LongWritable>();

    @Override
    public void reduce(LongWritable key, Iterator<LongWritable> values,
        OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
            throws IOException {
      // Deterministically ensure that all mapoutput has been received from all
      // mappers
      if (isSumKey(key.get(), numReducers, maxKeySpace)) {
        int mapTask = getMapperId(key.get(), numReducers, maxKeySpace);
        LongWritable sumLimit = expectedMapperSumList.get(mapTask);
        if (sumLimit.get() != -1) {
          throw new IllegalStateException(
              "Impossible that the reducer gets two sum keys from mapper " +
                  mapTask);
        }
        LongWritable sum = values.next();
        if (values.hasNext() != false) {
          throw new IllegalStateException(
              "Impossible that there is more than one sum for mapper " +
                  mapTask);
        }
        sumLimit.set(sum.get());
      } else {
        while (values.hasNext()) {
          LongWritable value = values.next();
          LongWritable sum =
              mapperSumList.get((int) (value.get() % MAX_MAP_TASKS));
          sum.set(sum.get() + value.get());
          if (getAttemptId(conf) == 0) {
            possiblyFail(chanceFailure, random);
          }
        }
      }
    }

    @Override
    public void configure(JobConf job) {
      this.conf = job;
      taskPartition = conf.getInt("mapred.task.partition", -1);
      int startingSeed = conf.getInt(SEED, -1) + taskPartition;
      random = new Random(startingSeed);
      LOG.info("Starting with seed " + startingSeed +
          " on partition " + taskPartition);
      numKeysPerMapper = conf.getInt(NUM_KEYS_PER_MAPPER, -1);
      numValuesPerKey = conf.getInt(NUM_VALUES_PER_KEY, -1);
      numMappers = conf.getNumMapTasks();
      numReducers = conf.getInt("mapred.reduce.tasks", -1);
      maxKeySpace = conf.getInt(MAX_KEY_SPACE, DEFAULT_MAX_KEY_SPACE);
      chanceFailure = conf.getFloat(CHANCE_FAILURE, 0.0f);
      if (numKeysPerMapper == -1 || numValuesPerKey == -1 || numReducers == -1
          || maxKeySpace == -1) {
        throw new IllegalArgumentException(
            "Illegal values " + numKeysPerMapper + " " + numValuesPerKey +
            " " + numReducers + " " + maxKeySpace);
      }
      for (int i = 0; i < numMappers; ++i) {
        mapperSumList.add(new LongWritable(0));
        expectedMapperSumList.add(new LongWritable(-1));
      }
    }

    @Override
    public void close() throws IOException {
      // Check the sums
      boolean success = true;
      int failedMappers = 0;
      for (int i = 0; i < numMappers; ++i) {
        if (expectedMapperSumList.get(i).get() == -1) {
          success = false;
          ++failedMappers;
          LOG.error(
              "Mapper " + i + ": Never received the expected sum " +
              " and got the sum " + mapperSumList.get(i));
        } else if (!mapperSumList.get(i).equals(expectedMapperSumList.get(i))) {
          success = false;
          ++failedMappers;
          LOG.error(
              "Mapper " + i + ": Incorrect values received, expected " +
              expectedMapperSumList.get(i) + ", but got " +
                  mapperSumList.get(i));
        } else {
          LOG.info("Mapper " + i + ": Received the expected sum of " +
              expectedMapperSumList.get(i));
        }
      }
      LOG.info("Summary: " + (numMappers - failedMappers) + " of " +
               numMappers + " maps successfully received");
      if (!success) {
        throw new IllegalStateException(
            failedMappers + " failed mapper outputs");
      }
    }
  }

  public static void main(String[] args) throws Exception{
    System.exit(
        ToolRunner.run(new Configuration(), new MapOutputCorrectness(), args));
  }

  @Override
  public void configure(JobConf job) {
  }
}
