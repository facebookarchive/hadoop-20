package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;

import com.sun.el.parser.ParseException;

public class SleepJobRunner {

  private static final Log LOG = LogFactory.getLog(SleepJobRunner.class);

  public static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("SleepJobRunner [options] numberOfJobs " +
        "percentOfSmallJobs percentOfShortJobs", options );
    System.out.println("");
    System.out.println("numberOfJobs\tthe number of jobs to launch");
    System.out.println("percentOfSmallJobs\tpercentage of jobs to be small: "
        + "100 mappers, 1 reducer (default). The long jobs are the rest "
        + "with 5000 maps and 397 reducers (default)");
    System.out.println("percentOfShortJobs\tpercentage of jobs to be short: "
        + "1 ms of wait time. The long jobas are the rest with "
        + "60 seconds of wait time in the mapper");
    System.out.println("poolCount\tnumber of pools to spread the jobs over: "
        + "default of 15");
  }

  /**
   * Helper class used to help return a set of values for calcStats()
   */
  private static class Stats {
    public double mean;
    public double variance;
    public double stdDev;

    public Stats(double mean, double variance, double stdDev) {
      this.mean = mean;
      this.variance = variance;
      this.stdDev = stdDev;
    }
  }

  /**
   * Calculates mean, variance, standard deviation for a set of numbers
   * @param nums the list of numbers to compute stats on
   * @return aforementioned values in a Stats helper class
   */
  private static Stats calcStats(List<Double> nums) {
    double sum = 0.0, mean = 0.0, variance = 0.0, stdDev = 0.0;
    for (Double d : nums) {
      sum += d.doubleValue();
    }
    if (nums.size() > 0) {
      mean = sum / nums.size();
    }

    sum = 0.0;
    for (Double d : nums) {
      sum += (d.doubleValue() - mean) * (d.doubleValue() - mean);
    }
    if (nums.size() > 0) {
      variance = sum / nums.size();
    }

    stdDev = Math.sqrt(variance);
    return new Stats(mean, variance, stdDev);
  }

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    // Parse the options
    int largeJobMappers = 5000, largeJobReducers = 397, poolCount = 15;
    int smallJobMappers = 10, smallJobReducers = 1;

    Option help = new Option( "help", "print this message" );
    Option largeJobMappersOption = OptionBuilder.withArgName("size").hasArg()
        .withDescription("number of mappers for large jobs" )
        .create("largeJobMappers");
    Option largeJobReducersOption = OptionBuilder.withArgName("size").hasArg()
        .withDescription("number of reducers for large jobs")
        .create("largeJobReducers");
    Option poolCountOption = OptionBuilder.withArgName("size").hasArg()
        .withDescription("number of pools to spread the load over")
        .create("poolCount");

    Options options = new Options();
    options.addOption(help);
    options.addOption(largeJobMappersOption);
    options.addOption(largeJobReducersOption);
    options.addOption(poolCountOption);

    CommandLineParser parser = new GnuParser();
    CommandLine line = null;
    line = parser.parse(options, args);

    if (line.hasOption( "help" ) ) {
      printHelp(options);
      return;
    }

    if (line.hasOption("largeJobMappers")) {
      largeJobMappers = Integer.parseInt(
          line.getOptionValue("largeJobMappers"));
    }

    if (line.hasOption("largeJobReducers")) {
      largeJobReducers = Integer.parseInt(
          line.getOptionValue("largeJobReducers"));
    }

    if (line.hasOption("poolCount")) {
      poolCount = Integer.parseInt(
          line.getOptionValue("poolCount"));
    }

    String[] pools = new String[poolCount];
    for (int i = 0; i < pools.length; i++) {
      pools[i] = "pool" + i;
    }
    if (line.getArgs().length != 3) {
      printHelp(options);
      return;
    }
    int jobs = Integer.valueOf(line.getArgs()[0]);
    int percentageSmall = Integer.valueOf(line.getArgs()[1]);
    int percentageShort = Integer.valueOf(line.getArgs()[2]);

    List<SleepJobRunnerThread> threads = new ArrayList<SleepJobRunnerThread>();
    Random rand = new Random();

    for (int i = 0; i < jobs; i++) {
      Configuration conf = new Configuration();
      conf.set("mapred.child.java.opts",
              "-Xmx50m -Djava.net.preferIPv4Stack=true "
              + "-XX:+UseCompressedOops");
      conf.set("io.sort.mb", "5");
      conf.set("mapred.fairscheduler.pool", pools[i % pools.length]);

      int nMappers, nReducers, sleepTime;
      if (rand.nextInt(100) + 1 <= percentageSmall) {
        nMappers = smallJobMappers;
        nReducers = smallJobReducers;
      } else {
        nMappers = largeJobMappers;
        nReducers = largeJobReducers;
      }

      if (rand.nextInt(100) + 1 <= percentageShort) {
        sleepTime = 1;
      } else {
        sleepTime = 60000;
      }

      SleepJob sleepJob = new SleepJob();
      sleepJob.setConf(conf);
      SleepJobRunnerThread t =
              new SleepJobRunnerThread(conf, nMappers, nReducers, sleepTime);
      threads.add(t);
    }

    long startTime = System.currentTimeMillis();

    for (SleepJobRunnerThread t : threads) {
      t.start();
    }

    for (SleepJobRunnerThread t : threads) {
      t.join();
    }

    long endTime = System.currentTimeMillis();

    // Compute stats
    List<Double> smallJobRuntimes = new ArrayList<Double>();
    List<Double> largeJobRuntimes = new ArrayList<Double>();

    for (SleepJobRunnerThread t : threads) {
      if (t.getNumMappers() == largeJobMappers &&
          t.getNumReducers() == largeJobReducers) {
        largeJobRuntimes.add(Double.valueOf(t.getRuntime()/1000.0));
      } else if (t.getNumMappers() == smallJobMappers &&
          t.getNumReducers() == smallJobReducers) {
        smallJobRuntimes.add(Double.valueOf(t.getRuntime()/1000.0));
      } else {
        throw new RuntimeException("Invalid mapper/reducer counts: " +
            t.getNumMappers() + ", " + t.getNumReducers());
      }
    }

    List<Double> allJobRuntimes = new ArrayList<Double>();
    allJobRuntimes.addAll(smallJobRuntimes);
    allJobRuntimes.addAll(largeJobRuntimes);

    Stats allStats = calcStats(allJobRuntimes);
    Stats largeStats = calcStats(largeJobRuntimes);
    Stats smallStats = calcStats(smallJobRuntimes);
    LOG.info(String.format("All jobs   - mean: %.1f s std dev: %.1f s\n",
        allStats.mean, allStats.stdDev));
    LOG.info(String.format("Large jobs - mean: %.1f s std dev: %.1f s\n",
        largeStats.mean, largeStats.stdDev));
    LOG.info(String.format("Small jobs - mean: %.1f s std dev: %.1f s\n",
        smallStats.mean, smallStats.stdDev));
    LOG.info(String.format("Total time - %.1f\n",
        (endTime - startTime)/1000.0));
  }

  public static class SleepJobRunnerThread extends Thread {

    SleepJob jobToRun = null;
    int nMappers = 0;
    int nReducers = 0;
    int sleepTime = 0;
    long startTime = 0;
    long endTime = 0;

    public SleepJobRunnerThread(Configuration conf, int nMappers,
            int nReducers, int sleepTime) {
      super();
      jobToRun = new SleepJob();
      jobToRun.setConf(conf);
      this.nMappers = nMappers;
      this.nReducers = nReducers;
      this.sleepTime = sleepTime;
    }

    @Override
    public void run() {
      try {
        this.startTime = System.currentTimeMillis();
        jobToRun.run(nMappers, nReducers, sleepTime, 10, sleepTime, 10, false,
                new ArrayList<String>(), new ArrayList<String>(), 10, 10,
                new ArrayList<String>(), 0, false, 0);
      } catch (Exception ex) {
        ex.printStackTrace();
      } finally {
        this.endTime = System.currentTimeMillis();
      }
    }

    /**
     * Returns the time it took to run this job in miliseconds
     */
    public long getRuntime() {
      if (endTime == 0) {
        throw new RuntimeException("Can't get runtime - job didn't finish");
      }
      return endTime - startTime;
    }

    public int getNumMappers() {
      return nMappers;
    }

    public int getNumReducers() {
      return nReducers;
    }
  }


}

