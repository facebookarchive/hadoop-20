package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;

public class SleepJobRunner {

  public static void printUsage() {
    System.out.println("Usage: SleepJobRunner numberOfJobs percentOfSmallJobs "
            + "percentOfShortJobs");
    System.out.println("numberOfJobs\tthe number of jobs to launch");
    System.out.println("percentOfSmallJobs\tpercentage of jobs to be small: "
            + "100 mappers, 1 reducer. The long jobs are the rest with "
            + "5000 maps and 397 reducers");
    System.out.println("percentOfShortJobs\tpercentage of jobs to be short: "
            + "1 ms of wait time. The long jobas are the rest with "
            + "60 seconds of wait time in the mapper");
  }

  public static void main(String[] args) throws Exception {
    String[] pools = new String[15];
    for (int i = 0; i < pools.length; i++) {
      pools[i] = "pool" + i;
    }
    if (args.length != 3) {
      printUsage();
    }
    int jobs = Integer.valueOf(args[0]);
    int percentageSmall = Integer.valueOf(args[1]);
    int percentageShort = Integer.valueOf(args[2]);

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
        nMappers = 10;
        nReducers = 1;
      } else {
        nMappers = 5000;
        nReducers = 397;
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

    for (SleepJobRunnerThread t : threads) {
      t.start();
    }

    for (SleepJobRunnerThread t : threads) {
      t.join();
    }
  }

  public static class SleepJobRunnerThread extends Thread {

    SleepJob jobToRun = null;
    int nMappers = 0;
    int nReducers = 0;
    int sleepTime = 0;

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
        jobToRun.run(nMappers, nReducers, sleepTime, 10, sleepTime, 10, false,
                new ArrayList<String>(), new ArrayList<String>(), 10, 10,
                new ArrayList<String>(), 1);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}

