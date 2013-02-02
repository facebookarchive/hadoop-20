/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Used to expire files in cache that hasn't been accessed for a while
 */
public class ExpireUnusedJobFiles extends Thread{
  /** Logger. */
  private static final Log LOG =
    LogFactory.getLog(ExpireUnusedJobFiles.class);

  /** Clock. */
  private final Clock clock;
  /** The directory to clean. */
  private final Path dirToClean;
  /** The filesystem to use. */
  private final Configuration conf;
  /** clean threshold in milliseconds. */
  private final long cleanThreshold;
  /** pattern to match for the files to be deleted */
  private final Pattern fileToCleanPattern;
  private long cleanInterval;

  /**
   * Constructor.
   * @param clock The clock.
   * @param dirToClean The directory to be cleaned
   * @param fs The filesystem.
   * @param fileToCleanPattern the pattern for the filename
   * @param cleanThreshold the time to clean the dir
   * @param cleanInterval the interval to clean the dir
   */
  public ExpireUnusedJobFiles(
    Clock clock, Configuration conf,
    Path dirToClean, Pattern fileToCleanPattern,
    long cleanThreshold, long cleanInterval) {
    this(clock, conf, dirToClean, fileToCleanPattern, cleanThreshold);
    this.cleanInterval = cleanInterval;
    setDaemon(true);
    LOG.info("ExpireUnusedJobFiles created with " +
      " path = " + dirToClean +
      " cleanInterval = " + cleanInterval +
      " cleanThreshold = " + cleanThreshold);
  }

  /**
   * Constructor.
   * @param clock The clock.
   * @param dirToClean The directory to be cleaned
   * @param fs The filesystem.
   * @param fileToCleanPattern the pattern for the filename
   * @param cleanThreshold the time to clean the dir
   */
  public ExpireUnusedJobFiles(
    Clock clock, Configuration conf,
    Path dirToClean, Pattern fileToCleanPattern,
    long cleanThreshold) {
    this.clock = clock;
    this.conf = conf;
    this.dirToClean = dirToClean;
    this.fileToCleanPattern = fileToCleanPattern;
    this.cleanThreshold = cleanThreshold;
    this.cleanInterval = 0;
  }

  @Override
  public void run() {
    while (true) {
      long currentTime = clock.getTime();
      try {
        LOG.info(Thread.currentThread().getId() + ":Trying to clean " + dirToClean);
        FileSystem fs = dirToClean.getFileSystem(conf);
        if (!fs.exists(dirToClean)) {
          LOG.info(dirToClean + " doesn't exist");
          return;
        }
  
        RemoteIterator<LocatedFileStatus> itor;
        for( itor = fs.listLocatedStatus(dirToClean); itor.hasNext();) {
          LocatedFileStatus dirStat = itor.next();
          // Check if this is a directory matching the pattern
          if (!dirStat.isDir()) {
            continue;
          }
          Path subDirPath = dirStat.getPath();
          String dirname = subDirPath.toUri().getPath();
          Matcher m = fileToCleanPattern.matcher(dirname);
          if (m.find()) {
            if (currentTime - dirStat.getModificationTime() > cleanThreshold) {
              // recursively delete all the files/dirs
              LOG.info("Delete " + subDirPath);
              fs.delete(subDirPath, true);
            }
          }
        }
      } catch (IOException ioe) {
        LOG.error("IOException when clearing dir ", ioe);
      }
      if (cleanInterval == 0) {
        return;
      }
      try {
        Thread.sleep(cleanInterval);
      } catch (InterruptedException e) {
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.err.println("Usage: " + ExpireUnusedJobFiles.class + " path pattern thresholdsec");
      System.exit(1);
    }
    Configuration conf = new Configuration();
    Path dir = new Path(args[0]);
    Pattern p = Pattern.compile(args[1]);
    long clearThreshold = Integer.parseInt(args[2]) * 1000L;
    ExpireUnusedJobFiles expire = new ExpireUnusedJobFiles(new Clock(), conf, dir, p , clearThreshold);
    expire.run();
  }
}


