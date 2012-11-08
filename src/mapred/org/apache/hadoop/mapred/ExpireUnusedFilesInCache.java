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

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Used to expire files in cache that hasn't been accessed for a while
 */
public class ExpireUnusedFilesInCache implements Runnable {
  /** Logger. */
  private static final Log LOG =
    LogFactory.getLog(ExpireUnusedFilesInCache.class);

  /** Configuration. */
  private final Configuration conf;
  /** Clock. */
  private final Clock clock;
  /** The directories to purge. */
  private final Path[] cachePath;
  /** The filesystem to use. */
  private final FileSystem fs;
  /** Expire threshold in milliseconds. */
  private final long expireCacheThreshold;

  /**
   * Constructor.
   * @param conf The configuration.
   * @param clock The clock.
   * @param systemDir The system directory.
   * @param fs The filesystem.
   */
  public ExpireUnusedFilesInCache(
    Configuration conf, Clock clock, Path systemDir, FileSystem fs) {
    this.conf = conf;
    this.clock = clock;
    this.fs = fs;

    Path sharedPath = new Path(systemDir, JobSubmissionProtocol.CAR);
    sharedPath = sharedPath.makeQualified(fs);
    this.cachePath = new Path[3];
    this.cachePath[0] = new Path(sharedPath, "files");
    this.cachePath[1] = new Path(sharedPath, "archives");
    this.cachePath[2] = new Path(sharedPath, "libjars");


    long clearCacheInterval = conf.getLong(
      "mapred.cache.shared.check_interval",
      24 * 60 * 60 * 1000);

    expireCacheThreshold =
      conf.getLong("mapred.cache.shared.expire_threshold",
        24 * 60 * 60 * 1000);
    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(
      this,
      clearCacheInterval,
      clearCacheInterval,
      TimeUnit.MILLISECONDS);

    LOG.info("ExpireUnusedFilesInCache created with " +
      " sharedPath = " + sharedPath +
      " clearCacheInterval = " + clearCacheInterval +
      " expireCacheThreshold = " + expireCacheThreshold);
  }

  @Override
  public void run() {
    long currentTime = clock.getTime();

    for (int i = 0; i < cachePath.length; i++) {
      try {
        if (!fs.exists(cachePath[i])) continue;

        FileStatus[] fStatus = fs.listStatus(cachePath[i]);

        for (int j = 0; j < fStatus.length; j++) {
          if (!fStatus[j].isDir()) {
            long atime = fStatus[j].getAccessTime();

            if (currentTime - atime > expireCacheThreshold) {
              fs.delete(fStatus[j].getPath(), false);
            }
          }
        }
      } catch (IOException ioe) {
        LOG.error("IOException when clearing cache");
      }
    }
  }
}


