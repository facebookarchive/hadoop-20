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
package org.apache.hadoop.raid;

import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestParallelReader extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestParallelReader.class);

  /**
   * Allows read of one byte of data.
   */
  public static class SleepInputStream extends InputStream {
    private int remaining = 1;
    private long millis;

    public SleepInputStream(long millis) {
      this.millis = millis;
    }

    public int available() { return remaining; }

    public int read() {
      if (remaining > 0) {
        try {
          Thread.sleep(millis);
        } catch (InterruptedException e) {}
        remaining--;
        return 1;
      }
      return -1;
    }
  }

  public void testParallelism() throws IOException, InterruptedException {
    LOG.info("testParallelism starting");

    int bufSize = 10;

    long sleep = 100;
    InputStream[] streams = new InputStream[10];
    for (int i = 0; i < streams.length; i++) {
      streams[i] = new SleepInputStream(sleep);
    }

    // Read using 1 thread.
    ParallelStreamReader parallelReader;
    parallelReader = new ParallelStreamReader(
      RaidUtils.NULL_PROGRESSABLE,
      streams,
      bufSize,
      1,
      1,
      bufSize);
    try {
      parallelReader.start();
      ParallelStreamReader.ReadResult readResult =
        parallelReader.getReadResult();
      LOG.info("Reads using 1 thread finished in " +
        parallelReader.readTime + " msec");
      assertTrue("Sequential read", parallelReader.readTime >= sleep * 10);
    } finally {
      parallelReader.shutdown();
    }

    // Read using 10 threads.
    for (int i = 0; i < streams.length; i++) {
      streams[i] = new SleepInputStream(sleep);
    }
    parallelReader = new ParallelStreamReader(
      RaidUtils.NULL_PROGRESSABLE,
      streams,
      bufSize,
      10,
      1,
      bufSize);
    try {
      parallelReader.start();
      ParallelStreamReader.ReadResult readResult =
        parallelReader.getReadResult();
      LOG.info("Reads using 10 threads finished in " +
        parallelReader.readTime  + " msec");
      assertTrue("Parallel read", parallelReader.readTime   <= sleep * 2);
    } finally {
      parallelReader.shutdown();
    }

    LOG.info("testParallelism finished");
  }
}
