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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test the use of DFSInputStream by multiple concurrent readers.
 * 
 */
public class TestParallelRead {
  static final Log LOG = LogFactory.getLog(TestParallelRead.class);
  static BlockReaderTestUtil util = null;
  static DFSClient dfsClient = null;
  static final int FILE_SIZE_K = 256;
  static Random rand = null;
  static final int NUM_DATANODES = 3;

  private class TestFileInfo {
    public DFSInputStream dis;
    public Path filePath;
    public byte[] authenticData;
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = new Configuration();
    
    util = new BlockReaderTestUtil(NUM_DATANODES, conf);
    dfsClient = util.getDFSClient();
    rand = new Random(System.currentTimeMillis());
  }

  static class ReadWorker extends Thread {
    static public final int N_ITERATIONS = 1024;

    private static final double PROPORTION_NON_POSITIONAL_READ = 0.10;

    private TestFileInfo testInfo;
    private long fileSize;
    private long bytesRead;
    private boolean error;

    ReadWorker(TestFileInfo testInfo, int id) {
      super("ReadWorker-" + id + "-" + testInfo.filePath.toString());
      this.testInfo = testInfo;
      fileSize = testInfo.dis.getFileLength();
      assertEquals(fileSize, testInfo.authenticData.length);
      bytesRead = 0;
      error = false;
    }

    /**
     * Randomly do one of (1) Small read; and (2) Large Pread.
     */
    @Override
    public void run() {
      for (int i = 0; i < N_ITERATIONS; ++i) {
        int startOff = rand.nextInt((int) fileSize - 2);
        int len = 0;
        try {
          double p = rand.nextDouble();
          if (p < PROPORTION_NON_POSITIONAL_READ) {
            // Do a small regular read. Very likely this will leave unread
            // data on the socket and make the socket uncacheable
            // make sure we will read at least 1 byte
            len = Math.min(rand.nextInt(64), 
                (int) (fileSize - startOff - 2)) + 1;
            read(startOff, len);
            bytesRead += len;
          } else {
            // Do a positional read most of the time.
            // make sure we will read at least 1 byte
            len = rand.nextInt((int) fileSize - startOff - 2) + 1;
            pRead(startOff, len);
            bytesRead += len;
          }
        } catch (Exception ex) {
          LOG.error(getName() + ": Error while testing read at " + startOff
              + " length " + len);
          error = true;
          fail(ex.getMessage());
        }
      }
    }

    public long getBytesRead() {
      return bytesRead;
    }

    /**
     * Raising error in a thread doesn't seem to fail the test. So check
     * afterwards.
     */
    public boolean hasError() {
      return error;
    }

    /**
     * Seek to somewhere and read.
     */
    private void read(int start, int len) throws Exception {
      assertTrue("Bad args: " + start + " + " + len + " should be < "
          + fileSize, start + len < fileSize);
      DFSInputStream dis = testInfo.dis;

      synchronized (dis) {
        dis.seek(start);

        byte buf[] = new byte[len];
        int cnt = 0;
        while (cnt < len) {
          cnt += dis.read(buf, cnt, buf.length - cnt);
        }
        verifyData("Read data corrupted", buf, start, start + len);
      }
    }

    /**
     * Positional read.
     */
    private void pRead(int start, int len) throws Exception {
      assertTrue("Bad args" + start + " + " + len + " should be < " + fileSize,
          start + len < fileSize);
      DFSInputStream dis = testInfo.dis;

      byte buf[] = new byte[len];
      int cnt = 0;
      while (cnt < len) {
        cnt += dis.read(start, buf, cnt, buf.length - cnt);
      }
      verifyData("Pread data corrupted", buf, start, start + len);
    }

    /**
     * Verify read data vs authentic data
     */
    private void verifyData(String msg, byte actual[], int start, int end)
        throws Exception {
      byte auth[] = testInfo.authenticData;
      if (end > auth.length) {
        throw new Exception(msg + ": Actual array (" + end
            + ") is past the end of authentic data (" + auth.length + ")");
      }

      int j = start;
      for (int i = 0; i < actual.length; ++i, ++j) {
        if (auth[j] != actual[i]) {
          throw new Exception(msg + ": Arrays byte " + i + " (at offset " + j
              + ") differs: expect " + auth[j] + " got " + actual[i]);
        }
      }
    }
  }

  /**
   * Do parallel read several times with different number of files and threads.
   * 
   * Not that while this is the only "test" in a junit sense, we're actually
   * dispatching a lot more. Failures in the other methods (and other threads)
   * need to be manually collected, which is inconvenient.
   */
  @Test
  public void testParallelRead() throws IOException {
    if (!runParallelRead(1, 4)) {
      fail("Check log for errors");
    }
    if (!runParallelRead(1, 16)) {
      fail("Check log for errors");
    }
    if (!runParallelRead(2, 4)) {
      fail("Check log for errors");
    }
  }

  /**
   * Start the parallel read with the given parameters.
   */
  boolean runParallelRead(int nFiles, int nWorkerEach) throws IOException {
    ReadWorker workers[] = new ReadWorker[nFiles * nWorkerEach];
    TestFileInfo testInfoArr[] = new TestFileInfo[nFiles * nWorkerEach];

    // Prepare the files and workers
    int nWorkers = 0;
    for (int i = 0; i < nFiles; i++) {
      TestFileInfo testInfo = new TestFileInfo();

      testInfo.filePath = new Path("/TestParallelRead.dat" + i);
      testInfo.authenticData = util.writeFile(testInfo.filePath, FILE_SIZE_K);

      for (int j = 0; j < nWorkerEach; ++j) {
        TestFileInfo infoPerThread = new TestFileInfo();
        infoPerThread.filePath = testInfo.filePath;
        infoPerThread.authenticData = testInfo.authenticData;
        infoPerThread.dis = dfsClient.open(testInfo.filePath.toString());
        testInfoArr[nWorkers] = infoPerThread;
        workers[nWorkers++] = new ReadWorker(infoPerThread, nWorkers);
      }
    }

    // Start the workers and wait
    long startTime = System.currentTimeMillis();
    for (ReadWorker worker : workers) {
      worker.start();
    }

    for (ReadWorker worker : workers) {
      try {
        worker.join();
      } catch (InterruptedException ignored) {
      }
    }
    long endTime = System.currentTimeMillis();

    // Cleanup
    for (TestFileInfo testInfo : testInfoArr) {
      testInfo.dis.close();
    }

    // Report
    boolean res = true;
    long totalRead = 0;
    for (ReadWorker worker : workers) {
      long nread = worker.getBytesRead();
      LOG.info("--- Report: " + worker.getName() + " read " + nread + " B; "
          + "average " + nread / ReadWorker.N_ITERATIONS + " B per read");
      totalRead += nread;
      if (worker.hasError()) {
        res = false;
      }
    }

    double timeTakenSec = (endTime - startTime) / 1000.0;
    long totalReadKB = totalRead / 1024;
    LOG.info("=== Report: " + nWorkers + " threads read " + totalReadKB
        + " KB (across" + nFiles + " file(s)) in " + timeTakenSec
        + "s; average " + totalReadKB / timeTakenSec + " KB/s");

    return res;
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    util.shutdown();
  }
}
