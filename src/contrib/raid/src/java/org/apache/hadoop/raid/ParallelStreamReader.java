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

import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
 * Reads data from multiple input streams in parallel.
 */
public class ParallelStreamReader {
  public static final Log LOG = LogFactory.getLog(ParallelStreamReader.class);
  Progressable reporter;
  InputStream[] streams;
  ExecutorService readPool;
  Semaphore slots;
  int numThreads;
  long remainingBytesPerStream;
  int bufSize;
  long readTime = 0;
  volatile boolean running = true;

  public static class ReadResult {
    public byte[][] readBufs;
    public IOException[] ioExceptions;
    ReadResult(int numStreams, int bufSize) {
      this.readBufs = new byte[numStreams][];
      for (int i = 0; i < readBufs.length; i++) {
        this.readBufs[i] = new byte[bufSize];
      }
      this.ioExceptions = new IOException[readBufs.length];
    }

    void setException(int idx, Exception e) {
      synchronized(ioExceptions) {
        if (e == null) {
          ioExceptions[idx] = null;
          return;
        }
        if (e instanceof IOException) {
          ioExceptions[idx] = (IOException) e;
        } else {
          ioExceptions[idx] = new IOException(e);
        }
      }
    }

    IOException getException() {
      synchronized(ioExceptions) {
        for (int i = 0; i < ioExceptions.length; i++) {
          if (ioExceptions[i] != null) {
            return ioExceptions[i];
          }
        }
      }
      return null;
    }
  }

  BlockingQueue<ReadResult> boundedBuffer;
  Thread mainThread;

  /**
   * Reads data from multiple streams in parallel and puts the data in a queue.
   * @param streams The input streams to read from.
   * @param bufSize The amount of data to read from each stream in each go.
   * @param numThreads Number of threads to use for parallelism.
   * @param boundedBuffer The queue to place the results in.
   */
  public ParallelStreamReader(
      Progressable reporter,
      InputStream[] streams,
      int bufSize,
      int numThreads,
      int boundedBufferCapacity,
      long maxBytesPerStream) {
    this.reporter = reporter;
    this.streams = new InputStream[streams.length];
    for (int i = 0; i < streams.length; i++) {
      this.streams[i] = streams[i];
      streams[i] = null; // Take over ownership of streams.
    }
    this.bufSize = bufSize;
    this.boundedBuffer =
      new ArrayBlockingQueue<ReadResult>(boundedBufferCapacity);
    if (numThreads > streams.length) {
      this.numThreads = streams.length;
    } else {
      this.numThreads = numThreads;
    }
    this.remainingBytesPerStream = maxBytesPerStream;
    this.slots = new Semaphore(this.numThreads);
    this.readPool = Executors.newFixedThreadPool(this.numThreads);
    this.mainThread = new MainThread();
  }

  public void start() {
    this.mainThread.start();
  }

  public void shutdown() {
    LOG.info("Shutting down parallel stream reader");
    running = false;
    try {
      readPool.shutdownNow();
    } catch (Exception e) {}
    try {
      mainThread.interrupt();
    } catch (Exception e) {}
    for (int i = 0; i < streams.length; i++) {
      if (streams[i] != null) {
        try {
          streams[i].close();
          streams[i] = null;
        } catch (IOException e) {}
      }
    }
  }

  public ReadResult getReadResult() throws InterruptedException {
    return boundedBuffer.take();
  }

  class MainThread extends Thread {
    public void run() {
      while (running) {
        ReadResult readResult = new ReadResult(streams.length, bufSize);
        try {
          // Do not try to read more data if the desired amount of data has
          // been read.
          if (remainingBytesPerStream == 0) {
            return;
          }
          performReads(readResult);
          // Enqueue to bounder buffer.
          boundedBuffer.put(readResult);
          remainingBytesPerStream -= bufSize;
        } catch (InterruptedException e) {
          running = false;
        }
      }
    }
  }

  /**
   * Performs a batch of reads from the given streams and waits
   * for the reads to finish.
   */
  private void performReads(ReadResult readResult) throws InterruptedException {
    long start = System.currentTimeMillis();
    for (int i = 0; i < streams.length; ) {
      boolean acquired = slots.tryAcquire(1, 10, TimeUnit.SECONDS);
      reporter.progress();
      if (acquired) {
        readPool.execute(new ReadOperation(readResult, i));
        i++;
      }
    }
    // All read operations have been submitted to the readPool.
    // Now wait for the operations to finish and release the semaphore.
    while (true) {
      boolean acquired =
        slots.tryAcquire(numThreads, 10, TimeUnit.SECONDS);
      reporter.progress();
      if (acquired) {
        slots.release(numThreads);
        break;
      }
    }

    readTime += (System.currentTimeMillis() - start);
  }

  class ReadOperation implements Runnable {
    ReadResult readResult;
    int idx;
    ReadOperation(ReadResult readResult, int idx) {
      this.readResult = readResult;
      this.idx = idx;
    }

    public void run() {
      readResult.setException(idx, null);
      try {
        if (streams[idx] == null) {
          // We encountered an error in this stream earlier, use zeros.
          Arrays.fill(readResult.readBufs[idx], (byte) 0);
          return;
        }
        boolean eofOK = true;
        byte[] buffer = readResult.readBufs[idx];
        RaidUtils.readTillEnd(streams[idx], buffer, eofOK);
      } catch (Exception e) {
        LOG.warn("Encountered exception in stream " + idx, e);
        readResult.setException(idx, e);
        try {
          streams[idx].close();
        } catch (IOException ioe) {}
        streams[idx] = null;
      } finally {
        ParallelStreamReader.this.slots.release();
      }
    }
  }
}
