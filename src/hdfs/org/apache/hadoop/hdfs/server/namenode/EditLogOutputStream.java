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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.jasper.compiler.JspUtil;

/**
 * A generic abstract class to support journaling of edits logs into 
 * a persistent storage.
 */
public abstract class EditLogOutputStream {
  // these are statistics counters
  private long numSync;        // number of sync(s) to disk
  private long totalTimeSync;  // total time to sync
  public MetricsTimeVaryingRate sync;

  public EditLogOutputStream() throws IOException {
    numSync = totalTimeSync = 0;
  }
  
  /** 
   * Get this stream name. 
   * 
   * @return name of the stream
   */    
  abstract public String getName();

  /**
   * Write edits log operation to the stream.
   * 
   * @param op operation
   * @throws IOException
   */
  abstract public void write(FSEditLogOp op) throws IOException;
  
  /**
   * Write a transaction to the edit log in serialized form.
   *
   * @param bytes the bytes of the transaction to write.
   * @param offset offset in the bytes to write from
   * @param length number of bytes to write
   * @param txid transaction id
   * @throws IOException
   */
  abstract public void writeRawOp(byte[] bytes, int offset, int length, long txid)
      throws IOException;
  
  /**
   * Write raw data to an edit log. This data should already have
   * the transaction ID, checksum, etc included. It is for use
   * within the JournalNode when replicating edits from the
   * NameNode.
   *
   * @param bytes the bytes to write.
   * @param offset offset in the bytes to write from
   * @param length number of bytes to write
   * @throws IOException
   */
  abstract public void writeRaw(byte[] bytes, int offset, int length)
      throws IOException;
  
  /**
   * Create and initialize new edits log storage.
   * 
   * @throws IOException
   */
  abstract public void create() throws IOException;

  /** {@inheritDoc} */
  abstract public void close() throws IOException;
  
  /**
   * All data that has been written to the stream so far will be flushed.
   * New data can be still written to the stream while flushing is performed.
   */
  abstract public void setReadyToFlush() throws IOException;

  /**
   * Flush and sync all data that is ready to be flush 
   * {@link #setReadyToFlush()} into underlying persistent store.
   * @throws IOException
   */
  abstract protected void flushAndSync(boolean durable) throws IOException;

  public void flush() throws IOException {
    flush(true);
  }
  
  /**
   * Flush data to persistent store.
   * Collect sync metrics.
   */
  public void flush(boolean durable) throws IOException {
    numSync++;
    long start = System.nanoTime();
    flushAndSync(durable);
    long time = DFSUtil.getElapsedTimeMicroSeconds(start);
    totalTimeSync += time;
    if (sync != null) {
      sync.inc(time);
    }
  }
  
  /**
   * Return the size of the current edits log.
   * Length is used to check when it is large enough to start a checkpoint.
   */
  abstract public long length() throws IOException;
  
  /**
   * Return total time spent in {@link #flushAndSync()}
   */
  public long getTotalSyncTime() {
    return totalTimeSync;
  }

  /**
   * Return number of calls to {@link #flushAndSync()}
   */
  public long getNumSync() {
    return numSync;
  }

  /**
   * Aborts this stream
   */
  public void abort() throws IOException {
    
  }
  
  /**
   * Implement the policy when to automatically sync the buffered edits log
   * The buffered edits can be flushed when the buffer becomes full or
   * a certain period of time is elapsed.
   * 
   * @return true if the buffered data should be automatically synced to disk
   */
  public boolean shouldForceSync() {
    return false;
  }
  
  /**
   * @return a short HTML snippet suitable for describing the current
   * status of the stream
   */
  public String generateHtmlReport() {
    return "";
  }
}
