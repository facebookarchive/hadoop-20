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
package org.apache.hadoop.metrics;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Tracing class for recording client-side API calls and RPC calls.
 */
public class APITrace {
  private static final Log API_TRACE_LOG = LogFactory.getLog(APITrace.class);
  private static final long baseTime = System.nanoTime();
  private static AtomicLong nextStreamId = new AtomicLong(1);

  // start auto(callIds)
  public static final int CALL_COLLAPSED = 1;
  public static final int CALL_open = 2;
  public static final int CALL_create = 3;
  public static final int CALL_create1 = 4;
  public static final int CALL_seek = 5;
  public static final int CALL_getPos = 6;
  public static final int CALL_seekToNewSource = 7;
  public static final int CALL_read = 8;
  public static final int CALL_readFully = 9;
  public static final int CALL_readFully1 = 10;
  public static final int CALL_read1 = 11;
  public static final int CALL_read2 = 12;
  public static final int CALL_read3 = 13;
  public static final int CALL_skip = 14;
  public static final int CALL_available = 15;
  public static final int CALL_close = 16;
  public static final int CALL_mark = 17;
  public static final int CALL_reset = 18;
  public static final int CALL_sync = 19;
  public static final int CALL_write = 20;
  public static final int CALL_write1 = 21;
  public static final int CALL_write2 = 22;
  public static final int CALL_flush = 23;
  public static final int CALL_close1 = 24;
  // end auto

  // we only support static methods
  private APITrace() {};

  /**
   * Record a method call and its return value in the log.
   *
   * @param entry the System.nanoTime timestamp for method entry
   * @param callIndex index into callTable
   * @param returnValue value returned by traced method
   * @param argValues arguments passed to traced method
   * @param streamId unique identifier for stream, or -1 if not applicable
   */
  public static void logCall(long entryTime,
                             long returnTime,
                             int callIndex,
                             Object returnValue,
                             Object argValues[],
                             long streamId) {
    if (!API_TRACE_LOG.isTraceEnabled()) {
      return;
    }

    // determine elapsed time
    long elapsed = returnTime;
    elapsed -= entryTime;
    entryTime -= baseTime;
    // TODO: for the first entry, we get negatives for entryTime.
    // is this something weird in order the Java instantiates?

    // append universal fields (i.e., ones that occur for every call)
    StringBuilder line = new StringBuilder();
    line.append(entryTime + ",");
    line.append(elapsed + ",");
    line.append(callIndex + ",");
    line.append(streamId + ",");
    line.append(escape(returnValue));

    // append the args to the method call
    if (argValues != null) {
      for (int i = 0; i < argValues.length; i++) {
        line.append("," + escape(argValues[i]));
      }
    }

    API_TRACE_LOG.trace(line);
  }

  // convert 
  private static String escape(Object val) {
    if (val == null) {
      return "null";
    } else if (val instanceof TraceableStream) {
      TraceableStream ts = (TraceableStream)val;
      return ts.getStreamTracer().getStreamId().toString();
    } else {
      try {
        return "#"+SimpleBase64.encode(val.toString().getBytes("UTF-8"));
      } catch (java.io.UnsupportedEncodingException e) {
        return "CouldNotEncode";
      }
    }
  }

  protected static long nextStreamId() {
    return nextStreamId.getAndIncrement();
  }


  /**
   * Represents a call to a method we trace, and records time info, args, etc.
   */
  public static class CallEvent {
    private long entryTime;

    // CallEvent objects should be created at the beginning of a traced method
    public CallEvent() {
      entryTime = System.nanoTime();
    }

    public void logCall(int callIndex,
                        Object returnValue,
                        Object argValues[],
                        long streamId,
                        long returnTime) {
      APITrace.logCall(entryTime, returnTime, callIndex,
                       returnValue, argValues, streamId);
    }

    public void logCall(int callIndex,
                        Object returnValue,
                        Object argValues[],
                        long streamId) {
      long returnTime = System.nanoTime();
      APITrace.logCall(entryTime, returnTime, callIndex,
                       returnValue, argValues, streamId);
    }

    public void logCall(int callIndex,
                       Object returnValue,
                       Object argValues[]) {
      logCall(callIndex, returnValue, argValues, -1);
    }
  }


  public static interface TraceableStream {
    StreamTracer getStreamTracer();
  }


  /**
   * Interface for all tracing of streams.  This serves two purposes beyond what
   * APITrace provides: (1) track stream IDs, (2) collapse many small calls.
   * Collapsing many small calls is useful, as some workloads (e.g., HBase)
   * perform many 1-byte writes.
   */
  public static class StreamTracer {
    private long streamId;

    // variables for collapsing calls
    private CallEvent collapsedCall;
    private int callCount;
    private int byteCount;
    private long returnTime;
    private static final int maxByteCount = 4096;

    public StreamTracer() {
      streamId = nextStreamId();
      reset();
    }

    public Long getStreamId() {
      return streamId;
    }

    // log a call that cannot be collapsed
    public synchronized void logCall(CallEvent event,
                        int callIndex,
                        Object returnValue,
                        Object argValues[]) {
      flush();
      event.logCall(callIndex, returnValue, argValues, getStreamId());
    }

    // log an I/O call that can be collapsed with other calls
    public synchronized void logIOCall(CallEvent call, long incVal) {
      if (byteCount + incVal > maxByteCount) {
        flush();
      }

      // increment aggregates that describe the I/Os
      if (this.collapsedCall == null) {
        this.collapsedCall = call;
      }
      callCount++;
      byteCount += incVal;
      returnTime = System.nanoTime();

      if (byteCount >= maxByteCount) {
        flush();
      }
    }

    // write log entry for all the small calls that are buffered so far
    private void flush() {
      if (collapsedCall == null) {
        return;
      }
      collapsedCall.logCall(CALL_COLLAPSED,
                            null,
                            new Object []{callCount, byteCount},
                            getStreamId(),
                            returnTime);
      reset();
    }

    private void reset() {
      collapsedCall = null;
      callCount = 0;
      byteCount = 0;
      returnTime = 0;
    }
  }
}
