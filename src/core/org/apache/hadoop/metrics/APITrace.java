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
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;

/**
 * Tracing class for recording client-side API calls and RPC calls.
 */
public class APITrace {
  private static final Log API_TRACE_LOG = LogFactory.getLog(APITrace.class);
  private static final long baseTime = System.nanoTime();
  private static AtomicLong nextEventId = new AtomicLong(1);
  private static AtomicLong nextStreamId = new AtomicLong(1);
  private static String pid = getPid();

  // start auto(callIds)
  public static final int CALL_COLLAPSED = 1;
  public static final int CALL_getName = 2;
  public static final int CALL_getFileBlockLocations = 3;
  public static final int CALL_open = 4;
  public static final int CALL_append = 5;
  public static final int CALL_create = 6;
  public static final int CALL_create1 = 7;
  public static final int CALL_createNonRecursive = 8;
  public static final int CALL_createNonRecursive1 = 9;
  public static final int CALL_setReplication = 10;
  public static final int CALL_hardLink = 11;
  public static final int CALL_rename = 12;
  public static final int CALL_delete = 13;
  public static final int CALL_delete1 = 14;
  public static final int CALL_listStatus = 15;
  public static final int CALL_mkdirs = 16;
  public static final int CALL_iterativeGetOpenFiles = 17;
  public static final int CALL_getUsed = 18;
  public static final int CALL_getDefaultBlockSize = 19;
  public static final int CALL_getDefaultReplication = 20;
  public static final int CALL_getContentSummary = 21;
  public static final int CALL_getFileStatus = 22;
  public static final int CALL_getFileChecksum = 23;
  public static final int CALL_setVerifyChecksum = 24;
  public static final int CALL_close = 25;
  public static final int CALL_setOwner = 26;
  public static final int CALL_setTimes = 27;
  public static final int CALL_setPermission = 28;
  public static final int CALL_seek = 29;
  public static final int CALL_getPos = 30;
  public static final int CALL_seekToNewSource = 31;
  public static final int CALL_read = 32;
  public static final int CALL_readFully = 33;
  public static final int CALL_readFully1 = 34;
  public static final int CALL_read1 = 35;
  public static final int CALL_read2 = 36;
  public static final int CALL_read3 = 37;
  public static final int CALL_skip = 38;
  public static final int CALL_available = 39;
  public static final int CALL_close1 = 40;
  public static final int CALL_mark = 41;
  public static final int CALL_reset = 42;
  public static final int CALL_sync = 43;
  public static final int CALL_write = 44;
  public static final int CALL_write1 = 45;
  public static final int CALL_write2 = 46;
  public static final int CALL_flush = 47;
  public static final int CALL_close2 = 48;
  // pseudo event for tracing of info besides API calls
  public static final int COMMENT_msg = 49;


  // we only support static methods
  private APITrace() {};

  private static String getPid() {
    // Generally .getName() will return "UNIX-PID@MACHINE-NAME", but this is JVM specific.  If the
    // string follows this format, we just return the UNIX-PID.  Otherwise, we return the entire ID.
    String name = ManagementFactory.getRuntimeMXBean().getName();
    String parts[] = name.split("@");
    if (parts.length == 2) {
      return parts[0];
    }
    return name;
  }

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
    if (!API_TRACE_LOG.isInfoEnabled()) {
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
    line.append(pid + ",");
    line.append(nextEventId.getAndIncrement() + ",");
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

    API_TRACE_LOG.info(line);
  }

  // convert 
  private static String escape(Object val) {
    if (val == null) {
      return "null";
    } else if (val instanceof TraceableStream) {
      TraceableStream ts = (TraceableStream)val;
      return ts.getStreamTracer().getStreamId().toString();
    } else if (val instanceof FileStatus) {
      FileStatus stat = (FileStatus)val;
      String properties = "isdir=" + stat.isDir() + ",len=" + stat.getLen();
      try {
        return "#"+SimpleBase64.encode(properties.toString().getBytes("UTF-8"));
      } catch (java.io.UnsupportedEncodingException e) {
        return "CouldNotEncode";
      }
    } else if (val instanceof FileStatus[]) {
      FileStatus stat[] = (FileStatus[])val;
      return Integer.toString(stat.length);
    } else if (val instanceof Long) {
      return Long.toString((Long)val);
    } else if (val instanceof Integer) {
      return Integer.toString((Integer)val);
    } else if (val instanceof Short) {
      return Integer.toString((Short)val);
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
