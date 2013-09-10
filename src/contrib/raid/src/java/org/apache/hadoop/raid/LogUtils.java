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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.log.LogSample;

public class LogUtils {
  public static enum LOGTYPES {
    ONLINE_RECONSTRUCTION,
    ONLINE_RECONSTRUCTION_GET_STRIPE,
    OFFLINE_RECONSTRUCTION_FILE,
    OFFLINE_RECONSTRUCTION_BLOCK,
    OFFLINE_RECONSTRUCTION_TOO_MANY_CORRUPTIONS,
    OFFLINE_RECONSTRUCTION_SIMULATION,
    OFFLINE_RECONSTRUCTION_GET_CHECKSUM,
    OFFLINE_RECONSTRUCTION_CHECKSUM_VERIFICATION,
    OFFLINE_RECONSTRUCTION_GET_STRIPE,
    OFFLINE_RECONSTRUCTION_USE_STRIPE,
    OFFLINE_RECONSTRUCTION_STRIPE_VERIFICATION,
    TOOL_FILECHECK,
    FILE_FIX_WAITTIME,
    MODIFICATION_TIME_CHANGE,
    ENCODING,
    READ,
  };
  public static enum LOGRESULTS {
    SUCCESS,
    FAILURE,
    NOACTION,
    NONE
  };
  public static enum LOGKEYS {
    // Int type:
    ConstructedBytes,
    Delay,
    DecodingTime,
    ReadBytes,
    RemoteRackReadBytes,
    RecoveryTime,
    Offset,
    ReadBlocks,
    MetaBlocks,
    MetaBytes,
    SavingBytes,
    Limit,
    FileFixWaitTime,
    MaxPendingJobs,
    MaxFilesPerTask,
    // Normal type:
    Result,
    Code,
    MissingBlocks,
    Path,
    Type,
    Cluster,
    Error,
  };
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.LogUtils");
  public static final Log DECODER_METRICS_LOG = LogFactory.getLog("RaidMetrics");
  public static final Log ENCODER_METRICS_LOG = LogFactory.getLog("RaidMetrics"); 
  public static final Log FILECHECK_METRICS_LOG = LogFactory.getLog("RaidMetrics");
  public static final Log EVENTS_LOG = LogFactory.getLog("RaidMetrics");
  public final static String LOG_COUNTER_GROUP_NAME = "log";
  
  public static String getCounterName(FileSystem fs, LOGTYPES type,  
      LOGRESULTS result) { 
    return getCounterName(fs, type, result, null); 
  }  

  public static String getCounterName(FileSystem fs, LOGTYPES type,
      LOGRESULTS result, String tag) { 
    String counterName = fs.getUri().getAuthority() + "_" +  type.name();
    if (result != LOGRESULTS.NONE) { 
      counterName += "_" + result.name();  
    }  
    if (tag != null && tag.length() > 0) 
      counterName += "_" + tag;  
    //remove all white charaters 
    return counterName.replace(" ", "_");  
  }
  
  public static void incrLogMetricCounter(Context context, FileSystem fs,
      LOGTYPES type, LOGRESULTS result, String tag) {
    if (context != null) {
      String totalName = getCounterName(fs, type, result);
      context.getCounter(LOG_COUNTER_GROUP_NAME, totalName).increment(1L);
      if (tag != null && tag.length() > 0) {
        String counterName = getCounterName(fs, type, result, tag); 
        context.getCounter(LOG_COUNTER_GROUP_NAME, counterName).increment(1L);
      }
    }
  }
  
  public static void incrLogMetricCounter(Progressable context, FileSystem fs,
      LOGTYPES type, LOGRESULTS result, String tag) {
    if (context != null && context instanceof Reporter && context != Reporter.NULL) {
      String totalName = getCounterName(fs, type, result);
      ((Reporter)context).getCounter(LOG_COUNTER_GROUP_NAME,
          totalName).increment(1L);
      if (tag != null && tag.length() > 0) {
        String counterName = getCounterName(fs, type, result, tag); 
        ((Reporter)context).getCounter(LOG_COUNTER_GROUP_NAME,
            counterName).increment(1L);
      }
    }
  }
  
  public static void incrRaidNodeMetricCounter(FileSystem fs, LOGTYPES type,
      LOGRESULTS result, String tag) {
    Map<String, Long> incrMetrics = new HashMap<String, Long>();
    String totalName = getCounterName(fs, type, result);
    incrMetrics.put(totalName, 1L);
    if (tag != null && tag.length() > 0) {
      String counterName = getCounterName(fs, type, result, tag);
      incrMetrics.put(counterName, 1L);
    }
    incrLogMetrics(incrMetrics); 
  }
  
  /**
   * Increase logMetrics in the Raidnode metrics
   */
  public static void incrLogMetrics(Map<String, Long> incrMetrics) {
    if (incrMetrics == null || incrMetrics.size() == 0) {
      return;
    }
    MetricsRegistry registry = RaidNodeMetrics.getInstance(
        RaidNodeMetrics.DEFAULT_NAMESPACE_ID).getMetricsRegistry();
    Map<String, MetricsTimeVaryingLong> logMetrics = RaidNodeMetrics.getInstance(
        RaidNodeMetrics.DEFAULT_NAMESPACE_ID).logMetrics;
    synchronized(logMetrics) {
      for (String key : incrMetrics.keySet()) {
        if (!logMetrics.containsKey(key)) {
          logMetrics.put(key, new MetricsTimeVaryingLong(key, registry));
        }
        ((MetricsTimeVaryingLong)logMetrics.get(key)).inc(incrMetrics.get(key));
      }
    }
  }
  
  public static void logRaidReconstructionMetrics(
      LOGRESULTS result, long bytes, Codec codec, Path srcFile,
      long errorOffset, LOGTYPES type, FileSystem fs,
      Throwable ex, Context context) {
    logRaidReconstructionMetrics(result, bytes, codec, -1, -1,
        -1, -1, -1, srcFile, errorOffset, type, fs, ex, context,
        -1);
  }
  
  public static void logRaidReconstructionMetrics(
      LOGRESULTS result, long bytes, Codec codec, Path srcFile,
      long errorOffset, LOGTYPES type, FileSystem fs,
      Throwable ex, Context context, long recoveryTime) {
    logRaidReconstructionMetrics(result, bytes, codec, -1, -1,
        -1, -1, -1, srcFile, errorOffset, type, fs, ex, context,
        recoveryTime);
  }
  
  public static void logRaidReconstructionMetrics(
      LOGRESULTS result, long bytes, Codec codec, long delay, 
      long decodingTime, int numMissingBlocks, long numReadBytes, 
      long numReadRemoteRackBytes, Path srcFile, 
      long errorOffset, LOGTYPES type, FileSystem fs,
      Throwable ex, Context context, long recoveryTime) {
    try {
      incrLogMetricCounter(context, fs, type, result, 
          codec == null? null: codec.id);
      LogSample sample = new LogSample();
      sample.addNormalValue(LOGKEYS.Result.name(), result.name());
      sample.addIntValue(LOGKEYS.ConstructedBytes.name(), bytes);
      if (null != codec) {
        sample.addNormalValue(LOGKEYS.Code.name(), codec.id);
      } else {
        sample.addNormalValue(LOGKEYS.Code.name(), "unknown");
      }
      if (delay >= 0) sample.addIntValue(LOGKEYS.Delay.name(), delay);
      if (decodingTime >= 0) sample.addIntValue(LOGKEYS.DecodingTime.name(),
          decodingTime);
      if (numMissingBlocks >= 0)
        sample.addNormalValue(LOGKEYS.MissingBlocks.name(),
            Integer.toString(numMissingBlocks));
      if (numReadBytes >= 0) sample.addIntValue(LOGKEYS.ReadBytes.name(),
          numReadBytes);
      if (numReadRemoteRackBytes >= 0) 
        sample.addIntValue(LOGKEYS.RemoteRackReadBytes.name(),
            numReadRemoteRackBytes);
      if (recoveryTime > 0) {
        sample.addIntValue(LOGKEYS.RecoveryTime.name(), recoveryTime);
      }
      sample.addNormalValue(LOGKEYS.Path.name(), srcFile.toString());
      sample.addIntValue(LOGKEYS.Offset.name(), errorOffset);
      sample.addNormalValue(LOGKEYS.Type.name(), type.name());
      sample.addNormalValue(LOGKEYS.Cluster.name(), fs.getUri().getAuthority());
      if (ex != null) {
        sample.addNormalValue(LOGKEYS.Error.name(),
            StringUtils.stringifyException(ex));
      }
      DECODER_METRICS_LOG.info(sample.toJSON());

    } catch(Exception e) {
      LOG.warn("Exception when logging the Raid metrics: " + e.getMessage(), 
               e);
    }
  }
  
  static public void logRaidEncodingMetrics(
      LOGRESULTS result, Codec codec, long delay, 
      long numReadBytes, long numReadBlocks,
      long metaBlocks, long metaBytes, 
      long savingBytes, Path srcPath, LOGTYPES type,
      FileSystem fs, Throwable ex, Progressable context) {
    try {
      incrLogMetricCounter(context, fs, type, result, codec.id);
      LogSample sample = new LogSample();
      sample.addNormalValue(LOGKEYS.Result.name(), result.name());
      sample.addNormalValue(LOGKEYS.Code.name(), codec.id);
      if (delay >= 0) sample.addIntValue(LOGKEYS.Delay.name(), delay);
      if (numReadBytes >= 0) sample.addIntValue(LOGKEYS.ReadBytes.name(),
          numReadBytes);
      if (numReadBlocks >= 0) sample.addIntValue(LOGKEYS.ReadBlocks.name(),
          numReadBlocks);
      if (metaBlocks >= 0) sample.addIntValue(LOGKEYS.MetaBlocks.name(),
          metaBlocks);
      if (metaBytes >=0) sample.addIntValue(LOGKEYS.MetaBytes.name(), metaBytes);
      if (savingBytes >=0) sample.addIntValue(LOGKEYS.SavingBytes.name(),
          savingBytes);
      sample.addNormalValue(LOGKEYS.Path.name(), srcPath.toString());
      sample.addNormalValue(LOGKEYS.Type.name(), type.name());
      sample.addNormalValue(LOGKEYS.Cluster.name(), fs.getUri().getAuthority());
      if (ex != null) {
        sample.addNormalValue(LOGKEYS.Error.name(),
            StringUtils.stringifyException(ex));
      }
      ENCODER_METRICS_LOG.info(sample.toJSON());
    } catch(Exception e) {
      LOG.warn("Exception when logging the Raid metrics: " + e.getMessage(), 
               e);
    }
  }
  
  static public void logFileCheckMetrics(
      LOGRESULTS result, Codec codec, Path srcPath, FileSystem fs,
      long offset, long limit,
      Throwable ex, Progressable context) {
    LOGTYPES type = LOGTYPES.TOOL_FILECHECK;
    try {
      incrLogMetricCounter(context, fs, type, result, codec.id);
      LogSample sample = new LogSample();
      sample.addNormalValue(LOGKEYS.Result.name(), result.name());
      sample.addNormalValue(LOGKEYS.Code.name(), codec.id);
      sample.addNormalValue(LOGKEYS.Path.name(), srcPath.toString());
      sample.addNormalValue(LOGKEYS.Type.name(), type.name());
      sample.addNormalValue(LOGKEYS.Cluster.name(), fs.getUri().getAuthority());
      sample.addIntValue(LOGKEYS.Offset.name(), offset);
      sample.addIntValue(LOGKEYS.Limit.name(), limit);
      if (ex != null) {
        sample.addNormalValue(LOGKEYS.Error.name(),
            StringUtils.stringifyException(ex));
      }
      new Throwable().printStackTrace();
      FILECHECK_METRICS_LOG.info(sample.toJSON());
    } catch(Exception e) {
      LOG.warn("Exception when logging the Raid metrics: " + e.getMessage(), 
          e);
    }
  }
  
  static public void logWaitTimeMetrics (long waitTime, long maxPendingJobsLimit,
      long filesPerTaskLimit, LOGTYPES type, FileSystem fs, Context context) {
    LogSample sample = new LogSample();
    sample.addIntValue(LOGKEYS.FileFixWaitTime.name(), waitTime);
    sample.addIntValue(LOGKEYS.MaxPendingJobs.name(), maxPendingJobsLimit);
    sample.addIntValue(LOGKEYS.MaxFilesPerTask.name(), filesPerTaskLimit);
    logEvent(fs, null, type, LOGRESULTS.NONE, null, context, sample, null);
  }
  
  static public void logEvent(FileSystem fs, Path path, LOGTYPES type,
      LOGRESULTS result, Codec codec, Context context, LogSample sample,
      String tag) {
    try {
      if (context == null) {
        incrRaidNodeMetricCounter(fs, type, result, tag);
      } else {
        incrLogMetricCounter(context, fs, type, result, tag);
      } 
      if (sample == null) sample = new LogSample();
      if (path != null) sample.addNormalValue(LOGKEYS.Path.name(), path.toString());
      if (codec != null) sample.addNormalValue(LOGKEYS.Code.name(), codec.id);
      sample.addNormalValue(LOGKEYS.Type.name(), type.name()); 
      sample.addNormalValue(LOGKEYS.Cluster.name(), fs.getUri().getAuthority());
      EVENTS_LOG.info(sample.toJSON());
    } catch (Exception e) {
      LOG.warn("Exception when logging the File_Fix_WaitTime metric : " +
          e.getMessage(), e);
    }
  }
}
