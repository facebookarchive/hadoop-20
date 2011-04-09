package org.apache.hadoop.hdfs.metrics;

import java.lang.management.ThreadMXBean;
import java.lang.management.ManagementFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

public class DFSClientMetrics implements Updater {
  public MetricsRegistry registry = new MetricsRegistry();
  public MetricsTimeVaryingRate lsLatency = new MetricsTimeVaryingRate(
      "client.ls.latency", registry,
      "The time taken by DFSClient to perform listStatus");
  public MetricsTimeVaryingLong readsFromLocalFile = new MetricsTimeVaryingLong(
      "client.read.localfile", registry,
      "The number of time read is fetched directly from local file.");
  public MetricsTimeVaryingRate preadLatency = new MetricsTimeVaryingRate(
      "client.pread.latency", registry,
      "The elapsed time taken by DFSClient to perform preads");
  public MetricsTimeVaryingRate preadSize = new MetricsTimeVaryingRate(
      "client.pread.size", registry,
      "The amount of data in bytes read by DFSClient via preads");
  public MetricsTimeVaryingRate preadCpu = new MetricsTimeVaryingRate(
      "client.pread.cputimenanos", registry,
      "The cputime(nanosec) taken by DFSClient to perform preads");
  public MetricsTimeVaryingRate readLatency = new MetricsTimeVaryingRate(
      "client.read.latency", registry,
      "The elapsed time taken by DFSClient to perform reads");
  public MetricsTimeVaryingRate readSize = new MetricsTimeVaryingRate(
      "client.read.size", registry,
      "The amount of data in bytes read by DFSClient via reads");
  public MetricsTimeVaryingRate readCpu = new MetricsTimeVaryingRate(
      "client.read.cputimenanos", registry,
      "The cputime(nanosec) taken by DFSClient to perform reads");
  public MetricsTimeVaryingRate syncLatency = new MetricsTimeVaryingRate(
      "client.sync.latency", registry,
      "The amount of elapsed time for syncs.");

  private long numLsCalls = 0;
  private static Log log = LogFactory.getLog(DFSClientMetrics.class);
  final MetricsRecord metricsRecord;
  private ThreadMXBean mxbean;

  public DFSClientMetrics() {
    // Create a record for FSNamesystem metrics
    MetricsContext metricsContext = MetricsUtil.getContext("hdfsclient");
    metricsRecord = MetricsUtil.createRecord(metricsContext, "DFSClient");
    metricsContext.registerUpdater(this);

    // get a handle to a JVM bean
    mxbean = ManagementFactory.getThreadMXBean();
  }

  public synchronized void incLsCalls() {
    numLsCalls++;
  }

  public synchronized void incPreadTime(long value) {
    preadLatency.inc(value);
  }

  public synchronized void incPreadSize(long value) {
    preadSize.inc(value);
  }

  public synchronized void incPreadCpu(long value) {
    preadCpu.inc(value);
  }

  public synchronized void incReadTime(long value) {
    readLatency.inc(value);
  }

  public synchronized void incReadSize(long value) {
    readSize.inc(value);
  }

  public synchronized void incReadCpu(long value) {
    readCpu.inc(value);
  }

  public synchronized void incSyncTime(long value) {
    syncLatency.inc(value);
  }

  public synchronized long getAndResetLsCalls() {
    long ret = numLsCalls;
    numLsCalls = 0;
    return ret;
  }

  /**
   * returns the current accumulated CPU time 
   * in nanoseconds for this thread
   */
  public long getCurrentThreadCpuTime() {
    // return mxbean.getCurrentThreadCpuTime();
    return 0;
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.setMetric("client.ls.calls", getAndResetLsCalls());
    metricsRecord.update();
  }
}
