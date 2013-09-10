package org.apache.hadoop.hdfs.metrics;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSClient;
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
	public MetricsTimeVaryingLong preadSize = new MetricsTimeVaryingLong(
			"client.pread.size", registry,
	"The amount of data in bytes read by DFSClient via preads");
	public MetricsTimeVaryingLong preadOps = new MetricsTimeVaryingLong(
			"client.pread.operations", registry,
	"The number of pread operation in DFSInputStream");
	public MetricsTimeVaryingRate readLatency = new MetricsTimeVaryingRate(
			"client.read.latency", registry,
	"The elapsed time taken by DFSClient to perform reads");
	public MetricsTimeVaryingLong readSize = new MetricsTimeVaryingLong(
			"client.read.size", registry,
	"The amount of data in bytes read by DFSClient via reads");
	public MetricsTimeVaryingLong readOps = new MetricsTimeVaryingLong(
			"client.read.operations", registry,
	"The number of read operation in DFSInputStream");
	public MetricsTimeVaryingRate syncLatency = new MetricsTimeVaryingRate(
			"client.sync.latency", registry,
	"The amount of elapsed time for syncs.");
	public MetricsTimeVaryingLong writeSize = new MetricsTimeVaryingLong(
			"client.write.size", registry,
	"The amount of data in byte write by DFSClient via writes");
	public MetricsTimeVaryingLong writeOps = new MetricsTimeVaryingLong(
			"client.write.operations", registry,
	"The total number of create and append operations");
	public MetricsTimeVaryingLong numCreateFileOps = new MetricsTimeVaryingLong(
			"client.create.file.operation", registry,
	"The number of creating file operations called by DFSClient");
	public MetricsTimeVaryingLong numCreateDirOps = new MetricsTimeVaryingLong(
			"client.create.directory.operation", registry,
	"The number of creating directory operations called by DFSClient");



	private AtomicLong numLsCalls = new AtomicLong(0);
	private static Log log = LogFactory.getLog(DFSClientMetrics.class);
	final MetricsRecord metricsRecord;
	private boolean enabled = false;

	// create a singleton DFSClientMetrics 
	private static DFSClientMetrics metrics;

	public DFSClientMetrics(boolean enabled) {
		// Create a record for FSNamesystem metrics
		MetricsContext metricsContext = MetricsUtil.getContext("hdfsclient");
		metricsRecord = MetricsUtil.createRecord(metricsContext, "DFSClient");
		metricsContext.registerUpdater(this);
		this.enabled = enabled;
	
	}


	public Object clone() throws CloneNotSupportedException{
		throw new CloneNotSupportedException();
	}

	public void incLsCalls() {
	  if (enabled) {
	    numLsCalls.incrementAndGet();
	  }
	}

	public void incReadsFromLocalFile() {
	  if (enabled) {
	    readsFromLocalFile.inc();
	  }
	}

	public void incPreadTime(long value) {
    if (enabled) {
      preadLatency.inc(value);
    }
	}

	public void incPreadSize(long value) {
	   if (enabled) {
	     preadSize.inc(value);
	   }
	}

	public void incPreadOps(){
	   if (enabled) {
	     preadOps.inc();
	   }
	}

	public void incReadTime(long value) {
	  if (enabled) {
	    readLatency.inc(value);
	  }
	}

	public void incReadSize(long value) {
	  if (enabled) {
	    readSize.inc(value);
	  }
	}

	public void incReadOps(){
	  if (enabled) {
	    readOps.inc();
	  }
	}

	public void incSyncTime(long value) {
	  if (enabled) {
	    syncLatency.inc(value);
	  }
	}

	public void incWriteSize(long value){
    if (enabled) {
      writeSize.inc(value);
    }
	}

	public void incWriteOps(){
	  if (enabled) {
	    writeOps.inc();
	  }
	}

	public void incNumCreateFileOps(){
	  if (enabled) {
	    numCreateFileOps.inc();
	  }
	}

	public void incNumCreateDirOps(){
	  if (enabled) {
	    numCreateDirOps.inc();
	  }
	}

	private long getAndResetLsCalls() {
	  if (enabled) {
	    return numLsCalls.getAndSet(0);
	  } else {
	    return 0;
	  }
	}

	public void enable(boolean enabled) {
    this.enabled = enabled;
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
