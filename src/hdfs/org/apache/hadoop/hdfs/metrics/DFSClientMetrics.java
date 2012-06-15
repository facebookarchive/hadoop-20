package org.apache.hadoop.hdfs.metrics;

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



	private long numLsCalls = 0;
	private static Log log = LogFactory.getLog(DFSClientMetrics.class);
	final MetricsRecord metricsRecord;

	// create a singleton DFSClientMetrics 
	private static DFSClientMetrics metrics;

	public DFSClientMetrics() {
		// Create a record for FSNamesystem metrics
		MetricsContext metricsContext = MetricsUtil.getContext("hdfsclient");
		metricsRecord = MetricsUtil.createRecord(metricsContext, "DFSClient");
		metricsContext.registerUpdater(this);
	
	}


	public Object clone() throws CloneNotSupportedException{
		throw new CloneNotSupportedException();
	}

	public synchronized void incLsCalls() {
		numLsCalls++;
	}
	
	public synchronized void incReadsFromLocalFile() {
		readsFromLocalFile.inc();
	}

	public synchronized void incPreadTime(long value) {
		preadLatency.inc(value);
	}

	public synchronized void incPreadSize(long value) {
		preadSize.inc(value);
	}

	public synchronized void incPreadOps(){
		preadOps.inc();
	}
	
	public synchronized void incReadTime(long value) {
		readLatency.inc(value);
	}

	public synchronized void incReadSize(long value) {
		readSize.inc(value);
	}
	
	public synchronized void incReadOps(){
		readOps.inc();
	}

	public synchronized void incSyncTime(long value) {
		syncLatency.inc(value);
	}
	
	public synchronized void incWriteSize(long value){
		writeSize.inc(value);
		
	}

	public synchronized void incWriteOps(){
		writeOps.inc();
	}
	
	public synchronized void incNumCreateFileOps(){
		numCreateFileOps.inc();
	}

	public synchronized void incNumCreateDirOps(){
		numCreateDirOps.inc();
	}
	
	private synchronized long getAndResetLsCalls() {
		long ret = numLsCalls;
		numLsCalls = 0;
		return ret;
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
