package org.apache.hadoop.hdfs;

import java.lang.reflect.Method;

import junit.framework.TestCase;

import org.apache.hadoop.hdfs.metrics.DFSClientMetrics;

public class TestDFSClientMetricsSync extends TestCase {

	private static final int THREAD_COUNT = 20;
	private static final int OPERATIONS_PER_THREAD = 100;
	private static final long PREAD_SIZE_VALUE = 3;
	private static final long PREAD_TIME_VALUE = 5;
	private static final long READ_TIME_VALUE = 7;
	private static final long SYNC_TIME_VALUE = 11;
	private static final long WRITE_SIZE_VALUE = 13;
	private static final long READ_SIZE_VALUE = 17;

	public void testSync() throws Exception{
		Thread[] threads = new Thread[THREAD_COUNT];
		final DFSClientMetrics metrics = new DFSClientMetrics(true);
		for(int i = 0; i < threads.length; i++) {
			threads[i] = new Thread(new Runnable() {

				@Override
				public void run() {
					for(int i = 0; i < OPERATIONS_PER_THREAD; i++) {
						metrics.incLsCalls();
						metrics.incNumCreateDirOps();
						metrics.incNumCreateFileOps();
						metrics.incPreadOps();
						metrics.incPreadSize(PREAD_SIZE_VALUE);
						metrics.incPreadTime(PREAD_TIME_VALUE);
						metrics.incReadOps();
						metrics.incReadsFromLocalFile();
						metrics.incReadSize(READ_SIZE_VALUE);
						metrics.incReadTime(READ_TIME_VALUE);
						metrics.incSyncTime(SYNC_TIME_VALUE);
						metrics.incWriteOps();
						metrics.incWriteSize(WRITE_SIZE_VALUE);
					}
				}
			});
		}
		for(int i = 0; i < threads.length; i++) {
			threads[i].start();
		}
		for(int i = 0; i < threads.length; i++) {
			threads[i].join();
		}

    // This is dependent to DFSClientMetrics private API; used to obtain value
    // of lsCalls.
    Method privateStringMethod = DFSClientMetrics.class
        .getDeclaredMethod("getAndResetLsCalls");
    privateStringMethod.setAccessible(true);

		long returnValue = (Long) privateStringMethod.invoke(metrics);
		assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD, returnValue);

    metrics.doUpdates(null);
		
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD,
        metrics.numCreateDirOps.getPreviousIntervalValue());
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD,
        metrics.numCreateFileOps.getPreviousIntervalValue());
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD,
        metrics.preadOps.getPreviousIntervalValue());
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD * PREAD_SIZE_VALUE,
        metrics.preadSize.getPreviousIntervalValue());
		assertEquals(PREAD_TIME_VALUE, metrics.preadLatency.getMinTime());
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD,
        metrics.readOps.getPreviousIntervalValue());
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD,
        metrics.readsFromLocalFile.getPreviousIntervalValue());
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD * READ_SIZE_VALUE,
        metrics.readSize.getPreviousIntervalValue());
		assertEquals(READ_TIME_VALUE, metrics.readLatency.getMinTime());
		assertEquals(SYNC_TIME_VALUE, metrics.syncLatency.getMinTime());
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD,
        metrics.writeOps.getPreviousIntervalValue());
    assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD * WRITE_SIZE_VALUE,
        metrics.writeSize.getPreviousIntervalValue());
	}
}
