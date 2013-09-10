package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.CoronaTaskTracker.JobTrackerReporter;
import org.apache.hadoop.mapred.SortedRanges.Range;

/**
 * TaskUmbilicalProtocol used by Task in Corona
 * In Corona we allow each individual Task to talk to JobTracker directly.
 */
class CoronaDirectTaskUmbilical implements TaskUmbilicalProtocol {

  public static final Log LOG = LogFactory.getLog(CoronaDirectTaskUmbilical.class);

  final private TaskUmbilicalProtocol taskTrackerUmbilical;
  private InterTrackerProtocol jobTracker;
  final private List<TaskCompletionEvent> mapEventFetched;
  private int totalEventsFetched = 0;
  /** Conf entry for direct umbilical address */
  public static final String DIRECT_UMBILICAL_JT_ADDRESS =
      "mapred.direct.task.umbilical.address";
  /** Conf entry for secondary tracker addres */
  public static final String DIRECT_UMBILICAL_FALLBACK_ADDRESS =
      "mapred.direct.task.umbilical.secondary";
  
  // member used to handle IO exception
  final int maxErrorCount = 10;
  int errorCount = 0;
  long jtCallTimeout;

  /**
   * Deserialize InetSocketAddress from String from given key in conf
   * @param conf job conf
   * @param key name of entry
   * @return address
   */
  public static InetSocketAddress getAddress(JobConf conf, String key) {
    String str = conf.get(key);
    if (str == null)
      return null;
    String hostPortPair[] = str.split(":");
    if (hostPortPair.length != 2)
      return null;
    return new InetSocketAddress(hostPortPair[0],
        Integer.parseInt(hostPortPair[1]));
  }

  /**
   * Serialize InetSocketAddress to String and saves in given conf
   * @param conf job conf
   * @param key name of entry
   * @param address address to save
   */
  public static void setAddress(JobConf conf, String key,
      InetSocketAddress address) {
    if (address == null) {
      conf.unset(key);
      return;
    }
    String addrStr = address.getHostName() + ":" + address.getPort();
    conf.set(key, addrStr);
  }

  /** Job tracker timeout */
  private static long jtConnectTimeoutMsec;

  /** Job configuration */
  private final JobConf conf;
  /** Secondary fallback address */
  private final InetSocketAddress secondaryTracker;
  /** Address of current JT */
  private InetSocketAddress currentTracker;
  /** Mutable pointer to handle closing RPC client outside of this class */
  private final VersionedProtocolPointer clientPointer;
  
  public static CoronaDirectTaskUmbilical createDirectUmbilical(
      TaskUmbilicalProtocol taskTracker, InetSocketAddress jobTrackerAddress,
      InetSocketAddress secondaryTrackerAddress, JobConf conf)
      throws IOException {

    LOG.info("Creating direct umbilical to " + jobTrackerAddress.toString());
    jtConnectTimeoutMsec = conf.getLong(
      "corona.jobtracker.connect.timeout.msec", 60000L);

    return new CoronaDirectTaskUmbilical(taskTracker, conf,
        jobTrackerAddress, secondaryTrackerAddress);
  }

  public List<VersionedProtocolPointer> getCreatedProxies() {
    return Collections.singletonList(clientPointer);
  }

  public void close() {
    RPC.stopProxy(jobTracker);
  }

  private CoronaDirectTaskUmbilical(TaskUmbilicalProtocol taskTrackerUmbilical,
      JobConf conf, InetSocketAddress currentTracker,
      InetSocketAddress secondaryTracker) throws IOException {
    this.taskTrackerUmbilical = taskTrackerUmbilical;
    this.mapEventFetched = new ArrayList<TaskCompletionEvent>();
    this.conf = conf;
    this.currentTracker = currentTracker;
    this.secondaryTracker = secondaryTracker;
    this.clientPointer = new VersionedProtocolPointer();
    this.jtCallTimeout = conf.getLong("corona.jt.call.timeout", 1000L);

    boolean succeeded = (new Caller<Boolean>() {
      /** Connect if this is first try, new tries reconnects will be handled by
       * caller fall back mechanism */
      private boolean firstTry = true;

      @Override
      protected Boolean call() throws IOException {
        if (firstTry) {
          firstTry = false;
          connect(CoronaDirectTaskUmbilical.this.currentTracker);
        }
        return true;
      }

    }).makeCall();

    if (!succeeded) {
      throw new IOException("Failed to initialize DirectTaskUmbilical client");
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws VersionIncompatible, IOException {
    return taskTrackerUmbilical.getProtocolVersion(protocol, clientVersion);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return taskTrackerUmbilical.getProtocolSignature(
        protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public JvmTask getTask(JvmContext context) throws IOException {
    return taskTrackerUmbilical.getTask(context);
  }

  @Override
  public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    return taskTrackerUmbilical.statusUpdate(taskId, taskStatus);
  }

  @Override
  public void reportDiagnosticInfo(TaskAttemptID taskid, String trace)
      throws IOException {
    taskTrackerUmbilical.reportDiagnosticInfo(taskid, trace);
  }

  @Override
  public void reportNextRecordRange(TaskAttemptID taskid, Range range)
      throws IOException {
    taskTrackerUmbilical.reportNextRecordRange(taskid, range);
  }

  @Override
  public boolean ping(TaskAttemptID taskid) throws IOException {
    return taskTrackerUmbilical.ping(taskid);
  }

  @Override
  public void done(TaskAttemptID taskid) throws IOException {
    taskTrackerUmbilical.done(taskid);
  }

  @Override
  public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    taskTrackerUmbilical.commitPending(taskId, taskStatus);
  }

  @Override
  public boolean canCommit(TaskAttemptID taskid) throws IOException {
    return taskTrackerUmbilical.canCommit(taskid);
  }

  @Override
  public void shuffleError(TaskAttemptID taskId, String message)
      throws IOException {
    taskTrackerUmbilical.shuffleError(taskId, message);
  }

  @Override
  public void fsError(TaskAttemptID taskId, String message) throws IOException {
    taskTrackerUmbilical.fsError(taskId, message);
  }

  @Override
  public void fatalError(TaskAttemptID taskId, String message)
      throws IOException {
    taskTrackerUmbilical.fatalError(taskId, message);
  }

  @Override
  public MapTaskCompletionEventsUpdate getMapCompletionEvents(final JobID jobId,
      int fromIndex, int maxLocs, TaskAttemptID id) throws IOException {
    // Remember old address to notice change
    InetSocketAddress oldTrackerAddr = currentTracker;

    TaskCompletionEvent[] recentEvents = (new Caller<TaskCompletionEvent[]>() {
      @Override
      protected TaskCompletionEvent[] call() throws IOException {
        return jobTracker.getTaskCompletionEvents(jobId, totalEventsFetched,
            Integer.MAX_VALUE);
      }
    }).makeCall();

    // Check if we've changed JobTracker
    if (!oldTrackerAddr.equals(currentTracker)) {
      // if so reset counter for all TaskCompletionEvents and send reset flag
      LOG.info("JobTracker did a failover from " + oldTrackerAddr  + " to " + 
          currentTracker);
      totalEventsFetched = 0;
      return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EMPTY_ARRAY,
          true);
    } else {
      // if not proceed as usual
      totalEventsFetched += recentEvents.length;
      for (TaskCompletionEvent event : recentEvents) {
        if (event.isMapTask()) {
          mapEventFetched.add(event);
        }
      }
      int toIndex = fromIndex + maxLocs;
      toIndex = toIndex > mapEventFetched.size() ? mapEventFetched.size()
          : toIndex;
      TaskCompletionEvent[] result = mapEventFetched
          .subList(fromIndex, toIndex).toArray(
              new TaskCompletionEvent[toIndex - fromIndex]);
      return new MapTaskCompletionEventsUpdate(result, false);
    }
  }
  
  /**
   * Caller that automatically handles switching to new JT.
   * @param <T> function return type
   */
  private abstract class Caller<T> extends CoronaJTFallbackCaller<T> {
    
    @Override
    protected void handleIOException(IOException e) throws IOException {
      errorCount++;
      if (errorCount >= maxErrorCount) {
        LOG.error("Too many errors " + maxErrorCount +
          " in calling " + currentTracker, e);
        throw e;
      } else {
        long backoff = errorCount * jtCallTimeout;
        LOG.warn(
          "Error " + errorCount + " in calling to " + currentTracker +
          " will wait " + backoff + " msec", e);
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException ie) {
        }
      }
    }

    @Override
    protected JobConf getConf() {
      return CoronaDirectTaskUmbilical.this.conf;
    }

    @Override
    protected InetSocketAddress getSecondaryTracker() {
      return CoronaDirectTaskUmbilical.this.secondaryTracker;
    }

    @Override
    protected InetSocketAddress getCurrentClientAddress() {
      return CoronaDirectTaskUmbilical.this.currentTracker;
    }

    @Override
    protected void connect(InetSocketAddress newAddress) throws IOException {
      int rpcTimeout = (int) jtConnectTimeoutMsec;

      CoronaDirectTaskUmbilical.this.currentTracker = newAddress;
      jobTracker = RPC.waitForProxy(InterTrackerProtocol.class,
          InterTrackerProtocol.versionID, newAddress, conf,
          jtConnectTimeoutMsec, rpcTimeout);
      clientPointer.setClient(jobTracker);
    }

    @Override
    protected void shutdown() {
      RPC.stopProxy(jobTracker);
    }

  }

  /**
   * Encapsulates VersionedProtocol in mutable object for closing RPC clients
   * that has changes
   */
  public static class VersionedProtocolPointer {
    /** RPC client */
    private VersionedProtocol client;

    /**
     * Default c'tor
     */
    public VersionedProtocolPointer() {
    }

    /**
     * Init c'tor
     */
    public VersionedProtocolPointer(VersionedProtocol client) {
      this.client = client;
    }

    public VersionedProtocol getClient() {
      return client;
    }

    public void setClient(VersionedProtocol client) {
      this.client = client;
    }

  }
  
}
