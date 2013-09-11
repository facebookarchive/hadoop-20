package org.apache.hadoop.hdfs.notifier.server.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

public class NamespaceNotifierMetrics implements Updater {
	public static final Log LOG = 
	    LogFactory.getLog(NamespaceNotifierMetrics.class);
  private MetricsRecord metricsRecord;
  private NamespaceNotifierActivityMBean notifierActivityMBean;
  public MetricsRegistry registry = new MetricsRegistry();
  
  // ServerCore metrics
  public MetricsIntValue numRegisteredClients =
      new MetricsIntValue("num_registered_clients", registry,
          "The number of clients currently registered on the server.");
  public MetricsLongValue numTotalSubscriptions =
      new MetricsLongValue("num_total_subscriptions", registry,
          "The current number of subscriptions done by all the clients");
  public MetricsLongValue queuedNotifications =
      new MetricsLongValue("queued_notifications", registry,
          "The number of notifications which are currently queued to be sent");
  
  // ServerDispatcher metrics
  public MetricsTimeVaryingLong failedClients =
      new MetricsTimeVaryingLong("failed_clients", registry,
          "The number of clients which are considered failed and removed.");
  public MetricsTimeVaryingLong markedClients =
      new MetricsTimeVaryingLong("marked_clients", registry,
          "The number of clients for which the most recent try to send a " +
          "notification failed.");
  public MetricsTimeVaryingLong dispatchedNotifications =
      new MetricsTimeVaryingLong("dispatched_notifications", registry,
          "The number of notifications succesfully dispatched.");
  public MetricsTimeVaryingLong failedNotifications =
      new MetricsTimeVaryingLong("failed_notifications", registry,
          "The number of notifications that failed to be dispatched");
  public MetricsTimeVaryingLong heartbeats =
      new MetricsTimeVaryingLong("heartbeats", registry,
          "The number of heartbeats sent (should be low).");
  public MetricsTimeVaryingRate dispatchNotificationRate =
      new MetricsTimeVaryingRate("dispatch_notification_rate", registry,
          "The rate with which the notifications are dispatched.");

  // ServerHandlerImpl metrics
  public MetricsTimeVaryingLong subscribeCalls =
      new MetricsTimeVaryingLong("subscribe_calls", registry,
          "The number of Thrift subscribe() calls");
  public MetricsTimeVaryingLong unsubscribeCalls =
      new MetricsTimeVaryingLong("unsubscribe_calls", registry,
          "The number of Thrift unsubscribe() calls");
  public MetricsTimeVaryingLong registerClientCalls =
      new MetricsTimeVaryingLong("register_client_calls", registry,
          "The number of Thrift registerClient() calls");
  public MetricsTimeVaryingLong unregisterClientCalls =
      new MetricsTimeVaryingLong("unregister_client_calls", registry,
          "The number of Thrift unregisterClient() calls");
  
  // ServerHistory metrics
  public MetricsLongValue historySize =
      new MetricsLongValue("history_size", registry,
          "The number of notifications stored in the server history");
  public MetricsLongValue historyQueues =
      new MetricsLongValue("history_queues", registry,
          "The number of queues for each (path, type) tuple.");
  public MetricsTimeVaryingLong trashedHistoryNotifications = 
      new MetricsTimeVaryingLong("trashed_history_notifications", registry,
      		"The number of notifications deleted from the history, though " +
          "they didn't timed out");
  
  // ServerLogReader metrics
  public MetricsTimeVaryingLong readOperations =
      new MetricsTimeVaryingLong("read_operations", registry,
          "The number of operations sucessfully read from the edit log");
  public MetricsTimeVaryingLong readNotifications =
      new MetricsTimeVaryingLong("read_notifications", registry,
          "The notifications produced by the log reader.");
  public MetricsTimeVaryingLong reachedEditLogEnd =
      new MetricsTimeVaryingLong("reached_edit_log_end", registry,
          "The number of times the reader reached the end of the edits log");
 
  public NamespaceNotifierMetrics(Configuration conf, String serverId) {
    String sessionId = conf.get("session.id"); 
    JvmMetrics.init("NamespaceNotifier", sessionId);
    
    notifierActivityMBean = new NamespaceNotifierActivityMBean(registry,
        "" + serverId);
    
    MetricsContext context = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(context, "namespacenotifier");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);

    LOG.info("Initializing NamespaceNotifierMetrics using context object:" +
        context.getClass().getName() + " and record: " +
        metricsRecord.getClass().getCanonicalName());
  }
  
  
  public void shutdown() {
    if (notifierActivityMBean != null) 
      notifierActivityMBean.shutdown();
  }
  
  
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }
  
  
  public void resetAllMinMax() {
    dispatchNotificationRate.resetMinMax();
  }
}
