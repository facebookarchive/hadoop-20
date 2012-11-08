package org.apache.hadoop.hdfs.notifier.server.metrics;

import javax.management.ObjectName;

import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsDynamicMBeanBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;

public class NamespaceNotifierActivityMBean extends MetricsDynamicMBeanBase {
  final private ObjectName mbeanName;

  protected NamespaceNotifierActivityMBean(MetricsRegistry mr,
      String serverId) {
    super(mr, serverId);
    mbeanName = MBeanUtil.registerMBean("NamespaceNotifier",
        "NamespaceNotifierActivity", this);
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }
}
