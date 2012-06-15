package org.apache.hadoop.metrics.jmx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.MetricsRecordImpl;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.util.MBeanUtil;

public class JMXContext extends AbstractMetricsContext {

  public static final String JMX_RECORDS = "jmx_records";
  protected static final String PERIOD_PROPERTY = "period";

  private Map<String, JMXContextMBean> JMXBeans = 
    new HashMap<String, JMXContextMBean>();
  private Map<JMXContextMBean, ObjectName> beanHandles = 
    new HashMap<JMXContextMBean, ObjectName>();
  private List<String> records = new ArrayList<String>();

  public JMXContext() {

  }
  
  private void initAllowedRecords() {
    String recordsList = getAttribute(JMX_RECORDS);
    if (recordsList != null) {
      String[] recordNames = recordsList.split(",");
      for (String record : recordNames) {
        records.add(record);
      }
    }
  }

  @Override
  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);
    initAllowedRecords();
    
    String periodStr = getAttribute(PERIOD_PROPERTY);
    if (periodStr != null) {
      int period = 0;
      try {
        period = Integer.parseInt(periodStr);
      } catch (NumberFormatException nfe) {
      }
      if (period <= 0) {
        throw new MetricsException("Invalid period: " + periodStr);
      }
      setPeriod(period);
    }
  }

  
  @Override
  protected MetricsRecord newRecord(String recordName) {
    MetricsRecord record = super.newRecord(recordName);
    if (records.isEmpty() || records.contains(recordName)) {
      // Create MBean to expose this record
      // Only if this record is to be exposed through JMX
      getOrCreateMBean(recordName);
    }

    return record;
  }

  private synchronized JMXContextMBean getOrCreateMBean(String recordName) {
    JMXContextMBean bean = JMXBeans.get(recordName);
    if (bean == null) {
      bean = new JMXContextMBean(recordName);
      JMXBeans.put(recordName, bean);
      if (isMonitoring()) {
        ObjectName registeredName = 
          MBeanUtil.registerMBean(getContextName(), recordName, bean);
        beanHandles.put(bean, registeredName);
      }
    }
    return bean;
  }

  @Override
  protected void remove(MetricsRecordImpl record) {
    super.remove(record);
    
    String recordName = record.getRecordName();
    
    JMXContextMBean bean = JMXBeans.remove(recordName);
    if (bean == null) {
      return;
    }
    // Currently - one bean per record, so remove the bean
    ObjectName name = beanHandles.remove(bean);
    MBeanUtil.unregisterMBean(name);
  }

  @Override
  public synchronized void startMonitoring() throws IOException {
    for (Map.Entry<String, JMXContextMBean> beanEntry : JMXBeans.entrySet()) {
      ObjectName registeredName = MBeanUtil.registerMBean(getContextName(),
                              beanEntry.getKey(),
                              beanEntry.getValue());
      beanHandles.put(beanEntry.getValue(), registeredName);
    }
    super.startMonitoring();
  }

  @Override
  public synchronized void stopMonitoring() {
    for (ObjectName name : beanHandles.values()) {
      MBeanUtil.unregisterMBean(name);
    }
    beanHandles.clear();
    super.stopMonitoring();
  }

  @Override
  protected void emitRecord(String contextName, String recordName,
      OutputRecord outRec) throws IOException {
    JMXContextMBean bean = JMXBeans.get(recordName);
    if (bean != null) {
      bean.processMetricsRecord(outRec);
    }
  }

  @Override
  protected void flush() throws IOException {
    for (JMXContextMBean bean : beanHandles.keySet()) {
      bean.flush();
    }
  }
}
