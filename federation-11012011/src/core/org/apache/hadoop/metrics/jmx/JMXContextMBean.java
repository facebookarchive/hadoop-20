package org.apache.hadoop.metrics.jmx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;

import org.apache.hadoop.metrics.spi.OutputRecord;

public class JMXContextMBean implements DynamicMBean {
  
  private Map<String, Number> metrics = new HashMap<String, Number>();
  private MBeanInfo mbeanInfo;
  private String recordName;

  public JMXContextMBean(String recordName) {
    this.recordName = recordName;
  }

  private synchronized MBeanInfo generateInfo() {
    List<MBeanAttributeInfo> attributesInfo = new ArrayList<MBeanAttributeInfo>();

    if (mbeanInfo != null && 
        mbeanInfo.getAttributes().length == metrics.entrySet().size()) {
      // bean information did not change yet
      return mbeanInfo;
    }

    for (Map.Entry<String, Number> metric : metrics.entrySet()) {
      String type = getType(metric.getValue());
      if (type == null) {
        continue;
      }
      attributesInfo.add(new MBeanAttributeInfo(metric.getKey(), type, 
          metric.getKey(), true, false, false));
    }

    MBeanAttributeInfo[] attrArray = 
      new MBeanAttributeInfo[attributesInfo.size()];
    MBeanInfo info = new MBeanInfo(this.getClass().getName(), recordName,
        attributesInfo.toArray(attrArray), null, null, null);
    return info;
  }

  @Override
  public Object getAttribute(String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    Number result = metrics.get(attribute);
    if (result == null) {
      throw new AttributeNotFoundException(attribute);
    }
    return result;
  }
  
  public void processMetricsRecord(OutputRecord outRec) {

    for (String metricName : outRec.getMetricNames()) {
      Number metricValue = outRec.getMetric(metricName);
      metrics.put(metricName, metricValue);
    }
  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    if (attributes == null || attributes.length == 0)
      throw new IllegalArgumentException();

    AttributeList result = new AttributeList();
    for (String attributeName : attributes) {
      Object value;
      try {
        value = getAttribute(attributeName);
        result.add(new Attribute(attributeName, value));
      } catch (Exception e) {
        continue;
      }
    }
    return result;
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    if (mbeanInfo == null) {
      mbeanInfo = generateInfo();
    }
    return mbeanInfo;
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature)
      throws MBeanException, ReflectionException {
    throw new IllegalArgumentException();
  }

  @Override
  public void setAttribute(Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException,
      MBeanException, ReflectionException {
    throw new ReflectionException(new NoSuchMethodException("set" + attribute));
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    return null;
  }
  
  private String getType(Number number) {
    return number.getClass().getCanonicalName();
  }
  
  public void flush() {
    mbeanInfo = generateInfo();
  }
}
