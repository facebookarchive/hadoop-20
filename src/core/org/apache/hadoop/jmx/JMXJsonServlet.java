/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.jmx;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * Provides Read only web access to JMX.
 * <p/>
 * This servlet generally will be placed under the /jmx URL for each HttpServer.
 * It provides read only access to JMX metrics. The optional <code>qry</code>
 * parameter may be used to query only a subset of the JMX Beans. This query
 * functionality is provided through the
 * {@link MBeanServer#queryNames(ObjectName, javax.management.QueryExp)} method.
 * <p/>
 * For example <code>http://.../jmx?qry=Hadoop:*</code> will return all hadoop
 * metrics exposed through JMX.
 * <p/>
 * The optional <code>get</code> parameter is used to query an specific
 * attribute of a JMX bean. The format of the URL is
 * <code>http://.../jmx?get=MXBeanName::AttributeName<code>
 * <p/>
 * For example
 * <code>
 * http://../jmx?get=Hadoop:service=NameNode,name=NameNodeInfo::ClusterId
 * </code>
 * will return the cluster id of the namenode mxbean.
 * <p/>
 * If the <code>qry</code> or the <code>get</code> parameter is not formatted
 * correctly then a 400 BAD REQUEST http response code will be returned.
 * <p/>
 * If a resource such as a mbean or attribute can not be found, a 404
 * SC_NOT_FOUND http response code will be returned.
 * <p/>
 * The return format is JSON and in the form
 * <p/>
 * <code><pre>
 *  {
 *    "(bean name)" :
 *      {
 *        "attribute": (value)
 *        ...
 *      }
 *    ...
 *  }
 *  </pre></code>
 * <p/>
 * The servlet attempts to convert the the JMXBeans into JSON. Each bean's
 * attributes will be converted to a JSON object member.
 * <p/>
 * If the attribute is a boolean, a number, a string, or an array it will be
 * converted to the JSON equivalent.
 * <p/>
 * If the value is a {@link CompositeData} then it will be converted to a JSON
 * object with the keys as the name of the JSON member and the value is
 * converted following these same rules.
 * <p/>
 * If the value is a {@link TabularData} then it will be converted to an array
 * of the {@link CompositeData} elements that it contains.
 * <p/>
 * All other objects will be converted to a string and output as such.
 * <p/>
 * The bean's name and modelerType will be returned for all beans.
 * <p/>
 * Optional parameter "callback" should be used to deliver JSONP response.
 */
public class JMXJsonServlet extends HttpServlet {
  private static final Log LOG = LogFactory.getLog(JMXJsonServlet.class);
  private static final long serialVersionUID = 1L;
  private static final String CALLBACK_PARAM = "callback";
  /**
   * MBean server.
   */
  protected transient MBeanServer mBeanServer;
  /**
   * Json Factory to create Json generators for write objects in json format
   */
  protected transient JsonFactory jsonFactory;

  /**
   * Initialize this servlet.
   */
  @Override
  public void init() throws ServletException {
    // Retrieve the MBean server
    mBeanServer = ManagementFactory.getPlatformMBeanServer();
    jsonFactory = new JsonFactory();
  }

  /**
   * Process a GET request for the specified resource.
   * 
   * @param request
   *          The servlet request we are processing
   * @param response
   *          The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    JsonGenerator jg = null;
    String jsonpcb = null;
    PrintWriter writer = null;
    try {

      writer = response.getWriter();

      // "callback" parameter implies JSONP output
      jsonpcb = request.getParameter(CALLBACK_PARAM);
      if (jsonpcb != null) {
        response.setContentType("application/javascript; charset=utf8");
        writer.write(jsonpcb + "(");
      } else {
        response.setContentType("application/json; charset=utf8");
      }

      jg = jsonFactory.createJsonGenerator(writer);
      jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      jg.useDefaultPrettyPrinter();

      int statusCode = renderMBeans(jg, request.getParameterValues("qry"));
      response.setStatus(statusCode);

    } catch (IOException e) {
      writeException(jg, e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (MalformedObjectNameException e) {
      writeException(jg, e);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    } finally {
      if (jg != null) {
        jg.close();
      }
      if (jsonpcb != null) {
        writer.write(");");
      }
      if (writer != null) {
        writer.close();
      }
    }

  }

  /**
   * Renders MBean attributes to jg.
   * The queries parameter allows selection of a subset of mbeans.
   * 
   * @param jg
   *          JsonGenerator that will be written to
   * @param mBeanNames
   *          Optional list of mbean names to render. If null, every
   *          mbean will be returned.
   * @return int
   *         Returns the appropriate HTTP status code.
   */
  private int renderMBeans(JsonGenerator jg, String[] mBeanNames) throws IOException,
      MalformedObjectNameException {
    jg.writeStartObject();

    Set<ObjectName> nameQueries, queriedObjects;
    nameQueries = new HashSet<ObjectName>();
    queriedObjects = new HashSet<ObjectName>();

    // if no mbean names provided, add one null entry to query everything
    if (mBeanNames == null) {
      nameQueries.add(null);
    } else {
      for (String mBeanName : mBeanNames) {
        if (mBeanName != null) {
          nameQueries.add(new ObjectName(mBeanName));
        }
      }
    }

    // perform name queries
    for (ObjectName nameQuery : nameQueries) {
      queriedObjects.addAll(mBeanServer.queryNames(nameQuery, null));
    }

    // render each query result
    for (ObjectName objectName : queriedObjects) {
      renderMBean(jg, objectName);
    }

    jg.writeEndObject();
    return HttpServletResponse.SC_OK;
  }

  /**
   * Render a particular MBean's attributes to jg.
   * 
   * @param jg
   *          JsonGenerator that will be written to
   * @param objectName
   *          ObjectName for the mbean to render.
   * @return void
   */
  private void renderMBean(JsonGenerator jg, ObjectName objectName) throws IOException {
    MBeanInfo beanInfo;
    String className;

    jg.writeObjectFieldStart(objectName.toString());
    jg.writeStringField("beanName", objectName.toString());

    try {
      beanInfo = mBeanServer.getMBeanInfo(objectName);
      className = beanInfo.getClassName();

      // if we have the generic BaseModelMBean for className, attempt to get
      // more specific name
      if ("org.apache.commons.modeler.BaseModelMBean".equals(className)) {
        try {
          className = (String) mBeanServer.getAttribute(objectName, "modelerType");
        } catch (Exception e) {
          // it's fine if no more-particular name can be found
        }
      }

      jg.writeStringField("className", className);
      for (MBeanAttributeInfo attr : beanInfo.getAttributes()) {
        writeAttribute(jg, objectName, attr);
      }

    } catch (OperationsException e) {
      // Some general MBean exception occurred.
      writeException(jg, e);
    } catch (ReflectionException e) {
      // This happens when the code inside the JMX bean threw an exception, so
      // log it and don't output the bean.
      writeException(jg, e);
    }

    jg.writeEndObject();
  }

  private void writeException(JsonGenerator jg, Exception e) throws IOException {
    jg.writeStringField("exception", e.toString());
  }

  private void writeAttribute(JsonGenerator jg, ObjectName oname, MBeanAttributeInfo attr)
      throws IOException {
    if (!attr.isReadable()) {
      return;
    }
    String attName = attr.getName();
    if ("modelerType".equals(attName)) {
      return;
    }
    if (attName.contains("=") || attName.contains(":") || attName.contains(" ")) {
      return;
    }
    try {
      writeAttribute(jg, attName, mBeanServer.getAttribute(oname, attName));
    } catch (Exception e) {
      // UnsupportedOperationExceptions are common, report at lower log level
      writeException(jg, e);
    }
  }

  private void writeAttribute(JsonGenerator jg, String attName, Object value) throws IOException {
    jg.writeFieldName(attName);
    writeObject(jg, value);
  }

  private void writeObject(JsonGenerator jg, Object value) throws IOException {
    if (value == null) {
      jg.writeNull();
    } else {
      Class<?> c = value.getClass();
      if (c.isArray()) {
        jg.writeStartArray();
        int len = Array.getLength(value);
        for (int j = 0; j < len; j++) {
          Object item = Array.get(value, j);
          writeObject(jg, item);
        }
        jg.writeEndArray();
      } else if (value instanceof Number) {
        Number n = (Number) value;
        jg.writeNumber(n.toString());
      } else if (value instanceof Boolean) {
        Boolean b = (Boolean) value;
        jg.writeBoolean(b);
      } else if (value instanceof CompositeData) {
        CompositeData cds = (CompositeData) value;
        CompositeType comp = cds.getCompositeType();
        Set<String> keys = comp.keySet();
        jg.writeStartObject();
        for (String key : keys) {
          writeAttribute(jg, key, cds.get(key));
        }
        jg.writeEndObject();
      } else if (value instanceof TabularData) {
        TabularData tds = (TabularData) value;
        jg.writeStartArray();
        for (Object entry : tds.values()) {
          writeObject(jg, entry);
        }
        jg.writeEndArray();
      } else {
        jg.writeString(value.toString());
      }
    }
  }
}
