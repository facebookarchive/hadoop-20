/*
 * FileContext.java
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;

/**
 * Metrics context for writing metrics to a file.<p/>
 *
 * This class is configured by setting ContextFactory attributes which in turn
 * are usually configured through a properties file.  All the attributes are
 * prefixed by the contextName. For example, the properties file might contain:
 * <pre>
 * myContextName.fileName=/tmp/metrics.log
 * myContextName.period=5
 * </pre>
 */
public class FileContext extends AbstractMetricsContext {
    
  /* Configuration attribute names */
  protected static final String FILE_NAME_PROPERTY = "fileName";
  protected static final String PERIOD_PROPERTY = "period";
  protected static final String RECORD_DATE_PATTERN_PROPERTY = "record.datePattern";
  
  private static final String DEFAULT_RECORD_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss,SSS";
  private static final String FILE_SUFFIX_DATE_PATTERN = "yyyy-MM-dd";
    
  private static SimpleDateFormat recordDateFormat;
  private static SimpleDateFormat fileSuffixDateFormat;
  
  private String fileName = null;
  private File file = null;              // file for metrics to be written to
  private PrintWriter writer = null;
  private Calendar lastRecordDate = null;

  /** Creates a new instance of FileContext */
  public FileContext() {}
    
  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);

    fileName = getAttribute(FILE_NAME_PROPERTY);
    
    String recordDatePattern = getAttribute(RECORD_DATE_PATTERN_PROPERTY);
    if (recordDatePattern == null)
      recordDatePattern = DEFAULT_RECORD_DATE_PATTERN;
    recordDateFormat = new SimpleDateFormat(recordDatePattern);
    
    fileSuffixDateFormat = new SimpleDateFormat(FILE_SUFFIX_DATE_PATTERN);
    Calendar currentDate = Calendar.getInstance();
    if (fileName != null)
      file = new File(getFullFileName(currentDate));
    lastRecordDate = currentDate;
        
    parseAndSetPeriod(PERIOD_PROPERTY);
  }
  
  private String getFullFileName(Calendar calendar) {
    if (fileName == null) {
      return null;
    } else {
      String fullFileName = fileName + "." + fileSuffixDateFormat.format(calendar.getTime());
      return fullFileName;
    }
  }

  /**
   * Returns the configured file name, or null.
   */
  public String getFileName() {
    return fileName;
  }
    
  /**
   * Starts or restarts monitoring, by opening in append-mode, the
   * file specified by the <code>fileName</code> attribute,
   * if specified. Otherwise the data will be written to standard
   * output.
   */
  public void startMonitoring()
    throws IOException 
  {
    if (file == null) {
      writer = new PrintWriter(new BufferedOutputStream(System.out));
    } else {
      writer = new PrintWriter(new FileWriter(file, true));
    }
    super.startMonitoring();
  }
    
  /**
   * Stops monitoring, closing the file.
   * @see #close()
   */
  public void stopMonitoring() {
    super.stopMonitoring();
        
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }
    
  /**
   * Emits a metrics record to a file.
   */
  public void emitRecord(String contextName, String recordName, OutputRecord outRec) 
    throws IOException
  {
    Calendar currentDate = Calendar.getInstance();
    if (fileName != null) {
      if (currentDate.get(Calendar.DAY_OF_MONTH) != lastRecordDate.get(Calendar.DAY_OF_MONTH)) {
        // rotate to a new context file
        file = new File(getFullFileName(currentDate));
        
        if (writer != null)
          writer.close();
        writer = new PrintWriter(new FileWriter(file, true));
      }
    }
    writer.print(recordDateFormat.format(currentDate.getTime()));
    writer.print(" ");
    writer.print(contextName);
    writer.print(".");
    writer.print(recordName);
    String separator = ": ";
    for (String tagName : outRec.getTagNames()) {
      writer.print(separator);
      separator = ", ";
      writer.print(tagName);
      writer.print("=");
      writer.print(outRec.getTag(tagName));
    }
    for (String metricName : outRec.getMetricNames()) {
      writer.print(separator);
      separator = ", ";
      writer.print(metricName);
      writer.print("=");
      writer.print(outRec.getMetric(metricName));
    }
    writer.println();
    lastRecordDate = currentDate;
  }
    
  /**
   * Flushes the output writer, forcing updates to disk.
   */
  public void flush() {
    writer.flush();
  }
}
