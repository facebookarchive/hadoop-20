/**
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
package org.apache.hadoop.util;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

/**
 * FlushableLogger can be used for logging messages that should be visible in
 * the system logs immediately. It exposes Log interface, however the calls are
 * much more expensive than simply calling a raw Log. This logger should only be
 * used in non-critical paths!
 * 
 * This implementation causes logs to be flushed when log4j.properties sets
 * bufferedIO option to true. If the option is set to false, this implementation
 * does not change any behavior of logging.
 */
public class FlushableLogger implements Log {

  private final static Log LOG = LogFactory.getLog(FlushableLogger.class);

  // log being wrapped
  private final Log log;
  
  /**
   * Wrap an instance of Log with a log whose messages are always immediately
   * flushed. Warning: Calls to this logger are much more expensive than to the
   * raw logger.
   */
  public static Log getLogger(Log log) {
    return new FlushableLogger(log);
  }

  private FlushableLogger(Log log) {
    if (log == null) {
      String msg = "Log should not be null";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    this.log = log;
  }

  /**
   * Set immediateFlush property for all file appenders.
   */
  private synchronized void setFlush(boolean immediateFlush) {
    try {
      Set<FileAppender> flushedFileAppenders = new HashSet<FileAppender>();
      Enumeration<?> currentLoggers = LogManager.getLoggerRepository()
          .getCurrentLoggers();
      while (currentLoggers.hasMoreElements()) {
        Object nextLogger = currentLoggers.nextElement();
        if (nextLogger instanceof Logger) {
          Logger currentLogger = (Logger) nextLogger;
          Enumeration<?> allAppenders = currentLogger.getParent()
              .getAllAppenders();
          while (allAppenders.hasMoreElements()) {
            Object nextElement = allAppenders.nextElement();
            if (nextElement instanceof FileAppender) {
              FileAppender fileAppender = (FileAppender) nextElement;

              if (!flushedFileAppenders.contains(fileAppender)) {
                flushedFileAppenders.add(fileAppender);
                fileAppender.setImmediateFlush(immediateFlush);
              }
            }
          }
        }
      }
    } catch (Throwable e) {
      LOG.error("Failed flushing logs", e);
    }
  }
  
  // Log interface

  @Override
  public void debug(Object arg0) {
    debug(arg0, null);
  }

  @Override
  public void debug(Object arg0, Throwable arg1) {
    setFlush(true);
    log.debug(arg0, arg1);
    setFlush(false);
  }

  @Override
  public void error(Object arg0) {
    error(arg0, null);
  }

  @Override
  public void error(Object arg0, Throwable arg1) {
    setFlush(true);
    log.error(arg0, arg1);
    setFlush(false);
  }

  @Override
  public void fatal(Object arg0) {
    fatal(arg0, null);
  }

  @Override
  public void fatal(Object arg0, Throwable arg1) {
    setFlush(true);
    log.fatal(arg0, arg1);
    setFlush(false);
  }

  @Override
  public void info(Object arg0) {
    info(arg0, null);
  }

  @Override
  public void info(Object arg0, Throwable arg1) {
    setFlush(true);
    log.info(arg0, arg1);
    setFlush(false);
  }

  @Override
  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  @Override
  public boolean isErrorEnabled() {
    return log.isErrorEnabled();
  }

  @Override
  public boolean isFatalEnabled() {
    return log.isErrorEnabled();
  }

  @Override
  public boolean isInfoEnabled() {
    return log.isInfoEnabled();
  }

  @Override
  public boolean isTraceEnabled() {
    return log.isTraceEnabled();
  }

  @Override
  public boolean isWarnEnabled() {
    return log.isWarnEnabled();
  }

  @Override
  public void trace(Object arg0) {
    trace(arg0, null);
  }

  @Override
  public void trace(Object arg0, Throwable arg1) {
    setFlush(true);
    log.trace(arg0, arg1);
    setFlush(false);
  }

  @Override
  public void warn(Object arg0) {
    warn(arg0, null);
  }

  @Override
  public void warn(Object arg0, Throwable arg1) {
    setFlush(true);
    log.warn(arg0, arg1);
    setFlush(false);
  }
}
