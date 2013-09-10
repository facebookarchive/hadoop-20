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
package org.apache.hadoop.hdfs.server.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Logger;

/**
 * This class wraps any class implementing Log interface so that on each error()
 * and warn() call a corresponding function in subscribing ErrorCounter object
 * is invoked.
 */
public class CountingLogger implements Log {
  private final Log log;
  private ErrorCounter counter;

  public CountingLogger(Log log) {
    if (log == null) {
      throw new NullPointerException();
    }
    this.log = log;
  }

  @Override
  public void debug(Object arg0) {
    log.debug(arg0);
  }

  @Override
  public void debug(Object arg0, Throwable arg1) {
    log.debug(arg0, arg1);
  }

  @Override
  public synchronized void error(Object arg0) {
    if (counter != null) {
      counter.errorInc();
    }
    log.error(arg0);
  }

  @Override
  public synchronized void error(Object arg0, Throwable arg1) {
    if (counter != null) {
      counter.errorInc();
    }
    log.error(arg0, arg1);
  }

  @Override
  public void fatal(Object arg0) {
    log.fatal(arg0);
  }

  @Override
  public void fatal(Object arg0, Throwable arg1) {
    log.fatal(arg0, arg1);
  }

  @Override
  public void info(Object arg0) {
    log.info(arg0);
  }

  @Override
  public void info(Object arg0, Throwable arg1) {
    log.info(arg0, arg1);
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
    return log.isFatalEnabled();
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
    log.trace(arg0);
  }

  @Override
  public void trace(Object arg0, Throwable arg1) {
    log.trace(arg0, arg1);
  }

  @Override
  public synchronized void warn(Object arg0) {
    if (counter != null) {
      counter.warnInc();
    }
    log.warn(arg0);
  }

  @Override
  public synchronized void warn(Object arg0, Throwable arg1) {
    if (counter != null) {
      counter.warnInc();
    }
    log.warn(arg0, arg1);
  }

  public synchronized void setCounter(ErrorCounter counter) {
    this.counter = counter;
  }

  public Logger getLogger() {
    return ((Log4JLogger) log).getLogger();
  }

  /**
   * Interface for counting failures of different types. An object implementing it is responsible
   * for maintaining counters for events of each type.
   */
  public interface ErrorCounter {
    /** Invoked each time a error() of corresponding logger is called */
    public void errorInc();

    /** Invoked each time a warn() of corresponding logger is called */
    public void warnInc();
  }
}
