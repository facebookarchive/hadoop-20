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

/**
 * Sequential number generator. Synchronized on method since we only have one variable
 * per instance. May need to consider other ways if we are going to add more variables to
 * this class.
 * 
 * This class is thread safe.
 */
public abstract class SequentialNumber {
  private long currentValue;

  /** Create a new instance with the given initial value. */
  protected SequentialNumber(final long initialValue) {
    currentValue = initialValue;
  }

  /** @return the current value. */
  public synchronized long getCurrentValue() {
    return currentValue;
  }

  /** Set current value. */
  public synchronized void setCurrentValue(final long value) {
    currentValue = value;
  }

  /** Increment and then return the next value. */
  public synchronized long nextValue() {
    return ++currentValue;
  }

  /** Skip to the new value. */
  public synchronized void skipTo(final long newValue) throws IllegalStateException {
    if (newValue < currentValue) {
      throw new IllegalStateException(
          "Cannot skip to less than the current value (="
          + currentValue + "), where newValue=" + newValue);
    }
    currentValue = newValue;
  }
  
  @Override
  public boolean equals(final Object that) {
    if (that == null || this.getClass() != that.getClass()) {
      return false;
    }
    final long thatValue = ((SequentialNumber)that).currentValue;
    return currentValue == thatValue;
  }
  
  @Override
  public int hashCode() {
    return (int)currentValue ^ (int)(currentValue >>> 32);
  }
}
