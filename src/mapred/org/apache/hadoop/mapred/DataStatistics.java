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

package org.apache.hadoop.mapred;

class DataStatistics{
  private int count = 0;
  private double sum = 0;
  private double sumSquares = 0;

  public DataStatistics() {
  }

  public DataStatistics(double initNum) {
    this.count = 1;
    this.sum = initNum;
    this.sumSquares = initNum * initNum;
  }

  synchronized public void add(double newNum) {
    this.count++;
    this.sum += newNum;
    this.sumSquares += newNum * newNum;
  }

  synchronized public void updateStatistics(double old, double update) {
    sub(old);
    add(update);
  }

  private void sub(double oldNum) {
    this.count--;
    this.sum = Math.max(this.sum -= oldNum, 0.0d);
    this.sumSquares = Math.max(this.sumSquares -= oldNum * oldNum, 0.0d);
  }

  public double mean() {
    return sum/count;
  }

  public double var() {
    // E(X^2) - E(X)^2
    return Math.max((sumSquares/count) - mean() * mean(), 0.0d);
  }

  public double std() {
    return Math.sqrt(this.var());
  }

  public String toString() {
    return "DataStatistics: count is " + count + ", sum is " + sum +
    ", sumSquares is " + sumSquares + " mean is " + mean() + " std() is " + std();
  }
}
