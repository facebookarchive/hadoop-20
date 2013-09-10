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
package org.apache.hadoop.hdfs.storageservice.server;

import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.Request;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ClientProxyMetrics {
  private EventDurationCalculator requestProcessing = new EventDurationCalculator();
  private EventDurationCalculator namenodeRPC = new EventDurationCalculator();
  private EventDurationCalculator executorWaiting = new EventDurationCalculator();
  private File outFile;

  public ClientProxyMetrics() throws IOException {
    outFile = File.createTempFile("ClientProxyMetrics-", ".txt", new File("/tmp/"));
  }

  public void startProcessing(@SuppressWarnings("unused") Request request) {
    requestProcessing.eventStarted();
  }

  public void executorSubmit(@SuppressWarnings("unused") Request request) {
    executorWaiting.eventStarted();
  }

  public void executorCall(@SuppressWarnings("unused") Request request) {
    executorWaiting.eventEnded();
  }

  public void namenodeCalled(@SuppressWarnings("unused") Request request) {
    namenodeRPC.eventStarted();
  }

  public void namenodeReturned(@SuppressWarnings("unused") Request request) {
    namenodeRPC.eventEnded();
  }

  public void endProcessing(@SuppressWarnings("unused") Request request) {
    requestProcessing.eventEnded();
  }

  public void dump(OutputStreamWriter output) throws IOException {
    output.write("requestProcessing : " + requestProcessing.getMean() + "\n");
    output.write("namenodeRPC : " + namenodeRPC.getMean() + "\n");
    output.write("executorWaiting : " + executorWaiting.getMean() + "\n");
  }

  // TODO: this is temporary, all metrics should get into Hadoop Metrics-like thing
  public void dump() throws IOException {
    FileWriter out = new FileWriter(outFile, true);
    try {
      dump(out);
    } finally {
      out.close();
    }
  }

  public static class EventDurationCalculator {
    private long lastEventTime = 0;
    private double cumulativeTimeMillis = 0;
    private int eventsCountTotal = 0;
    private int eventsCountCurrent = 0;

    private void update() {
      System.nanoTime(); // Discarding first result, empirically improves stability
      long now = System.nanoTime();
      cumulativeTimeMillis += ((double) eventsCountCurrent * (now - lastEventTime)) / 1e6;
      lastEventTime = now;
    }

    public synchronized void eventStarted() {
      update();
      eventsCountCurrent++;
      eventsCountTotal++;
    }

    public synchronized void eventEnded() {
      if (eventsCountCurrent < 0) {
        throw new IllegalStateException("No matching eventStarted()");
      }
      update();
      eventsCountCurrent--;
    }

    public synchronized double getMean() {
      update();
      return ((double) cumulativeTimeMillis / eventsCountTotal);
    }
  }
}
