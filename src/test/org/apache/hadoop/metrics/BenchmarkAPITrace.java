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
package org.apache.hadoop.metrics;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.metrics.APITrace.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;


/**
 * 
 */
public class BenchmarkAPITrace extends Configured implements Tool {

  // measure performance of logging method
  public static void takeMeasurements() {
    long startTime = System.currentTimeMillis();
    long lastTime = startTime;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 1e5; j++) {
        CallEvent event = new CallEvent();
        event.logCall(0,
                      100,
                      new Object[] {"arg1", "arg2", "arg3"},
                      123);
      }
      long currTime = System.currentTimeMillis();
      System.out.println(i + " group: " + (currTime - lastTime));
      lastTime = currTime;
    }
    long totalTime = System.currentTimeMillis() - startTime;
    System.out.println("Total time: " + totalTime);
  }

  public int run(String[] args) throws IOException {
    takeMeasurements();
    return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    // To execute, run:
    //
    // ./bin/hadoop jar build/hadoop-0.20.1-dev-core.jar
    //    org.apache.hadoop.metrics.BenchmarkAPITrace 2> /dev/null

    int res = ToolRunner.run(new Configuration(),
                             new BenchmarkAPITrace(), args);
    System.exit(res);
  }

}
