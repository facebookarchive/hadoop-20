package org.apache.hadoop.mapred;
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

import org.apache.hadoop.hdfs.DirConstant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class DirReduce extends MapReduceBase implements
    Reducer<Text, Text, Text, Text>, DirConstant {

  private FileSystem fs;
  private Configuration conf;

  public void configure(JobConf configuration) {
    conf = configuration;
  }

  @Override
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    int size = 0;

    double averageIORate = 0;
    List<Float> list = new ArrayList<Float>();

    fs = FileSystem.get(conf);
    FSDataOutputStream out;

    if (fs.exists(new Path(OUTPUT, "result-writing")))
      out = fs.create(new Path(OUTPUT, "result-reading"), true);
    else
      out = fs.create(new Path(OUTPUT, "result-writing"), true);

    long nTasks = Long.parseLong(conf.get("dfs.nTasks"));
    long nmaps = Long.parseLong(conf.get("dfs.nmaps"));


    out.writeChars("-----------------------------\n");
    out.writeChars("Number of tasks:\t" + nmaps + "\n");
    out.writeChars("Files per task:\t\t" + nTasks + "\n");
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;

    // TODO Auto-generated method stub
    for (; values.hasNext();) {
      size++;
      // tokens.nextToken(); there is only one value per line
      double ioRate = Double.parseDouble(values.next().toString());
      if (ioRate > max)
        max = (float) ioRate;
      if (ioRate < min)
        min = (float) ioRate;
      list.add((float) ioRate);
      // this is for testing
      // output.collect(new Text(String.valueOf(bufferSize)), new
      // Text(
      // String.valueOf(ioRate)));
      // out.writeChars(bufferSize + " bytes\t\t" + ioRate +
      // " Mb/s\n");
      averageIORate += ioRate;
    }
    out.writeChars("Min\t\t\t" + min + "\n");
    out.writeChars("Max\t\t\t" + max + "\n");
    averageIORate /= size;
    float temp = (float) 0.0;
    for (int i = 0; i < list.size(); i++) {
      temp += Math.pow(list.get(i) - averageIORate, 2);
    }
    out.writeChars("Average\t\t\t: " + averageIORate + "\n");
    float dev = (float) Math.sqrt(temp / size);
    out.writeChars("Std. dev\t\t: " + dev + "\n");
    out.close();
  }

}
