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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.GeneralConstant;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

@SuppressWarnings("deprecation")
public class GenReduce extends MapReduceBase implements
    Reducer<Text, Text, Text, Text>, GeneralConstant {
  private static final Log LOG = LogFactory.getLog(GenReduce.class);
  private Configuration conf;

  public void configure(JobConf configuration) {
    conf = configuration;
  }
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    FSDataOutputStream out = null;
    try {
      int size = 0;
      FileSystem fs = FileSystem.get(conf);
      double averageIORate = 0;
      List<Float> list = new ArrayList<Float>();
      
      String output_dir = conf.get(OUTPUT_DIR_KEY);
      Path result_file = new Path(output_dir, "results");
      if (fs.exists(result_file)) {
        fs.delete(result_file);
      }
      out = fs.create(result_file, true);
    
      long nmaps = conf.getLong(NUMBER_OF_MAPS_KEY, NMAPS);
      long nthreads = conf.getLong(NUMBER_OF_THREADS_KEY, NTHREADS);

      out.writeChars("-----------------------------\n");
      out.writeChars("Number of mapper :\t\t\t" + nmaps + "\n");
      out.writeChars("Number of threads :\t\t\t" + nthreads + "\n");
    
      float min = Float.MAX_VALUE;
      float max = Float.MIN_VALUE;
      Class<?> clazz = conf.getClass(THREAD_CLASS_KEY, null);
      if (clazz == null) {
        throw new IOException("Class " + conf.get(THREAD_CLASS_KEY) + " not found");
      }
      GenThread t = (GenThread)ReflectionUtils.newInstance(clazz, conf);
      t.reset();
      long total_files = 0;
      long total_processed_size = 0;
      long total_num_errors = 0;
      String total_error = "";
      TypeReference<Map<String, String>> type = 
          new TypeReference<Map<String, String>>() { };
      while (values.hasNext()) {
        Map<String, String> stat = mapper.readValue(values.next().toString(), type);
        size++;
        total_files += Long.parseLong(stat.get("files"));
        total_processed_size += Long.parseLong(stat.get("size"));
        total_num_errors += Long.parseLong(stat.get("nerrors"));
        total_error += stat.get("errors");
        double ioRate = Double.parseDouble(stat.get("rate"));
        if (ioRate > max)
          max = (float) ioRate;
        if (ioRate < min)
          min = (float) ioRate;
        list.add((float) ioRate);
        averageIORate += ioRate;
        t.analyze(stat);
      }
    
      out.writeChars("Number of files processed:\t\t" + total_files + "\n");
      out.writeChars("Number of size processed:\t\t" + total_processed_size + "\n");
      out.writeChars("Min IO Rate(MB/sec): \t\t\t" + min + "\n");
      out.writeChars("Max IO Rate(MB/sec): \t\t\t" + max + "\n");
      averageIORate /= size;
      float temp = (float) 0.0;
      for (int i = 0; i < list.size(); i++) {
        temp += Math.pow(list.get(i) - averageIORate, 2);
      }
      out.writeChars("Average(MB/sec): \t\t\t" + averageIORate + "\n");
      float dev = (float) Math.sqrt(temp / size);
      out.writeChars("Std. dev: \t\t\t\t" + dev + "\n");
      out.writeChars("Total number of errors:\t\t\t" + total_num_errors + "\n");
      out.writeChars(total_error);
      t.output(out);
    } catch (IOException e) {
      LOG.error("Error:", e);
      throw e;
    } finally {
      out.close();
      
    }
  }
}


