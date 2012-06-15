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
import org.apache.hadoop.hdfs.GeneralConstant;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.util.ajax.JSON;

@SuppressWarnings("deprecation")
  public class GenMapper extends MapReduceBase implements
      Mapper<Text, Text, Text, Text>, GeneralConstant{

    private static final Log LOG = LogFactory.getLog(GenReduce.class);
    private static JobConf conf;
    public void configure(JobConf configuration) {
      conf = configuration;
    }
    
    @Override
    public void map(Text key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      try {
        Class<?> clazz = conf.getClass(THREAD_CLASS_KEY, null);
        if (clazz == null) {
          throw new IOException("Class " + conf.get(THREAD_CLASS_KEY) + " not found");
        }
        GenThread t = (GenThread)ReflectionUtils.newInstance(clazz, conf);
        GenThread[] threads = t.prepare(conf, key, value);
        long startTime = System.currentTimeMillis();
        for (GenThread thread: threads) {
          thread.start();
        }
        for (GenThread thread : threads) {
          try {
            thread.join();
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
        long endTime = System.currentTimeMillis();
        
        Map<String, String> stat = 
            t.collectStats(conf, threads, endTime - startTime);
        output.collect(new Text("stat"), 
            new Text(JSON.toString(stat)));
      } catch (IOException e) {
        LOG.error("Error: ", e);
        throw e;
      }
    }
  }
