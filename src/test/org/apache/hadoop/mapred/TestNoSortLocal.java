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

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;


import junit.framework.TestCase;

public class TestNoSortLocal extends TestCase {
  private static String TEST_ROOT_DIR = System.getProperty("test.build.data", ".");

  static String INPUTPATH = TEST_ROOT_DIR + "/testing/nosortlocal/input";
  static String OUTPUTPATH = TEST_ROOT_DIR + "/testing/nosortlocal/output";

  void prepareInput(int size, FileSystem fs) throws IOException {
    FSDataOutputStream out = fs.create(new Path(new Path(INPUTPATH), "part-00000"));
    for (int i=0;i<size;i++) {
      out.write(String.format("%09d\t%09d\n", i/4, i).getBytes());
    }
    out.close();
    out = fs.create(new Path(new Path(INPUTPATH), "part-00001"));
    for (int i=0;i<size;i++) {
      out.write(String.format("%09d\t%09d\n", i/4, i).getBytes());
    }
    out.close();
  }

  public void testNoSort() throws IOException {
    JobConf conf = new JobConf();
    FileSystem fs = FileSystem.get(conf);
    prepareInput(10000, fs);
    conf.setInt("io.sort.mb", 2);
    conf.setBoolean("mapred.map.output.sort", false);
    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    KeyValueTextInputFormat.setInputPaths(conf, new Path(INPUTPATH));
    TextOutputFormat.setOutputPath(conf, new Path(OUTPUTPATH));
    conf.setNumReduceTasks(1);
    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    JobClient.runJob(conf);
  }
}
