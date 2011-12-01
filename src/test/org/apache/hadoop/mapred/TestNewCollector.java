/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TestMapCollection.FakeIF;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

import junit.framework.TestCase;

@SuppressWarnings("deprecation")
public class TestNewCollector extends TestCase {

  private static Log LOG = LogFactory.getLog(TestNewCollector.class);

  private MiniMRCluster mrCluster;

  protected void setUp() {
    JobConf conf = new JobConf();
    try {
      mrCluster =
          new MiniMRCluster(2, "file:///", 3, null, null, conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void tearDown() {
    mrCluster.shutdown();
  }

  public static class BytesWritableFactory {

    private static Random random = new Random();

    public static BytesWritable getRandomBytesWritable(int size) {
      byte[] bytes = new byte[size];
      random.nextBytes(bytes);
      BytesWritable bytesWritable = new BytesWritable(bytes);
      return bytesWritable;
    }

    public static BytesWritable getRepeatedBytesWritable(
        byte[] bytes, int repeatNum) {
      int newLen = bytes.length * repeatNum;
      byte[] bb = new byte[newLen];
      for (int i = 0; i < repeatNum; i++) {
        System
            .arraycopy(bytes, 0, bb, bytes.length * i, bytes.length);
      }
      BytesWritable bytesWritable = new BytesWritable(bb);
      return bytesWritable;
    }
  }

  public static class TestNewCollectorKey extends BytesWritable {
    private int hashCode = -1;

    public TestNewCollectorKey(BytesWritable k) {
      super(k.getBytes());
    }
    
    public TestNewCollectorKey() {
      super();
    }

    public int hashCode() {
      if (hashCode < 0) {
        hashCode = super.hashCode();
      }
      return hashCode;
    }

    public void setHashCode(int hashCode) {
      this.hashCode = hashCode;
    }
  }

  public static class RecordNumStore {

    private static String RECORD_NUM_CONF =
        "test.reducer.records.num";

    private JobConf currentJobConf;
    private List<Integer> reducerToReciveRecNum;
    private int[] mapperOutNumForEachReducer;

    private static RecordNumStore inst;
    private static Object instanceLock = new Object();

    private RecordNumStore(JobConf job) {
      this.currentJobConf = job;
      init(job);
    }

    public void init(JobConf job) {
      String recordNumStr = job.get(RECORD_NUM_CONF);
      int numMappers = job.getNumMapTasks();
      reducerToReciveRecNum = new ArrayList<Integer>(numMappers);
      if (recordNumStr != null) {
        String[] splits = recordNumStr.split(",");
        for(String num: splits) {
          if (num == null || num.trim().equals("")) {
            continue;
          }
          reducerToReciveRecNum.add(Integer.parseInt(num));
        }
      }
      
      for (int i = reducerToReciveRecNum.size(); i < numMappers; i++) {
        reducerToReciveRecNum.add(0);
      }
    }

    public static RecordNumStore getInst(JobConf job) {
      synchronized(instanceLock) {
        if (job != null
            && (inst == null || job != inst.getCurrentJobConf())) {
          inst = new RecordNumStore(job);        
        }
        return inst;
      }
    }
    
    protected JobConf getCurrentJobConf() {
      return currentJobConf;
    }
    
    public synchronized int[] getMapperOutNumForEachReducer() {
      int numReducers = currentJobConf.getNumReduceTasks();
      int numMappers = currentJobConf.getNumMapTasks();
      if (mapperOutNumForEachReducer == null) {
        mapperOutNumForEachReducer = new int[numReducers];
      }

      List<Integer> reducerToReciveNum = this.reducerToReciveRecNum;
      for (int i = 0; i < numReducers; i++) {
        mapperOutNumForEachReducer[i] =
            reducerToReciveNum.get(i) / numMappers;
      }

      return mapperOutNumForEachReducer;
    }
    
    public boolean checkReducerReceiveRecNum(int reducerNum) {
      return reducerToReciveRecNum
          .remove(Integer.valueOf(reducerNum));
    }

    public static void setJobConf(int numReducers, int mappers,
        int recordNumPerMapper, double[] percents, JobConf job) {
      int[] recNumReducerOneMapper = new int[numReducers];
      double left = 1.0f;
      int preAllocated = 0;
      int leftToAllocate = recordNumPerMapper;
      if (percents != null) {
        if (percents.length > numReducers) {
          throw new IllegalArgumentException(
              "percents array length is " + percents.length
                  + " while numReducers is " + numReducers);
        }
        preAllocated = percents.length;
      }
      for (int i = 0; i < preAllocated; i++) {
        left -= percents[i];
        if (left < 0) {
          throw new IllegalArgumentException(
              "sum of percents array is bigger than 1.0");
        }
        recNumReducerOneMapper[i] =
            (int) (recordNumPerMapper * percents[i]);
        leftToAllocate -= recNumReducerOneMapper[i];
      }

      int toAllocateReducer = preAllocated;
      while (leftToAllocate > 0 && toAllocateReducer < numReducers) {
        recNumReducerOneMapper[toAllocateReducer] += 1;
        toAllocateReducer++;
        if (toAllocateReducer == numReducers) {
          toAllocateReducer = preAllocated;
        }
        leftToAllocate--;
      }

      for (int i = 0; i < recNumReducerOneMapper.length; i++) {
        recNumReducerOneMapper[i] =
            recNumReducerOneMapper[i] * mappers;
      }

      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (int num : recNumReducerOneMapper) {
        if (first) {
          first = false;
        } else {
          sb.append(",");
        }
        sb.append(num);
      }
      
      job.set(RECORD_NUM_CONF, sb.toString());
    }
  }

  public static class TestNewCollectorMapper
      implements
      Mapper<NullWritable, NullWritable, BytesWritable, BytesWritable> {

    private int keylen = 1;
    private int vallen = 1;
    private int[] recNumForReducer;

    public void configure(JobConf job) {
      recNumForReducer =
          RecordNumStore.getInst(job).getMapperOutNumForEachReducer();
      keylen = job.getInt("test.key.length", 1);
      vallen = job.getInt("test.value.length", 1);
    }

    public void close() {
    }

    @Override
    public void map(NullWritable key, NullWritable value,
        OutputCollector<BytesWritable, BytesWritable> output,
        Reporter reporter) throws IOException {
      boolean outputed = false;
      int i = -1;
      while (true) {
        reporter.progress();
        i++;
        if (i == recNumForReducer.length) {
          if (!outputed) {
            break;
          }
          i = 0;
          outputed = false;
        }
        if (recNumForReducer[i] == 0) {
          continue;
        }
        BytesWritable k =
            BytesWritableFactory.getRandomBytesWritable(keylen);
        BytesWritable val =
            BytesWritableFactory.getRandomBytesWritable(vallen);
        TestNewCollectorKey collectorKey = new TestNewCollectorKey(k);
        collectorKey.setHashCode(i);
        output.collect(collectorKey, val);
        outputed = true;
        recNumForReducer[i]--;
      }
    }
  }

  public static class TestNewCollectorReducer
      implements
      Reducer<BytesWritable, BytesWritable, NullWritable, NullWritable> {

    private int received = 0;
    private JobConf job;

    public void configure(JobConf job) {
      this.job = job;
    }

    public void close() {
      boolean found =
          RecordNumStore.getInst(job).checkReducerReceiveRecNum(
              received);
      System.out.println("received count is " + received
          + ", found is " + found);
      assertTrue("Unexpected record count (" + received + ")", found);
    }

    @Override
    public void reduce(BytesWritable key,
        Iterator<BytesWritable> values,
        OutputCollector<NullWritable, NullWritable> output,
        Reporter reporter) throws IOException {
      while (values.hasNext()) {
        values.next();
        ++received;
      }
    }
  }

  private void runTest(String name, int keyLen, int valLen,
      int recordsNumPerMapper, int sortMb, float spillPer,
      int numMapperTasks, int numReducerTask,
      double[] reducerRecPercents) throws Exception {
    JobConf conf = mrCluster.createJobConf();
    conf.setInt("io.sort.mb", sortMb);
    conf.set("io.sort.spill.percent", Float.toString(spillPer));
    conf.setInt("test.key.length", keyLen);
    conf.setInt("test.value.length", valLen);
    conf.setNumMapTasks(numMapperTasks);
    conf.setNumReduceTasks(numReducerTask);
    conf.setInputFormat(FakeIF.class);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapperClass(TestNewCollectorMapper.class);
    conf.setReducerClass(TestNewCollectorReducer.class);
    conf.setMapOutputKeyClass(TestNewCollectorKey.class);
    conf.setMapOutputValueClass(BytesWritable.class);
    conf.setBoolean("mapred.map.output.blockcollector", true);

    RecordNumStore.setJobConf(numReducerTask, numMapperTasks,
        recordsNumPerMapper, reducerRecPercents, conf);
    RecordNumStore.getInst(conf);
    LOG.info("Running " + name);
    JobClient.runJob(conf);
  }
  
  public void testNormalInMemory() throws Exception {
    runTest("testSmallScale_1", 1, 1, 1, 40, 0.5f, 1, 1,
        new double[] { 1.0f });
    // 200 bytes for each record, and 10000 records for each mapper, so
    // serialized data will use 2MB
    // data, and should be able to hold in memory.
    runTest("testSmallScale_2", 100, 100, 10000, 4, 0.8f, 1, 1,
        new double[] { 1.0f });
    runTest("testSmallScale_2", 100, 100, 10000, 4, 0.8f, 10, 1,
        new double[] { 1.0f });
    // run 2 mappers and 1 reducers, and each mapper output 4MB data.
    runTest("testSmallScale_3", 100, 100, 10000, 4, 0.8f, 2, 1,
        new double[] { 1.0f });
    // run 2 mappers and 2 reducers, and each mapper output 4MB data.
    runTest("testSmallScale_4", 100, 100, 10000, 4, 0.8f, 2, 2,
        new double[] { 0.5f, 0.5f });
  }
  
  //test cases that require spilling data to disk
  public void testSpill() throws Exception {
    // 600 bytes for each mapper, 10K records for each mapper, and totally use
    // 6MB data. So it will require spill to disk
    runTest("testSpill_1", 100, 500, 10000, 4, 0.8f, 1, 1,
        new double[] { 1.0f });
    runTest("testSpill_2", 100, 500, 10000, 4, 0.8f, 2, 1,
        new double[] { 1.0f });
    runTest("testSpill_3", 100, 500, 10000, 4, 0.8f, 2, 2,
        new double[] { 0.5f, 0.5f });
  }
  
  //test skew cases
  public void testSkew() throws Exception {
    // first reducer got 90% records
    runTest("testSpill_1", 100, 500, 10000, 4, 0.8f, 1, 10,
        new double[] { 0.9f});
    // first got 40%, and second got 40%
    runTest("testSpill_2", 100, 500, 10000, 4, 0.8f, 1, 10,
        new double[] { 0.4f, 0.4f });
    // first got 60%, and second got 30%
    runTest("testSpill_3", 100, 500, 10000, 4, 0.8f, 2, 10,
        new double[] { 0.6f, 0.3f });
  }
  
}
