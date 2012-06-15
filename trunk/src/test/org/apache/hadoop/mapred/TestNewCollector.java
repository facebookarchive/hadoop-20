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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
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

    /*
     * conf to specify number of big records to spill right after the mapper
     * starts, it is comma separated string for a list of values, each value is
     * one reducer.
     */
    private static String BIG_RECORDS_BEGINNING = "test.reducer.bigrecords.start";

    /*
     * conf to specify number of big records to spill in the middle of a
     * mapper, it is comma separated string for a list of values, each value is
     * one reducer.
     */
    private static String BIG_RECORDS_MIDDLE = "test.reducer.bigrecords.middle";

    /*
     * conf to specify number of big records to spill right before the mapper
     * finish, it is comma separated string for a list of values, each value is
     * one reducer.
     */
    private static String BIG_RECORDS_END = "test.reducer.bigrecords.end";
    
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

    public synchronized int[] getBigRecodsStart() {
      String bigRecordNumStartStr =
          currentJobConf.get(BIG_RECORDS_BEGINNING);
      int[] bigRecordsStart =
          splitConfToIntArray(bigRecordNumStartStr);
      
      return bigRecordsStart;
    }

    public synchronized int[] getBigRecodsMiddle() {
      String bigRecordNumMiddleStr =
          currentJobConf.get(BIG_RECORDS_MIDDLE);
      int[] bigRecordsMiddle =
          splitConfToIntArray(bigRecordNumMiddleStr);
      
      return bigRecordsMiddle;
    }

    public synchronized int[] getBigRecodsEnd() {
      String bigRecordNumEndStr = currentJobConf.get(BIG_RECORDS_END);
      int[] bigRecordsEnd = splitConfToIntArray(bigRecordNumEndStr);
      
      return bigRecordsEnd;
    }

    private int[] splitConfToIntArray(String confStr) {
      String[] splits = confStr.split(",");
      int[] numArray = new int[splits.length];
      for (int i = 0; i < splits.length; i++) {
        String num = splits[i];
        if (num == null || num.trim().equals("")) {
          numArray[i] = 0;
        } else {
          numArray[i] = Integer.parseInt(num);
        }
      }
      return numArray;
    }
    
    public boolean checkReducerReceiveRecNum(int reducerNum) {
      return reducerToReciveRecNum
          .remove(Integer.valueOf(reducerNum));
    }
    
    /**
     * Each mapper is omitting the same number of records. And
     * reducerRecPercents array decides how many should go to each reducer. One
     * reducer will receive the same number of records from different mappers.
     * 
     * @param numReducers
     *          number of reducers to run
     * @param mappers
     *          number of mappers to run
     * @param recordNumPerMapper
     *          how many records each mapper outputs
     * @param reducerRecPercents
     *          for one mapper, how to allocate output records to reducers
     * @param numBigRecordsStart
     * @param numBigRecordsMiddle
     * @param numBigRecordsEnd
     * @param job
     */
    public static void setJobConf(int numReducers, int mappers,
        int recordNumPerMapper, double[] reducerRecPercents,
        int[] numBigRecordsStart, int[] numBigRecordsMiddle,
        int[] numBigRecordsEnd, JobConf job) {
      int[] recNumReducerOneMapper = new int[numReducers];
      double left = 1.0f;
      int preAllocated = 0;
      int leftToAllocate = recordNumPerMapper;

      if (numBigRecordsStart == null) {
        numBigRecordsStart = new int[numReducers];
        fillZero(numBigRecordsStart);
      }

      if (numBigRecordsMiddle == null) {
        numBigRecordsMiddle = new int[numReducers];
        fillZero(numBigRecordsMiddle);
      }

      if (numBigRecordsEnd == null) {
        numBigRecordsEnd = new int[numReducers];
        fillZero(numBigRecordsEnd);
      }

      if (reducerRecPercents != null) {
        if (reducerRecPercents.length > numReducers) {
          throw new IllegalArgumentException(
              "percents array length is " + reducerRecPercents.length
                  + " while numReducers is " + numReducers);
        }
        preAllocated = reducerRecPercents.length;
      }
      for (int i = 0; i < preAllocated; i++) {
        left -= reducerRecPercents[i];
        if (left < 0) {
          throw new IllegalArgumentException(
              "sum of percents array is bigger than 1.0");
        }
        recNumReducerOneMapper[i] =
            (int) (recordNumPerMapper * reducerRecPercents[i]);
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
        int bigRecords =
            numBigRecordsStart[i] + numBigRecordsMiddle[i]
                + numBigRecordsEnd[i];
        if (bigRecords > recNumReducerOneMapper[i]) {
          throw new IllegalArgumentException(
              "big records number is bigger than total.");
        }
      }

      String recordNumConf = getStringConf(recNumReducerOneMapper);
      job.set(RECORD_NUM_CONF, recordNumConf);

      String bigRecordStartConf = getStringConf(numBigRecordsStart);
      job.set(BIG_RECORDS_BEGINNING, bigRecordStartConf);

      String bigRecordMiddleConf = getStringConf(numBigRecordsMiddle);
      job.set(BIG_RECORDS_MIDDLE, bigRecordMiddleConf);

      String bigRecordEndConf = getStringConf(numBigRecordsEnd);
      job.set(BIG_RECORDS_END, bigRecordEndConf);
      
      System.out.println("RECORD_NUM_CONF is " + recordNumConf);
      System.out.println("BIG_RECORDS_BEGINNING is " + bigRecordStartConf);
      System.out.println("BIG_RECORDS_MIDDLE is " + bigRecordMiddleConf);
      System.out.println("BIG_RECORDS_END is " + bigRecordEndConf);
    }

    private static String getStringConf(int[] numArray) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (int num : numArray) {
        if (first) {
          first = false;
        } else {
          sb.append(",");
        }
        sb.append(num);
      }
      return sb.toString();
    }

    private static void fillZero(int[] numBigRecordsStart) {
      for (int i = 0; i < numBigRecordsStart.length; i++) {
        numBigRecordsStart[i] = 0;
      }
    }
  }
  
  public static String toString(int[] numArray) {
    StringBuilder sb = new StringBuilder();
    for(int num: numArray) {
      sb.append(num);
      sb.append(",");
    }
    
    return sb.toString();
  }

  public static class TestNewCollectorMapper
      implements
      Mapper<NullWritable, NullWritable, BytesWritable, BytesWritable> {

    private int keylen = 1;
    private int vallen = 1;
    private int bigKeyLen = 10000;
    private int bigValLen = 10000;

    private int[] recNumForReducer;
    private int[] bigRecordsStart;
    private int[] normalKVNum;
    private int[] bigRecordsMiddle;
    private int[] bigRecordsEnd;
    
    
    public void configure(JobConf job) {
      recNumForReducer =
          RecordNumStore.getInst(job).getMapperOutNumForEachReducer();
      keylen = job.getInt("test.key.length", 1);
      vallen = job.getInt("test.value.length", 1);
      bigKeyLen = job.getInt("test.bigkey.length", 10000);
      bigValLen = job.getInt("test.bigvalue.length", 10000);
      bigRecordsStart =
          RecordNumStore.getInst(job).getBigRecodsStart();
      bigRecordsMiddle =
          RecordNumStore.getInst(job).getBigRecodsMiddle();
      bigRecordsEnd = RecordNumStore.getInst(job).getBigRecodsEnd();
      normalKVNum = new int[bigRecordsStart.length];
      for (int i = 0; i < normalKVNum.length; i++) {
        normalKVNum[i] =
            recNumForReducer[i]
                - (bigRecordsStart[i] + bigRecordsMiddle[i] + bigRecordsEnd[i]); 
      }
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
        if (bigRecordsStart[i] > 0) {
          collectBigKV(output, i);
          bigRecordsStart[i]--;
          recNumForReducer[i]--;
        } else if (normalKVNum[i] > 0 || bigRecordsMiddle[i] > 0) {
          if (normalKVNum[i] > 0) {
            collectNormalKV(output, i);
            normalKVNum[i]--;
            recNumForReducer[i]--;
          }
          if (bigRecordsMiddle[i] > 0) {
            collectBigKV(output, i);
            bigRecordsMiddle[i]--;
            recNumForReducer[i]--;
          }
        } else if (bigRecordsEnd[i] > 0) {
          collectBigKV(output, i);
          bigRecordsEnd[i]--;
          recNumForReducer[i]--;
        } else {
          throw new RuntimeException("Uncatched situation.");
        }
        outputed = true;
      }
    }
    
    private void collectKV(
        OutputCollector<BytesWritable, BytesWritable> output,
        int reducerNo, int keyLen, int valueLen) throws IOException {
      BytesWritable k =
          BytesWritableFactory.getRandomBytesWritable(keyLen);
      BytesWritable val =
          BytesWritableFactory.getRandomBytesWritable(valueLen);
      TestNewCollectorKey collectorKey = new TestNewCollectorKey(k);
      collectorKey.setHashCode(reducerNo);
      output.collect(collectorKey, val);
    }
    
    private void collectBigKV(
        OutputCollector<BytesWritable, BytesWritable> output,
        int reduceNo) throws IOException {
      this.collectKV(output, reduceNo, bigKeyLen, bigValLen);
    }

    private void collectNormalKV(
        OutputCollector<BytesWritable, BytesWritable> output,
        int reducerNo) throws IOException {
      this.collectKV(output, reducerNo, keylen, vallen);
    }
    
  }

  public static class TestNewCollectorReducer
      implements
      Reducer<BytesWritable, BytesWritable, NullWritable, NullWritable> {

    private int received = 0;
    private JobConf job;
    private BytesWritable lastKey = null;
    private RawComparator rawComparator;

    public void configure(JobConf job) {
      this.job = job;
      this.rawComparator =
          WritableComparator.get(BytesWritable.class);
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
    @SuppressWarnings("unchecked")
    public void reduce(BytesWritable key,
        Iterator<BytesWritable> values,
        OutputCollector<NullWritable, NullWritable> output,
        Reporter reporter) throws IOException {
      if(lastKey == null) {
        lastKey = new BytesWritable();
        lastKey.set(key.getBytes(), 0, key.getLength());
      } else {
        int ret = rawComparator.compare(lastKey, key);
        assertTrue("Incorrect comparasion result given by mapreduce",
            ret < 0);
        lastKey.set(key.getBytes(), 0, key.getLength());
      }
      while (values.hasNext()) {
        values.next();
        ++received;
      }
    }

    private void printBytes(BytesWritable key) {
      byte[] bytes = key.getBytes();
      for (int i = 0; i < key.getLength(); i++) {
        System.out.printf("%02x", bytes[i]);
      }
      System.out.println();
    }
  }
  
  private void runTest(String name, int keyLen, int valLen,
      int recordsNumPerMapper, int sortMb, float spillPer,
      int numMapperTasks, int numReducerTask,
      double[] reducerRecPercents) throws Exception {
    this.runTest(name, keyLen, valLen, 0, 0, recordsNumPerMapper,
        sortMb, spillPer, numMapperTasks, numReducerTask,
        reducerRecPercents, null, null, null);
  }

  private void runTest(String name, int keyLen, int valLen,
      int bigKeyLen, int bigValLen, int recordsNumPerMapper,
      int sortMb, float spillPer, int numMapperTasks,
      int numReducerTask, double[] reducerRecPercents,
      int[] numBigRecordsStart, int[] numBigRecordsMiddle,
      int[] numBigRecordsEnd) throws Exception {
    JobConf conf = mrCluster.createJobConf();
    conf.setInt("io.sort.mb", sortMb);
    conf.set("io.sort.spill.percent", Float.toString(spillPer));
    conf.setInt("test.key.length", keyLen);
    conf.setInt("test.value.length", valLen);
    conf.setInt("test.bigkey.length", bigKeyLen);
    conf.setInt("test.bigvalue.length", bigValLen);
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
        recordsNumPerMapper, reducerRecPercents, numBigRecordsStart,
        numBigRecordsMiddle, numBigRecordsEnd, conf);
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
  
  //test cases that require spilling data to disk
  public void testSpillMore() throws Exception {
    // 600 bytes for each mapper, 10K records for each mapper, and totally use
    // 6MB data. So it will require spill to disk
    runTest("testSpillMore_1", 100, 500, 10000, 1, 0.8f, 1, 1,
        new double[] { 1.0f });
    runTest("testSpillMore_2", 100, 500, 10000, 1, 0.8f, 2, 1,
        new double[] { 1.0f });
    runTest("testSpillMore_3", 100, 500, 10000, 1, 0.8f, 2, 2,
        new double[] { 0.5f, 0.5f });
  }

  //test skew cases
  public void testSkew() throws Exception {
    // first reducer got 90% records
    runTest("testSpillSkew_1", 100, 500, 10000, 4, 0.8f, 1, 10,
        new double[] { 0.9f});
    // first got 40%, and second got 40%
    runTest("testSpillSkew_2", 100, 500, 10000, 4, 0.8f, 1, 10,
        new double[] { 0.4f, 0.4f });
    // first got 60%, and second got 30%
    runTest("testSpillSkew_3", 100, 500, 10000, 4, 0.8f, 2, 10,
        new double[] { 0.6f, 0.3f });
  }
  
  public void testBigRecords() throws Exception {
    // 600 bytes for each small kv, and also output 60 big
    // records, 20 at the beginning, 20 in the middle, and 20 at the end
    runTest("testSpillBigRecords_1", 100, 500, 10000, 500000, 3000,
        1, 0.8f, 1, 1, new double[] { 1.0f }, new int[] { 20 },
        new int[] { 20 }, new int[] { 20 });
    runTest("testSpillBigRecords_2", 100, 500, 10000, 500000, 3000,
        1, 0.8f, 2, 1, new double[] { 1.0f }, new int[] { 20 },
        new int[] { 20 }, new int[] { 20 });
    runTest("testSpillBigRecords_3", 100, 500, 10000, 500000, 3000,
        1, 0.8f, 2, 2, new double[] { 0.5f, 0.5f }, new int[] { 20,
            20 }, new int[] { 20, 20 }, new int[] { 20, 20 });
  }
  
}
