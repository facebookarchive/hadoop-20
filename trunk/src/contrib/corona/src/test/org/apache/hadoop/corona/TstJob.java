package org.apache.hadoop.corona;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TstJob {
  static class TestInputSplit extends InputSplit implements Writable {
    private String[] locs;
    
    public TestInputSplit() {}

    TestInputSplit(String[] locs) { this.locs = locs; }

    @Override
    public long getLength() {  return 0; }

    @Override
    public String[] getLocations() { return locs; }

    @Override
    public void readFields(DataInput in) throws IOException {
      int len = in.readInt();
      locs = new String[len];
      for (int i = 0; i < len; i++) {
        locs[i] = in.readUTF();
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(locs.length);
      for (String s: locs) {
        out.writeUTF(s);
      }
    }
  }
  
  static class TestRecordReader extends RecordReader<LongWritable, Text> {
    @Override
    public void close() {}

    @Override
    public LongWritable getCurrentKey() { return null; }

    @Override
    public Text getCurrentValue() { return null; }

    @Override
    public float getProgress() { return 0; }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {}

    @Override
    public boolean nextKeyValue() { return false; }
  }
  
  public static class TestInputFormat extends InputFormat<LongWritable, Text> {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      return new TestRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
        InterruptedException {
      String locationsCsv = context.getConfiguration().get("test.locations");
      String[] locations = locationsCsv.split(",");
      List<InputSplit> splits = new ArrayList<InputSplit>();
      for (String loc: locations) {
        splits.add(new TestInputSplit(new String[]{loc}));
      }
      return splits;
    }
  }

  public static class TestMapper extends
      Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text val, Context c) {}
  }
}
