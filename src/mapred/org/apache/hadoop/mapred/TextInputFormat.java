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

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. 
 * @deprecated Use {@link org.apache.hadoop.mapreduce.lib.input.TextInputFormat}
 *  instead.
 */
@Deprecated
public class TextInputFormat extends FileInputFormat<LongWritable, Text>
  implements JobConfigurable {

  private CompressionCodecFactory compressionCodecs = null;
  
  public void configure(JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }
  
  protected boolean isSplitable(FileSystem fs, Path file) {
    return compressionCodecs.getCodec(file) == null;
  }

  static class EmptyRecordReader implements RecordReader<LongWritable, Text> {
    @Override
    public void close() throws IOException {}
    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }
    @Override
    public Text createValue() {
      return new Text();
    }
    @Override
    public long getPos() throws IOException {
      return 0;
    }
    @Override
    public float getProgress() throws IOException {
      return 0;
    }
    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
      return false;
    }
  }
    
  public RecordReader<LongWritable, Text> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
    throws IOException {
    
    reporter.setStatus(genericSplit.toString());

    // This change is required for CombineFileInputFormat to work with .gz files.
    
    // Check if we should throw away this split
    long start = ((FileSplit)genericSplit).getStart();
    Path file = ((FileSplit)genericSplit).getPath();
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (codec != null && start != 0) {
      // (codec != null) means the file is not splittable.
      
      // In that case, start should be 0, otherwise this is an extra split created
      // by CombineFileInputFormat. We should ignore all such extra splits.
      //
      // Note that for the first split where start = 0, LineRecordReader will
      // ignore the end pos and read the whole file.
      return new EmptyRecordReader();
    }

    String delimiter = job.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes();
    }
    return new LineRecordReader(job, (FileSplit) genericSplit,
      recordDelimiterBytes);
  }
}
