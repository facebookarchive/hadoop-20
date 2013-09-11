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

package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class HarReader {
  private FSDataInputStream inputStream;
  private LineReader lineReader;
  private long totalSize;
  private long readSize = 0;
  
  public HarReader(Path archivePath, Configuration conf) throws IOException {
    Path indexFilePath = new Path(archivePath, HarFileSystem.INDEX_NAME);
    FileSystem fs = indexFilePath.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(indexFilePath);
    inputStream = fs.open(indexFilePath);
    lineReader = new LineReader(inputStream, conf);
    totalSize = fileStatus.getLen();
  }
  
  public boolean hasNext() {
    return readSize < totalSize; 
  } 
  
  public HarStatus getNext() throws IOException {
      Text line = new Text();
      readSize += lineReader.readLine(line);
      return new HarStatus(line.toString());
  }
  
  public void close() throws IOException {
    inputStream.close();
  }

}
