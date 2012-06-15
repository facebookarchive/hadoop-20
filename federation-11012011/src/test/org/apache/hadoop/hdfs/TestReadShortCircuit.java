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
package org.apache.hadoop.hdfs;

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests the DFS class via the FileSystem interface in a single node
 * mini-cluster configured to use short circuit reads.
 */
public class TestReadShortCircuit extends TestCase {

  private MiniDFSCluster cluster = null;
  private FileSystem fileSystem = null;
  private Path file = null;

  protected void setUp() throws IOException {    
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.read.shortcircuit", true);
    conf.setInt("dfs.block.size", 2048);
    
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fileSystem = cluster.getFileSystem();
    
    file = new Path("testfile.txt");
    DataOutputStream outputStream = fileSystem.create(file);
    
    // Fill the file with 10,000 bytes of the repeated string
    // "ABCDEFGHIJ"
    
    byte[] buffer = new byte[100];
    for (int i = 0; i < 100; i++) {
      buffer[i] = (byte) ('A' + (i % 10));
    }
    for (int i = 0; i < 100; i++) {
      outputStream.write(buffer);      
    }
    outputStream.close();
  }
  
  protected void tearDown() {    
    cluster.shutdown();
    cluster = null;
    fileSystem = null;
    file = null;
  }
  
  public void testVariousPositional() throws IOException {
    readFrom(0, 10);
    readFrom(5, 10);
    readFrom(512 + 5, 10);
    readFrom(2048 + 512 + 5, 10);
    readFrom(512 - 5, 10);
    readFrom(512 - 5, 512 * 2 + 5);
    complexSkipAndReadSequence();
    
    // This one changes the state of the fileSystem, so do it
    // last.
    readFullWithoutChecksum();
  }
   
  private void complexSkipAndReadSequence() throws IOException {
    DataInputStream inputStream = fileSystem.open(file);
    byte[] buffer = createBuffer(2048 * 10);

    // We start by positioning mid-way in the third chunk
    int position = 0;
    int length = 512 * 3 - 256;
    inputStream.skip(length);
    position += length;
    
    // And read the second half of the third chunk
    length = 256 - 36;
    inputStream.read(buffer, 0, length);
    assertBufferHasCorrectData(position, buffer, 0, length);
    position += length;
    
    // At this point we should be chunk-aligned, lets read 32 bytes
    length = 32;
    inputStream.read(buffer, 0, length);
    assertBufferHasCorrectData(position, buffer, 0, length);
    position += length;

    // Read the remainder of the file    
    length = 10000 - position;
    buffer = createBuffer(length);
    inputStream.readFully(buffer);
    assertBufferHasCorrectData(position, buffer, 0, length);
    
    inputStream.close();
  }
  
  private void readFullWithoutChecksum() throws IOException {
    fileSystem.setVerifyChecksum(false);
    DataInputStream inputStream = fileSystem.open(file);
    byte[] buffer = createBuffer(10000);
    inputStream.readFully(buffer);
    assertBufferHasCorrectData(0, buffer, 0, 10000);
    inputStream.close();
  }
  
  /**
   * Reads length bytes from the stream and verifies them. If position
   * is positive, it uses the positional read API from FSDataInputStream,
   * otherwise it uses the read API from DataInputStream.
   * 
   * @param position - optional number of bytes to skip
   * @param length - number of bytes to read
   * @throws IOException
   */
  private void readFrom(int position, int length) throws IOException {
    FSDataInputStream inputStream = fileSystem.open(file);
    byte[] buffer = createBuffer(10 + length);
    if (position > 0) {
      inputStream.read(position, buffer, 10, length);
    } else {
      inputStream.read(buffer, 10, length);
    }
    assertBufferHasCorrectData(position, buffer, 10, length);
    inputStream.close();
  }
  
  private static byte[] createBuffer(int size) {
    byte[] buffer = new byte[size];
    for (int i = 0; i < size; i++) {
      buffer[i] = '#';
    }
    return buffer;
  }
  
  private static void assertBufferHasCorrectData(int dataOffset, byte[] buffer, int bufOffset, int length) {
    for (int i = 0; i < length; i++) {
      assertEquals(
          "At offset '" + i + "'",  
          (byte) ('A' + ((dataOffset + i) % 10)),
          buffer[bufOffset + i]);
    }
  }
}
