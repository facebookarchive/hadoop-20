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
import org.apache.hadoop.hdfs.server.datanode.DataNode;

/**
 * This class tests the DFS class via the FileSystem interface in a single node
 * mini-cluster configured to use short circuit reads.
 */
public class TestReadShortCircuit extends TestCase {

  private MiniDFSCluster cluster = null;
  private FileSystem fileSystem = null;
  private Path file = null;
  private Path fileInline = null;
  private Path fileNonInline = null;
  
  private void writeFile(Path file) throws IOException {
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

  protected void setUp() throws IOException {    
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.read.shortcircuit", true);
    conf.setInt("dfs.block.size", 2048);
    
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fileSystem = cluster.getFileSystem();
    fileSystem.clearOsBuffer(true);

    for (DataNode dn : cluster.getDataNodes()) {
      dn.useInlineChecksum = true;
    }
    fileInline = new Path("testfile_inline.txt");
    writeFile(fileInline);

    for (DataNode dn : cluster.getDataNodes()) {
      dn.useInlineChecksum = false;
    }
    fileNonInline = new Path("testfile_non_inline.txt");
    writeFile(fileNonInline);

    for (DataNode dn : cluster.getDataNodes()) {
      dn.useInlineChecksum = true;
    }
  }
  
  protected void tearDown() {    
    cluster.shutdown();
    cluster = null;
    fileSystem = null;
    file = null;
  }

  public void testVariousPositional() throws IOException {
    // inline checksum case
    file = fileInline;
    variousPositionalIntenal();
    
    // non inline checksum case
    file = fileNonInline;
    variousPositionalIntenal();
  }


  private void variousPositionalIntenal() throws IOException {
    readFrom(null, 0, 10);
    readFrom(null, 5, 10);
    readFrom(null, 512 + 5, 10);
    readFrom(null, 2048 + 512 + 5, 10);
    readFrom(null, 512 - 5, 10);
    readFrom(null, 512 - 5, 512 * 2 + 5);
    readFrom(null, 100 * 100 - 5, 2);
    readFrom(null, 100 * 100 - 5, 5);
    readFrom(null, 10000 - 10000 % 512, 5);
    readFrom(null, 10000 - 10000 % 512, 10000 % 512);
    complexSkipAndReadSequence();
    
    fileSystem.setVerifyChecksum(false);
    FSDataInputStream inputStream = fileSystem.open(file);
    readFrom(inputStream, 0, 10);
    readFrom(inputStream, 5, 10);
    readFrom(inputStream, 512 + 5, 10);
    readFrom(inputStream, 2048 + 512 + 5, 10);
    readFrom(inputStream, 512 - 5, 10);
    readFrom(inputStream, 512 - 5, 512 * 2 + 5);
    readFrom(inputStream, 512 - 5, 512 * 4 + 5);
    readFrom(inputStream, 2048 + 512 + 5, 10);
    readFrom(inputStream, 2048 - 512 - 57, 512 * 3 + 119);
    readFrom(inputStream, 2048 - 512 - 57, 2048);
    readFrom(inputStream, 2048 - 512 * 2 - 57, 512 * 2);
    readFrom(inputStream, 5, 10);
    readFrom(inputStream, 512 + 5, 10);
    // Read to the last partial chunk
    readFrom(inputStream, 100 * 100 - 7, 7);
    readFrom(inputStream, 100 * 100 - 7, 5);
    readFrom(inputStream, 100 * 100 - 1024 - 7, 1024 + 7);
    inputStream.close();
    fileSystem.setVerifyChecksum(true);

    inputStream = fileSystem.open(file);
    readFrom(inputStream, 0, 10);
    readFrom(inputStream, 5, 10);
    readFrom(inputStream, 512 + 5, 10);
    readFrom(inputStream, 2048 + 512 + 5, 10);
    readFrom(inputStream, 512 - 5, 10);
    readFrom(inputStream, 512 - 5, 512 * 2 + 5);
    readFrom(inputStream, 512 - 5, 512 * 4 + 5);
    readFrom(inputStream, 2048 + 512 + 5, 10);
    readFrom(inputStream, 2048 - 512 - 57, 512 * 3 + 119);
    readFrom(inputStream, 2048 - 512 - 57, 2048);
    readFrom(inputStream, 2048 - 512 * 2 - 57, 512 * 2);
    readFrom(inputStream, 5, 10);
    readFrom(inputStream, 512 + 5, 10);
    // Read to the last partial chunk
    readFrom(inputStream, 100 * 100 - 7, 7);
    readFrom(inputStream, 100 * 100 - 7, 5);
    readFrom(inputStream, 100 * 100 - 1024 - 7, 1024 + 7);
    inputStream.close();
    
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
  private void readFrom(FSDataInputStream userInputStream, int position,
      int length) throws IOException {
    FSDataInputStream inputStream = (userInputStream != null) ? userInputStream
        : fileSystem.open(file);
    byte[] buffer = createBuffer(10 + length);
    if (position > 0) {
      inputStream.read(position, buffer, 10, length);
    } else {
      inputStream.read(buffer, 10, length);
    }
    assertBufferHasCorrectData(position, buffer, 10, length);
    if (userInputStream == null) {
      inputStream.close();
    }
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
