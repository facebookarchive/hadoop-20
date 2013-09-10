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
package org.apache.hadoop.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.InjectionEventCore;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

public class TestBufferedByteInputOutput {

  static final Log LOG = LogFactory.getLog(TestBufferedByteInputOutput.class
      .getName());

  private byte[] input;
  private byte[] output;

  private Random rand = new Random();

  private void setUp(int inputSize) {
    input = new byte[inputSize];
    output = new byte[inputSize];
    rand.nextBytes(input);
  }
  
  @After
  public void tearDown() {
    InjectionHandler.clear();
  }
  
  /**
   * Test raw buffer writes and reads.
   */
  @Test
  public void testBuffer() throws Exception {
    for (int i = 0; i < 100; i++) {
      testBuffer(rand(1000), rand(1000), -1, true);
    }
  }
  
  /**
   * Test basic functionality
   */
  @Test
  public void testBufferBasic() throws Exception {
    
    setUp(50);
    final BufferedByteInputOutput buffer = new BufferedByteInputOutput(50);
    
    // empty buffer
    assertEquals(0, buffer.available());
    assertEquals(0, buffer.totalRead());
    assertEquals(0, buffer.totalWritten());
    
    // write 20 bytes
    buffer.write(input, 0, 20);
    assertEquals(20, buffer.available());
    assertEquals(0, buffer.totalRead());
    assertEquals(20, buffer.totalWritten());
    
    // read 10 bytes
    buffer.read(output, 0, 10);
    assertEquals(10, buffer.available());
    assertEquals(10, buffer.totalRead());
    assertEquals(20, buffer.totalWritten());
    
    // no more writes should be accepted
    // we still should be able to read 10 bytes
    buffer.close();
    try {
      buffer.write(1);
      fail("Should not accept writes");
    } catch (Exception e) {
      LOG.info("Expected exception");
    }
    // try to read 20 bytes
    assertEquals(10, buffer.available());
    assertEquals(10, buffer.read(output, 10, 20));
    
    // next read should return -1
    assertEquals(0, buffer.available());
    assertEquals(-1, buffer.read(output, 20, 5));
    assertEquals(-1, buffer.read());
  }
  
  /**
   * Can be used for testing throughput on large buffer.
   */
  @Test
  public void testBufferThroughput() throws Exception {
    LOG.info("Test buffer throughput");
    long time = testBuffer(1024 * 1024, 10 * 1024, 1024, false);
    LOG.info("Time taken: " + time);
  }

  /**
   * Test pipeline where background thread reads from underlying stream.
   */
  @Test
  public void testInputStream() throws IOException {
    for (int i = 0; i < 100; i++) {
      testInputStream(rand(1000), rand(1000), rand(1000));
    }
  }

  /**
   * Test pipeline where background thread writes to underlying stream.
   */
  @Test
  public void testOutputStream() throws IOException {
    // do close after completing writes
    for (int i = 0; i < 100; i++) {
      testOutputStream(rand(1000), rand(1000), rand(1000), true);
    }
    
    // do flush after completing writes
    for (int i = 0; i < 100; i++) {
      testOutputStream(rand(1000), rand(1000), rand(1000), false);
    }
  }

  /**
   * Test reading from closed buffer.
   */
  @Test
  public void testCloseInput() throws IOException {
    LOG.info("Running test close input");
    setUp(1000);

    // input is of size 1000, so the ReadThread will attempt to write to
    // the buffer, which will fail, but we should be able to read 100 bytes
    ByteArrayInputStream is = new ByteArrayInputStream(input);
    DataInputStream dis = BufferedByteInputStream.wrapInputStream(is, 100, 10);

    // wait for the thread to read from is and
    // write to the buffer
    while(dis.available() < 100) {
      sleep(10);
    }
    
    // no more writes to the internal buffer
    dis.close();
 
    try {
      dis.read(); // read will call DataInputStream fill() which should fail
      fail("Read should fail because we are closed");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
    
    dis.close(); // can call multiple close()  
    
    try {
      dis.read(new byte[10], 0, 10);
      fail("Read should fail because we are closed");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
    
    try {
      dis.available();
      fail("Available should fail because we are closed");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
  }

  /**
   * Test if writing to closed buffer fails.
   */
  @Test
  public void testCloseOutput() throws IOException {
    LOG.info("Running test close output");
    ByteArrayOutputStream os = new ByteArrayOutputStream(1000);
    DataOutputStream dos = BufferedByteOutputStream.wrapOutputStream(os, 100,
        10);

    dos.close();
    dos.close(); // can close multiple times
    
    try {
      // this will cause to flush BufferedOutputStream
      for (int i = 0; i < 10000; i++) {
        dos.write(1);
      }
      fail("Write should fail");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
    
    try {
      // this will cause to flush BufferedOutputStream
      for (int i = 0; i < 1000; i++) {
        dos.write(new byte[10], 0, 10);
      }
      fail("Write should fail");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
    
    try {
      dos.flush();
      fail("Flush should fail");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
  }
  
  private long testBuffer(final int inputSize, final int bufferSize,
      final int fixedTempBufferSize, final boolean writeOutput)
      throws Exception {
    LOG.info("Running test raw buffer with input size: " + inputSize
        + ", buffer size: " + bufferSize);
    setUp(inputSize);
    final BufferedByteInputOutput buffer = new BufferedByteInputOutput(bufferSize);
    
    Callable<Void> writerThread = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        int totalWritten = 0, inputCursor = 0;
        while (totalWritten < inputSize) {
          if (rand.nextBoolean()) {
            // write single byte
            buffer.write(input[inputCursor++]);
            totalWritten++;
          } else {
            int count;
            if (fixedTempBufferSize > 0) {
              count = Math.min(inputSize - totalWritten, rand.nextInt(fixedTempBufferSize) + 1);
            } else {
              count = rand.nextInt(inputSize - totalWritten) + 1;
            }
            buffer.write(input, inputCursor, count);
            inputCursor += count;
            totalWritten += count;
          }
        } 
        buffer.close();
        return null;
      }
    };
    Callable<Void> readerThread = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        int totalRead = 0, outputCursor = 0;
        while (totalRead < inputSize) {
          if (rand.nextBoolean()) {
            // read single byte
            int b = buffer.read();
            assertFalse(b == -1);
            output[outputCursor++] = (byte) b;
            totalRead++;
          } else {
            int count;
            if (fixedTempBufferSize > 0) {
              count = rand.nextInt(fixedTempBufferSize) + 1;
            } else {
              count = rand.nextInt(inputSize - totalRead) + 1;
            }
            byte[] bytes = new byte[count];
            int bytesRead = buffer.read(bytes, 0, count);
            assertFalse(bytesRead == -1);
            if (writeOutput) {
              System.arraycopy(bytes, 0, output, outputCursor, bytesRead);
              outputCursor += bytesRead;
            }           
            totalRead += bytesRead;
          }
        }     
        return null;
      }
    };

    assertEquals(0, buffer.available());
    
    //////////////// run writer and reader
    long start = System.currentTimeMillis();
    ExecutorService executor = Executors.newFixedThreadPool(2); 
    Future<Void> readerFuture = executor.submit(readerThread); 
    Future<Void> writerFuture = executor.submit(writerThread);   
    readerFuture.get();  
    writerFuture.get(); 
    long stop = System.currentTimeMillis();
    ////////////////

    if (writeOutput) {
      assertTrue(Arrays.equals(input, output));
    }
    
    // check written and read bytes
    assertEquals(inputSize, buffer.totalRead());
    assertEquals(inputSize, buffer.totalWritten());
    
    buffer.close();
    
    // should get -1 after closing for reads 
    // since everything has been read
    assertEquals(-1, buffer.read());
    assertEquals(-1, buffer.read(new byte[10], 0, 10));
    
    // should fail writes
    try {
      buffer.write(1);
      fail("Should get exception after closing");
    } catch (IOException e) {
      LOG.info("Expected exception " + e.getMessage());
    }
    
    try {
      buffer.write(new byte[10], 0, 10);
      fail("Should get exception after closing");
    } catch (IOException e) {
      LOG.info("Expected exception " + e.getMessage());
    }
    
    assertTrue(buffer.isClosed());
    return stop - start;
  }


  private void testInputStream(int inputSize, int bufferSize, int readBufferSize)
      throws IOException {
    LOG.info("Running test input stream with inputSize: " + inputSize
        + ", bufferSize: " + bufferSize + ", readBufferSize: " + readBufferSize);
    setUp(inputSize);
    ByteArrayInputStream is = new ByteArrayInputStream(input);
    DataInputStream dis = BufferedByteInputStream.wrapInputStream(is,
        bufferSize, readBufferSize);

    int totalRead = 0;
    int outputCursor = 0;
    while (totalRead < inputSize) {
      if (rand.nextBoolean()) {
        // read single byte
        output[outputCursor++] = dis.readByte();
        totalRead++;
      } else {
        int count = rand.nextInt(inputSize - totalRead) + 1;
        byte[] bytes = new byte[count];
        int bytesRead = dis.read(bytes, 0, count);
        System.arraycopy(bytes, 0, output, outputCursor, bytesRead);
        outputCursor += bytesRead;
        totalRead += bytesRead;
      }
    }

    assertEquals(inputSize, totalRead);
    assertTrue(Arrays.equals(input, output));
    
    dis.close();
    dis.close(); // multiple close should work
    dis.close(); // multiple close should work
  }

  private void testOutputStream(int inputSize, int bufferSize,
      int writeBufferSize, boolean closeAndCheck) throws IOException {
    TestBufferedByteInputOutputInjectionHandler h = 
        new TestBufferedByteInputOutputInjectionHandler();
    InjectionHandler.set(h);
    LOG.info("Running test output stream with inputSize: " + inputSize
        + ", bufferSize: " + bufferSize + ", readBufferSize: "
        + writeBufferSize);
    setUp(inputSize);
    ByteArrayOutputStream os = new ByteArrayOutputStream(inputSize);
    DataOutputStream dos = BufferedByteOutputStream.wrapOutputStream(os,
        bufferSize, writeBufferSize);

    int totalWritten = 0;
    int inputCursor = 0;
    while (totalWritten < inputSize) {
      if (rand.nextBoolean()) {
        // write single byte
        dos.write(input[inputCursor++]);
        totalWritten++;
      } else {
        int count = rand.nextInt(inputSize - totalWritten) + 1;
        dos.write(input, inputCursor, count);
        inputCursor += count;
        totalWritten += count;
      }
      
      // random flush
      if (rand.nextBoolean()) {
        h.bytesFlushed = -1;
        dos.flush();
        assertEquals(totalWritten, h.bytesFlushed);
      }
    }

    if (closeAndCheck) {
      // we either close
      dos.close();
    } else {
      // or need to flush the stream
      dos.flush();
    } // in either case data must be out

    assertEquals(inputSize, totalWritten);
    assertTrue(Arrays.equals(input, os.toByteArray()));
    
    // close
    dos.close();
    dos.close(); // should be fine to call it multiple times
    dos.close(); 
  }
  
  private int rand(int n) {
    return rand.nextInt(n) + 1;
  }
  
  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      LOG.info("Interrupted exception", e);
      Thread.currentThread().interrupt();
    }
  }
  
  class TestBufferedByteInputOutputInjectionHandler extends InjectionHandler {
    volatile long bytesFlushed;
    
    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEventCore.BUFFEREDBYTEOUTPUTSTREAM_FLUSH) {
        bytesFlushed = (Long)args[0];
      }
    }
  }
}
