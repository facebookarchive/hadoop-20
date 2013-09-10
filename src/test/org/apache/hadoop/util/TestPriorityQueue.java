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
package org.apache.hadoop.util;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.WritableComparator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test to verify that the Floyd priority queue 
 * algorithm.
 */
public class TestPriorityQueue {
  
  class IntPriorityQueue extends PriorityQueue<Integer> {
    protected boolean lessThan(Object a, Object b) {
      Integer i = (Integer) a;
      Integer j = (Integer) b;
      return i.intValue() < j.intValue();
    }
  }
  
  class IntPriorityQueueFloyd extends PriorityQueueFloyd<Integer> {
    protected boolean lessThan(Object a, Object b) {
      Integer i = (Integer) a;
      Integer j = (Integer) b;
      return i.intValue() < j.intValue();
    }
  }
  
  @Test
    public void testCorrectness() throws Exception {
    
    IntPriorityQueue pQ = new IntPriorityQueue();
    IntPriorityQueueFloyd fQ = new IntPriorityQueueFloyd();
    
    pQ.initialize(8);
    fQ.initialize(8);
    
    /* Expecting
     *             2
     *          4      6
     *        8  10  12  14
     *      16
     */
    for (int i = 1; i <=8; i++ ) {
      pQ.insert(Integer.valueOf(i * 2));
      fQ.insert(Integer.valueOf(i * 2));
    }
    Assert.assertEquals("After inserting even",  
                        pQ.toString(), 
                        fQ.toString());
    System.out.println("new = " + fQ.toString());
    
    /* 
     * Expecting
     * 
     *             10
     *          11      12
     *        16  13  15  14
     *      17
     */
    for (int i = 1; i <= 8; i++) {
      Integer elem = Integer.valueOf(i * 2 + 1);
      pQ.insert(elem);
      fQ.insert(elem);
    }
    
    Assert.assertEquals("After inserting odd",  
                        pQ.toString(), 
                        fQ.toString());
    System.out.println("new = " + fQ.toString());
    
    // random sampling test
    Random rand = new Random(395788);
    for (int i = 1; i < 100000; i++) {
      Integer elem = Integer.valueOf(rand.nextInt(100000));
      pQ.insert(elem);
      fQ.insert(elem);
    }
    Assert.assertEquals("After inserting odd",  
                        pQ.toString(), 
                        fQ.toString());
    System.out.println("new = " + fQ.toString());
  }
  
  /**
   * Performance tests to compare performance of the Typical PriorityQueue
   * and Floyd version of PriorityQueue.
   * 
   * To compile this class:
   * 
   *   ant test-core -Dtestcase=TestPriorityQueue
   *   
   * This can be run from the command line with:
   *
   *   java -cp build/hadoop-0.20-test.jar:build/hadoop-0.20-core.jar \
   *      -Xms10g -Xmx10g \
   *      'org.apache.hadoop.util.TestPriorityQueue$PerformanceTest'
   */
  public static class PerformanceTest {
    
    /**
     * Ordered input stream -- simulating map output Segment
     */
    class Segment {
      ArrayList<byte[]> inputs;
      int index;
      int maxIndex;
      
      Segment(int max_len) {
        reset();
        maxIndex = max_len;
        inputs = new ArrayList<byte[]>(max_len);
      }

      byte[] top() {
        return inputs.get(index);
      }

      boolean hasNext() {
        return index < maxIndex;
      }

      byte[] next() {
        byte[] elem = top();
        pop();
        return elem;
      }

      void pop() {
        index++;
      }

      void reset() {
        index = 0;
      }

      void push(byte[] elem) {
        inputs.add(elem);
      }

      void setMaxLen(int maxIndex) {
        this.maxIndex = maxIndex;
      }
    }

    class SegmentPriorityQueue extends PriorityQueue<Segment> {
      protected boolean lessThan(Object a, Object b) {
        Segment s1 = (Segment) a;
        Segment s2 = (Segment) b;
        byte[] b1 = (byte[]) s1.top();
        byte[] b2 = (byte[]) s2.top();
        int r = WritableComparator.compareBytes(b1, 0, b1.length, 
                                                b2, 0, b2.length);
        return r < 0;
      }
    }

    class SegmentPriorityQueueFloyd extends PriorityQueueFloyd<Segment> {
      protected boolean lessThan(Object a, Object b) {
        Segment s1 = (Segment) a;
        Segment s2 = (Segment) b;
        byte[] b1 = (byte[]) s1.top();
        byte[] b2 = (byte[]) s2.top();
        int r = WritableComparator.compareBytes(b1, 0, b1.length, 
                                                b2, 0, b2.length);
        return r < 0;
      }
    }

    Segment[] segments;
    PerformanceTest(int queueSize, int maxLength) {
      segments = genInputSegments(queueSize, maxLength);
    }

    public static void main(String[] args) {
      int QUEUE_SIZE = 24;
      int MAX_INPUT_SIZE = 1000000;
      PerformanceTest bench = new PerformanceTest(QUEUE_SIZE, MAX_INPUT_SIZE);
      printHeader();

      for (int i = 1000; i <= MAX_INPUT_SIZE; i *= 10) {
        bench.doBenchmark(i, System.out);
      }

      QUEUE_SIZE = 100;
      bench = new PerformanceTest(QUEUE_SIZE, MAX_INPUT_SIZE);

      for (int i = 1000; i <= MAX_INPUT_SIZE; i *= 10) {
        bench.doBenchmark(i, System.out);
      }
    }
    
    private static void printHeader() {
      System.out.printf("\nPerformance Table (msec)\n");
      printCell("Queue Size", 0, System.out);
      printCell("Input Size", 0, System.out);
      printCell("PriorityQueue", 0, System.out);
      printCell("PriorityQueueFloyd", 0, System.out);
      printCell("Improvements", 0, System.out);
      System.out.printf("\n");
    }


    /**
     * Bench the CPU time of original vs Floyd variation of priority queues. 
     * Each benchmark depends on 2 input variables:
     * @param queueSize is the number of input streams
     * @param inputLen is the size of each input stream.
     */
    void doBenchmark(int inputLen, PrintStream out) {
      long begin_time, end_time;
      double pq_ms, fq_ms;
      
      // limit max input size
      for (Segment s : segments) {
        s.setMaxLen(inputLen);
      }

      int queueSize = segments.length;
      printCell(String.valueOf(queueSize), "Queue Size".length(), out);
      printCell(String.valueOf(inputLen), "Input Size".length(), out);

      SegmentPriorityQueue pQ = new SegmentPriorityQueue();
      pq_ms = measureSort(pQ, queueSize);

      printCell(String.format("%9.2f", pq_ms), 
                "PriorityQueue".length(), out);

      // reset segments
      for (Segment s : segments) {
        s.reset();
      }

      SegmentPriorityQueueFloyd fQ = new SegmentPriorityQueueFloyd();
      fq_ms = measureSort(fQ, queueSize);
      
      printCell(String.format("%9.2f", fq_ms), 
                "PriorityQueueFloyd".length(), out);
      
      printCell(String.format("%5.2f%%", (100.0d*(pq_ms-fq_ms)/pq_ms)), 
                "Improvements".length(), out);
      
      System.out.printf("\n");
    }

    private final double measureSort(PriorityQueue<Segment> pQ, int queueSize) {
      pQ.initialize(queueSize);
      long begin_time = System.nanoTime();
      mergeSort(pQ, segments);
      long end_time = System.nanoTime();
      return (end_time - begin_time) / 1000000.0d;
    }

    void mergeSort(PriorityQueue<Segment> queue, Segment[] segments) {
      for (Segment s : segments) {
        queue.insert(s);
      }
      do {
        Segment minSegment = queue.top();
        if (minSegment == null) {
          break;
        }
        minSegment.next();
        if (minSegment.hasNext()) {
          queue.adjustTop();
        } else {
          queue.pop();
        }
      } while (true);
    }

    /**
     * Prepare the input streams in main memory so that we only measure
     * the priority queue time.
     * @param queueSize is the number of input streams
     * @param inputLen is the size of each input stream.
     * @return list of ordered input streams
     */
    Segment[] genInputSegments(int queueSize, int inputLen) {
      Segment[] segments = new Segment[queueSize];
      Random rand = new Random(385902);
      for (int i = 0; i < queueSize; i++) {
        Segment segment = genSortedList(inputLen, rand.nextInt(1000));
        segments[i] = segment;
      }
      return segments;
    }

    /**
     * Generate an ordered list of byte arrays.
     * @param head the smallest number
     * @return list of ordered byte arrays representing 0-padded ASCII number
     * representation. 
     */
    public Segment genSortedList(int len, long head) {
      Segment segment = new Segment(len);
      Random rand = new Random(93854);
      long l = head;
      for (int i = 0; i < len; ++i) {
        byte[] elem = zeroPaddedArray(l);
        segment.push(elem);
        long l2 = l + rand.nextInt(10000);
        if (l2 < l) { // overflow
          break;
        }
        l = l2;
      }
      return segment;
    }

    private byte[] zeroPaddedArray(long l) {
      byte[] asciiLong = Long.toString(l).getBytes();
      byte[] elem = new byte[20]; // Long.MAX = 9,223,372,036,854,775,807
      if (asciiLong.length < elem.length) {
        Arrays.fill(elem, 0, elem.length - asciiLong.length, (byte)'0');
      }
      System.arraycopy(asciiLong, 
                       0, 
                       elem, 
                       elem.length - asciiLong.length, 
                       asciiLong.length);
      return elem;
    }

    private static void printCell(String s, int width, PrintStream out) {
      final int w = s.length() > width? s.length(): width;
      out.printf(" %" + w + "s |", s);
    }
  }
}
