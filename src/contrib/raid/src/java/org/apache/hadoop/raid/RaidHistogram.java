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
package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;

public class RaidHistogram {
  protected static final Log LOG = LogFactory.getLog(RaidHistogram.class);
  // how much we divide from the value 
  public final static String RAID_HISTOGRAM_DIVIDEND_KEY =
      "hdfs.raid.histogram.dividend";
  public final static long RECOVERY_FAIL = Integer.MAX_VALUE;
  // it's unnecessary to record recovery time at msec level
  // Use second as unit should be enough
  public final static int DEFAULT_RAID_HISTOGRAM_DIVIDEND = 1000;
  public class Point implements Comparable<Point> {
    public long time;
    public long value;
    public String path;
    public String taskId;
    public Point(long newValue, String newPath, long newTime, String newTaskId) {
      value = newValue;
      path = newPath;
      time = newTime;
      taskId = newTaskId;
    }
    
    public int compareTo(Point otherPoint) {
      if (this.time > otherPoint.time) return 1;
      if (this.time < otherPoint.time) return -1;
      if (this == otherPoint) return 0;
      if (this.value > otherPoint.value) return 1;
      if (this.value < otherPoint.value) return -1;
      return this.path.compareTo(otherPoint.path);
    }
  }
  
  public class CounterArray {
    private AtomicInteger[] counters;
    private int length;
    public CounterArray(int newLength) {
      length = newLength;
      counters = new AtomicInteger[length];
      for (int i = 0; i < length; i++) {
        counters[i] = new AtomicInteger(0);
      }
    }
    
    public int decrementAndGet(int index) {
      return counters[index].decrementAndGet();
    }
    
    public int incrementAndGet(int index) {
      return counters[index].incrementAndGet();
    }
    
    public AtomicInteger get(int index) {
      return counters[index];
    }
  }
  
  /**
   * Status used by web ui 
   */
  public class BlockFixStatus {
    // Used for displaying histogram graph
    public int[] counters;
    // values at X%  
    public long[] percentValues;
    // array of percents shown
    public ArrayList<Float> percents;
    // number of failed recovery paths
    public int failedPaths;
    public BlockFixStatus(int[] newCounters, long[] newPercentValues,
        ArrayList<Float> newPercents, int newFailedPaths) {
      counters = newCounters;
      percentValues = newPercentValues;
      percents = newPercents;
      failedPaths = newFailedPaths;
    }
  }
  public ConcurrentSkipListSet<Point> points;
  public ConcurrentHashMap<String, AtomicInteger> failedRecoveredFiles;
  // Record the total number of failed paths for every window
  public CounterArray totalFailedPaths;
  // how large we monitor recent 5hours/1day/1week
  public ArrayList<Long> windows;
  public int dividend = DEFAULT_RAID_HISTOGRAM_DIVIDEND;
  // how many windows we monitor
  public int windowNum; 
  public ConcurrentSkipListMap<Long, CounterArray> histo;
  // Record the total number of points for every window
  public CounterArray totalPoints;
  public RaidHistogram(ArrayList<Long> newWindows, Configuration conf)
      throws Exception {
    initialize(newWindows);
    dividend = conf.getInt(RAID_HISTOGRAM_DIVIDEND_KEY, 
        DEFAULT_RAID_HISTOGRAM_DIVIDEND);
  }
  
  public synchronized int getNumberOfPoints() {
    return points.size();
  }
  
  public synchronized void initialize(ArrayList<Long> newWindows) {
    windows = newWindows;
    Collections.sort(windows);
    points = new ConcurrentSkipListSet<Point>();
    windowNum = windows.size();    
    totalPoints = new CounterArray(windowNum);
    totalFailedPaths = new CounterArray(windowNum);
    histo = new ConcurrentSkipListMap<Long, CounterArray>();
    failedRecoveredFiles = new ConcurrentHashMap<String, AtomicInteger>();
  }
  
  // Only for testing
  public synchronized void setNewWindows(ArrayList<Long> newWindows)
      throws IOException {
    if (newWindows.size() != windows.size()) {
      throw new IOException(
          "Number of new windows need to be the same as that of old ones");
    }
    Collections.sort(newWindows);
    for (int i = 0; i < newWindows.size(); i++) {
      if (newWindows.get(i) > windows.get(i)) {
        throw new IOException ("New window " + newWindows.get(i) +
          " should be smaller than the old one " + windows.get(i)); 
      }
      windows.set(i, newWindows.get(i));
    }
  }
  
  public synchronized ArrayList<Point> getPointsWithGivenRecoveryTime(
      long recoveryTime) {
    ArrayList<Point> resultPoints = new ArrayList<Point>();
    Iterator<Point> iterator = this.points.iterator();
    while (iterator.hasNext()) {
      Point p = iterator.next();
      if (p.value == recoveryTime) {
        resultPoints.add(p);
      }
    }
    return resultPoints;
  }
  
  /*
   * If value is RECOVERY_FAIL, we consider it as recovery failure
   */
  public synchronized void put(String path, long value, String taskId) {
    Point p;
    int last = windowNum - 1;
    if (value == RECOVERY_FAIL) {
      p = new Point(value, path, System.currentTimeMillis(), taskId);
      AtomicInteger counter = failedRecoveredFiles.get(path);
      if (counter == null) {
        counter = new AtomicInteger(0);
        failedRecoveredFiles.put(path, counter);
      }
      if (counter.incrementAndGet() == 1) {
        totalFailedPaths.get(last).incrementAndGet();
      }
    } else {
      value /= dividend;
      p = new Point(value, path, System.currentTimeMillis(), taskId);
      CounterArray counters = histo.get(value);
      if (counters == null) {
        counters = new CounterArray(windowNum);
        histo.put(value, counters);
      }
      counters.incrementAndGet(last);
      totalPoints.incrementAndGet(last);
    }
    points.add(p);
    InjectionHandler.processEvent(InjectionEvent.RAID_SEND_RECOVERY_TIME, this, path, 
        value, taskId);
  }
  
  public synchronized void filterStalePoints(long endTime) throws IOException {
    int last = windowNum - 1;
    long windowTime = windows.get(last);
    NavigableSet<Point> windowSet =
        points.headSet(new Point(0, "", endTime - windowTime, null)); 
    Iterator<Point> windowIterator = windowSet.iterator();
    while (windowIterator.hasNext()) {
      Point p = windowIterator.next();
      if (p.value == RECOVERY_FAIL) {
        AtomicInteger ca = failedRecoveredFiles.get(p.path);
        if (ca == null) {
          throw new IOException(p.path +
              " doesn't have counter in failedRecoveredFiles");
        }
        if (ca.decrementAndGet() == 0) {
          totalFailedPaths.decrementAndGet(last);
          failedRecoveredFiles.remove(p.path);
        }
      } else {
        CounterArray ca = histo.get(p.value);
        if (ca == null) {
          throw new IOException(p.value + " doesn't have counter in histo");
        }
        if (ca.decrementAndGet(last) == 0) {
          histo.remove(p.value);
        }
        totalPoints.decrementAndGet(last);
      }
      points.remove(p);
    }
  }
  
  public synchronized void collectCounters(long endTime) throws IOException {
    for (int i = 0; i < windowNum - 1; i++) {
      long windowTime = windows.get(i);
      // reset totalFailedPaths and totalPoints
      totalFailedPaths.get(i).set(0);
      totalPoints.get(i).set(0);
      NavigableSet<Point> windowSet =
          points.tailSet(new Point(0, "", endTime - windowTime, null)); 
      Iterator<Point> windowIterator = windowSet.iterator();
      Set<String> failedRecoveredFiles = new HashSet<String>();
      while (windowIterator.hasNext()) {
        Point p = windowIterator.next();
        if (p.value == RECOVERY_FAIL) {
          if (failedRecoveredFiles.add(p.path)) {
            totalFailedPaths.incrementAndGet(i);
          }
        } else {
          CounterArray ca = histo.get(p.value);
          if (ca == null) {
            throw new IOException(p.value + " doesn't have counter in histo");
          }
          ca.incrementAndGet(i);
          totalPoints.incrementAndGet(i);
        }
      }
    }
  }
  
  public synchronized TreeMap<Long, BlockFixStatus> getBlockFixStatus(
      int histoLen, ArrayList<Float> percents, long endTime)
          throws IOException {
    TreeMap<Long, BlockFixStatus> blockFixStatuses =
        new TreeMap<Long, BlockFixStatus>();
    filterStalePoints(endTime);
    collectCounters(endTime);
    for (int i = 0 ;i < windowNum; i++) {
      blockFixStatuses.put(windows.get(i),
          new BlockFixStatus(null, null, percents,
              totalFailedPaths.get(i).get()));
    }
    int percentsNum = percents.size();
    if (percentsNum == 0 || histo.size() == 0 ) {
      return blockFixStatuses;
    }
    Collections.sort(percents);
    long[] percentThresholds = new long[windowNum];
    int[] percentIndexes = new int[windowNum];
    int[][] counters = new int[windowNum][];
    long[][] percentValues = new long[windowNum][];
    for (int i = 0; i < windowNum; i++) {
      percentIndexes[i] = 0;
      percentThresholds[i] = (long)(percents.get(0) * totalPoints.get(i).get());
      counters[i] = new int[histoLen];
      percentValues[i] = new long[percentsNum];
      Arrays.fill(percentValues[i], -1);
    }
    long width = (long)Math.ceil(histo.lastKey() * 1.0 / histoLen);
    int startIdx = 0;
    // iterate the histo
    Iterator<Entry<Long, CounterArray>> it = histo.entrySet().iterator();
    int[] currentCounter = new int[windowNum];
    ArrayList<Integer> counterIndexes = new ArrayList<Integer>();
    for (int i = 0; i < windowNum; i++) {
      counterIndexes.add(i);
    }
    while (it.hasNext()) {
      Entry<Long, CounterArray> pairs = (Entry<Long, CounterArray>)it.next();
      Long recoveryTime = (Long)pairs.getKey();
      CounterArray counter = (CounterArray)pairs.getValue();
      if (startIdx * width + width <= recoveryTime && startIdx + 1 < histoLen) {
        startIdx++;
      }
      
      Iterator<Integer> iter = counterIndexes.iterator();
      while (iter.hasNext()) {
        int idx = iter.next();
        currentCounter[idx] += counter.counters[idx].get();
        counters[idx][startIdx] += counter.counters[idx].get();
        if (currentCounter[idx] >= percentThresholds[idx] && currentCounter[idx] > 0) {
          percentValues[idx][percentIndexes[idx]] = recoveryTime;
          percentIndexes[idx]++;
          if (percentIndexes[idx] == percentsNum) {
            percentThresholds[idx] = RECOVERY_FAIL;
            iter.remove();
          } else {
            percentThresholds[idx] =
                (long)(percents.get(percentIndexes[idx]) *
                    totalPoints.get(idx).get());
          }
        }
      }
      // reset counters except the last window
      for (int i = 0; i < windowNum - 1; i++) {
        counter.get(i).set(0);
      }
    }
    for (int i = 0 ;i < windowNum; i++) {
      // Fill the values if all data are scanned.
      if (percentIndexes[i] > 0 && percentIndexes[i] < percentsNum) {
        for (int j = percentIndexes[i]; j < percentsNum; j++) {
          percentValues[i][j] = percentValues[i][percentIndexes[i] - 1];
        }
      }
      blockFixStatuses.put(windows.get(i),
          new BlockFixStatus(counters[i], percentValues[i], percents,
              totalFailedPaths.get(i).get()));
    }
    return blockFixStatuses;
  }
}