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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *  A record reader that returns the key-value pairs of an underlying
 *  record reader in random order. It first reads all the key-value pairs
 *  into memory and shuffles them. So the split should not be too large.
 */
public class ShuffleRecordReader<K, V> extends RecordReader<K, V> {
  static class Pair<K, V> {
    K key;
    V value;
    Pair(K key, V value) {
      this.key = key;
      this.value = value;
    }
  }
  
  private RecordReader<K, V> underlying;
  private List<Pair<K, V>> data;
  private int position;
  private Pair<K, V> currentPair;

  public ShuffleRecordReader(RecordReader<K, V> underlying) {
    this.underlying = underlying;
    this.data = new ArrayList<Pair<K,V>>();
    this.position = 0;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    underlying.initialize(split, context);
    // Read all underlying data.
    while (underlying.nextKeyValue()) {
      K key = underlying.getCurrentKey();
      V value = underlying.getCurrentValue();
      data.add(new Pair<K, V>(key, value));
    }
    // Shuffle now.
    Collections.shuffle(data);
  }

  @Override
  public void close() throws IOException {
    this.underlying.close();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return currentPair.key;
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return currentPair.value;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (position >= data.size()) {
      return false;
    }
    currentPair = data.get(position);
    position++;
    return true;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (data.isEmpty()) {
      return 0.0f;
    } else {
      int size = data.size();
      return position * 1.0f / size;
    }
  }
}
