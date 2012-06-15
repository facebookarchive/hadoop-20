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

public class MemoryBlockIndex {
  MemoryBlock memoryBlock;
  int index;

  public MemoryBlockIndex(MemoryBlock memoryBlock, 
      int offset) {
    this.memoryBlock = memoryBlock;
    this.index = offset;
  }

  public MemoryBlockIndex() {
    this.memoryBlock = null;
    index = -1;
  }

  public MemoryBlock getMemoryBlock() {
    return memoryBlock;
  }

  public int getIndex() {
    return index;
  }

  public void setMemoryBlockIndex(MemoryBlock memoryBlock, int index) {
    this.memoryBlock = memoryBlock;
    this.index = index;
  }
}
