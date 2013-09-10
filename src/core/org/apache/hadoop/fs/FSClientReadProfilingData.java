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

import java.util.ArrayList;
import java.util.List;

public class FSClientReadProfilingData {
  
  private final List<FSDataNodeReadProfilingData> dataNodeProfilingDataList;
  
  public FSClientReadProfilingData() {
    this.dataNodeProfilingDataList = new ArrayList<FSDataNodeReadProfilingData>();
  }
  
  public void addDataNodeReadProfilingData(FSDataNodeReadProfilingData dnData) {
    this.dataNodeProfilingDataList.add(dnData);
  }
  
  public List<FSDataNodeReadProfilingData> getDataNodeReadProfilingDataList() {
    return dataNodeProfilingDataList;
  }
  
  private long startReadTime = 0L;
  private long startTraceTime = 0L;
  public void startRead() {
    startReadTime = System.nanoTime();
    startTraceTime = System.nanoTime();
  }
  
  public void endRead() {
    readTime += (System.nanoTime() - startReadTime);
  }
  
  private long readTime = 0L;
  
  public long getReadTime() {
    return readTime;
  }
  
  // sequence read profiling time
  
  private long readBlockSeekToTime = 0L;
  public void recordReadBlockSeekToTime() {
    long curTime = System.nanoTime();
    readBlockSeekToTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getReadBlockSeekToTime() {
    return readBlockSeekToTime;
  }
  
  private long readBufferTime = 0L;
  public void recordReadBufferTime() {
    long curTime = System.nanoTime();
    readBufferTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getReadBufferTime() {
    return readBufferTime;
  }
  
  // pread profiling time
  private long preadBlockSeekContextTime = 0L;
  public void recordPreadBlockSeekContextTime() {
    long curTime = System.nanoTime();
    preadBlockSeekContextTime += curTime - startTraceTime;
    startTraceTime = curTime;
  } 
  
  public long getPreadBlockSeekContextTime() {
    return preadBlockSeekContextTime;
  }
  
  private long preadChooseDataNodeTime = 0L;
  public void recordPreadChooseDataNodeTime() {
    long curTime = System.nanoTime();
    preadChooseDataNodeTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getPreadChooseDataNodeTime() {
    return preadChooseDataNodeTime;
  }
  
  private long preadActualGetFromOneDNTime = 0L;
  public void recordPreadActualGetFromOneDNTime() {
    long curTime = System.nanoTime();
    preadActualGetFromOneDNTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getPreadActualGetFromOneDNTime() {
    return preadActualGetFromOneDNTime;
  }
  
  private long preadGetBlockReaderTime = 0L;
  public void recordPreadGetBlockReaderTime() {
    long curTime = System.nanoTime();
    preadGetBlockReaderTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getPreadGetBlockReaderTime() {
    return preadGetBlockReaderTime;
  }
  
  private long preadAllTime = 0L;
  public void recordPreadAllTime() {
    long curTime = System.nanoTime();
    preadAllTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getPreadAllTime() {
    return preadAllTime;
  }
  
  // block reader profiling time
  private long readChunkTime = 0L;
  public void recordReadChunkTime() {
    long curTime = System.nanoTime();
    readChunkTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getReadChunkTime() {
    return readChunkTime;
  }
  
  private long verifyChunkCheckSumTime = 0L;
  public void recordVerifyChunkCheckSumTime() {
    long curTime = System.nanoTime();
    verifyChunkCheckSumTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getVerifyChunkCheckSumTime() {
    return verifyChunkCheckSumTime;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    
    sb.append("total_read_time: " ).append(this.readTime)
      .append("\nDFSClient_block_seek_to: ").append(this.readBlockSeekToTime)
      .append("\nDFSClient_read_buffer: ").append(this.readBufferTime)
      .append("\nDFSClient_read_chunk: ").append(this.readChunkTime)
      .append("\nDFSClient_pread_block_seek_context: ").append(this.preadBlockSeekContextTime)
      .append("\nDFSClient_pread_choose_datanode: ").append(this.preadChooseDataNodeTime)
      .append("\nDFSClient_pread_get_block_reader: ").append(this.preadGetBlockReaderTime)
      .append("\nDFSClient_pread_actual_get_from_one_datanode: ").append(this.preadActualGetFromOneDNTime)
      .append("\nDFSClient_pread_all: ").append(this.preadAllTime)
      .append("\n\n");
    
    if (dataNodeProfilingDataList.size() > 0) {
      sb.append(dataNodeProfilingDataList.size()).append(" Datanodes involved.");
      for (int i = 0; i < dataNodeProfilingDataList.size(); i++) {
        sb.append("\nDatanode ").append(i + 1);
        sb.append("\n").append(dataNodeProfilingDataList.get(i));
      }
    }
  
    return sb.toString();
  }
}
