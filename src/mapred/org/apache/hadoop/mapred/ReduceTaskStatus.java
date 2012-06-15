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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



class ReduceTaskStatus extends TaskStatus {

  private long shuffleFinishTime; 
  private long sortFinishTime;
  // Copy and reduce rates are in bytes/ms. Sort processing rate is in % of
  // progress per ms
  private double copyProcessingRate;
  private double sortProcessingRate;
  private double reduceProcessingRate;
  private List<TaskAttemptID> failedFetchTasks = new ArrayList<TaskAttemptID>(1);
  
  public ReduceTaskStatus() {}

  public ReduceTaskStatus(TaskAttemptID taskid, float progress, int numSlots,
                          State runState, String diagnosticInfo, String stateString, 
                          String taskTracker, Phase phase, Counters counters) {
    super(taskid, progress, numSlots, runState, diagnosticInfo, stateString, 
          taskTracker, phase, counters);
  }

  @Override
  public Object clone() {
    ReduceTaskStatus myClone = (ReduceTaskStatus)super.clone();
    myClone.failedFetchTasks = new ArrayList<TaskAttemptID>(failedFetchTasks);
    return myClone;
  }

  @Override
  public boolean getIsMap() {
    return false;
  }

  @Override
  void setFinishTime(long finishTime) {
    if (shuffleFinishTime == 0) {
      this.shuffleFinishTime = finishTime; 
    }
    if (sortFinishTime == 0){
      this.sortFinishTime = finishTime;
    }
    super.setFinishTime(finishTime);
  }

  @Override
  public long getShuffleFinishTime() {
    return shuffleFinishTime;
  }

  @Override
  void setShuffleFinishTime(long shuffleFinishTime) {
    this.shuffleFinishTime = shuffleFinishTime;
  }

  @Override
  public long getSortFinishTime() {
    return sortFinishTime;
  }

  @Override
  void setSortFinishTime(long sortFinishTime) {
    this.sortFinishTime = sortFinishTime;
    if (0 == this.shuffleFinishTime){
      this.shuffleFinishTime = sortFinishTime;
    }
  }

  //for copy phase using bytes copied divided by the time as the processing 
  // rate
  @Override
  public double getCopyProcessingRate(long currentTime) {
    @SuppressWarnings("deprecation")
    long bytesCopied = super.getCounters().findCounter
        (Task.Counter.REDUCE_SHUFFLE_BYTES).getCounter();
    long timeSpentCopying = 0;
    long startTime = getStartTime();
    if(getPhase() == Phase.SHUFFLE) {
      if (currentTime <= startTime) {
        LOG.error("current time is " + currentTime + ", which is <= start " +
            "time " + startTime + " in " + this.getTaskID());
      }
      timeSpentCopying =  currentTime - startTime;
    } else {
      //shuffle phase is done
      long shuffleFinishTime = getShuffleFinishTime();
      if (shuffleFinishTime <= startTime) {
        LOG.error("Shuffle finish time is " + shuffleFinishTime + 
            ", which is <= start time " + startTime + 
            " in " + this.getTaskID());
        return 0;
      }
      timeSpentCopying = shuffleFinishTime - startTime;
    }
    copyProcessingRate = bytesCopied/timeSpentCopying;
    return copyProcessingRate;
  }
   
   // for sort phase, use the accumulated progress rate as the processing rate
   @Override
   public double getSortProcessingRate(long currentTime) {
     long timeSpentSorting = 0;
     float progress = 0;
     Phase phase = getPhase();
     long sortFinishTime = getSortFinishTime();
     long shuffleFinishTime = getShuffleFinishTime();
     
     if (phase == Phase.SHUFFLE ) {
       return 0;
     } else if (getPhase() == Phase.SORT) {
       if (shuffleFinishTime < currentTime) {
         LOG.error("Shuffle finish time is " + shuffleFinishTime + 
             " which is < current time " + currentTime + 
             " in " + this.getTaskID());
       }
       timeSpentSorting =  currentTime - shuffleFinishTime;
       progress = getProgress() - (float)1.0/3;
       if (progress < 0) {
         LOG.error("Shuffle progress calculated to be " + progress + 
             " in task status for  " + this.getTaskID() + ".  Settings to 0");
         progress = 0;
       }
     } else if (getPhase() == Phase.REDUCE) {
       // when it is reduce phase, use 33%/(sort finish time - shuffle 
       // finish time as the progress rate. Using percentages instead of bytes
       // as it is tricky
       progress = (float)1.0/3;
       if (shuffleFinishTime <= sortFinishTime) {
         LOG.error("Shuffle finish fime is " + shuffleFinishTime + 
             " which is <= sort finish time " + sortFinishTime + 
             " in " + this.getTaskID());
         return 0;
       }
       timeSpentSorting = sortFinishTime - shuffleFinishTime;
     }
     
     sortProcessingRate = progress/timeSpentSorting; 
     return sortProcessingRate;
   }

   // for reduce phase, using bytes read by calling next() divided by the time 
   // as the processing rate
   @Override
   public double getReduceProcessingRate(long currentTime) {
     Phase phase = getPhase();
     
     if (phase != Phase.REDUCE) {
       return 0;
     }
     
     @SuppressWarnings("deprecation")
    long bytesProcessed = super.getCounters().findCounter
         (Task.Counter.REDUCE_INPUT_BYTES).getCounter();
     long timeSpentInReduce = 0;
     long sortFinishTime = getSortFinishTime();
     if (sortFinishTime >= currentTime) {
       LOG.error("Sort finish time is " + sortFinishTime + 
           " which is >= current time " + currentTime + 
           " in " + this.getTaskID());
       return 0;
     }
     timeSpentInReduce = currentTime - sortFinishTime;
     
     reduceProcessingRate = bytesProcessed/timeSpentInReduce;   
     return reduceProcessingRate;
   }
   
  @Override
  public List<TaskAttemptID> getFetchFailedMaps() {
    return failedFetchTasks;
  }
  
  @Override
  void addFetchFailedMap(TaskAttemptID mapTaskId) {
    failedFetchTasks.add(mapTaskId);
  }
  
  @Override
  synchronized void statusUpdate(TaskStatus status) {
    super.statusUpdate(status);
    
    if (status.getShuffleFinishTime() != 0) {
      this.shuffleFinishTime = status.getShuffleFinishTime();
    }
    
    if (status.getSortFinishTime() != 0) {
      sortFinishTime = status.getSortFinishTime();
    }
    
    List<TaskAttemptID> newFetchFailedMaps = status.getFetchFailedMaps();
    if (failedFetchTasks == null) {
      failedFetchTasks = newFetchFailedMaps;
    } else if (newFetchFailedMaps != null){
      failedFetchTasks.addAll(newFetchFailedMaps);
    }
  }

  @Override
  synchronized void clearStatus() {
    super.clearStatus();
    failedFetchTasks.clear();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    shuffleFinishTime = in.readLong(); 
    sortFinishTime = in.readLong();
    int noFailedFetchTasks = in.readInt();
    failedFetchTasks = new ArrayList<TaskAttemptID>(noFailedFetchTasks);
    for (int i=0; i < noFailedFetchTasks; ++i) {
      TaskAttemptID id = new TaskAttemptID();
      id.readFields(in);
      failedFetchTasks.add(id);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(shuffleFinishTime);
    out.writeLong(sortFinishTime);
    out.writeInt(failedFetchTasks.size());
    for (TaskAttemptID taskId : failedFetchTasks) {
      taskId.write(out);
    }
  }
  
}
