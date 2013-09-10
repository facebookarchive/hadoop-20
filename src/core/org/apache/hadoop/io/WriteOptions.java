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

import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * This class manages options which are passed while writing to files.
 */
public class WriteOptions extends DataTransferHeaderOptions {

  private static final int SYNC_FILE_RANGE_OFFSET = 0;
  private static final int SYNC_FILE_RANGE_SHIFT = 10;
  public static final int SYNC_FILE_RANGE_DEFAULT = NativeIO.SYNC_FILE_RANGE_WRITE;
  private static final int syncFileRangeMask = (0x7 << SYNC_FILE_RANGE_SHIFT);
  
  private static final int LOG_SLOW_WRITE_PROFILEDATA_OFFSET = 1;
  
  private static final int OVERWRITE_OFFSET = 0;
  private static final int OVERWRITE_BIT = 3;
  private static final boolean OVERWRITE_DEFAULT = true;
  
  private static final int FORCE_SYNC_OFFSET = 0;
  private static final int FORCE_SYNC_BIT = 13;
  private static final boolean FORCE_SYNC_DEFAULT = false;
  
  public void setLogSlowWriteProfileDataThreshold(long threshold) {
    options[LOG_SLOW_WRITE_PROFILEDATA_OFFSET] = threshold;
  }

  public long getLogSlowWriteProfileDataThreshold() {
    return options[LOG_SLOW_WRITE_PROFILEDATA_OFFSET];
  }

  public WriteOptions() {
    setSyncFileRange(SYNC_FILE_RANGE_DEFAULT);
    setOverwrite(OVERWRITE_DEFAULT);
    setForcesync(FORCE_SYNC_DEFAULT);
  }

  public int getSyncFileRange() {
    return (int) ((options[SYNC_FILE_RANGE_OFFSET] & syncFileRangeMask) >>> SYNC_FILE_RANGE_SHIFT);
  }

  public WriteOptions setSyncFileRange(int syncFileRange) {
    options[SYNC_FILE_RANGE_OFFSET] = (options[SYNC_FILE_RANGE_OFFSET] & (~syncFileRangeMask))
        | (syncFileRange << SYNC_FILE_RANGE_SHIFT);
    return this;
  }
  
  public WriteOptions setOverwrite(boolean overwrite){
    options[OVERWRITE_OFFSET] = setBits(options[OVERWRITE_OFFSET], OVERWRITE_BIT, 1, overwrite ? 1 : 0);
    return this;
  }

  public boolean getOverwrite(){
    return getBits(options[OVERWRITE_OFFSET], OVERWRITE_BIT, 1) != 0;
  }
  
  public WriteOptions setForcesync(boolean forceSync){
    options[FORCE_SYNC_OFFSET] = setBits(options[FORCE_SYNC_OFFSET], FORCE_SYNC_BIT, 1, forceSync ? 1 : 0);
    return this;
  }

  public boolean getForceSync(){
    return getBits(options[FORCE_SYNC_OFFSET], FORCE_SYNC_BIT, 1) != 0;
  }
  
  
}
