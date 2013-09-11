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

  public void setLogSlowWriteProfileDataThreshold(long threshold) {
    options[LOG_SLOW_WRITE_PROFILEDATA_OFFSET] = threshold;
  }

  public long getLogSlowWriteProfileDataThreshold() {
    return options[LOG_SLOW_WRITE_PROFILEDATA_OFFSET];
  }

  public WriteOptions() {
    setSyncFileRange(SYNC_FILE_RANGE_DEFAULT);
  }

  public int getSyncFileRange() {
    return (int) ((options[SYNC_FILE_RANGE_OFFSET] & syncFileRangeMask) >>> SYNC_FILE_RANGE_SHIFT);
  }

  public void setSyncFileRange(int syncFileRange) {
    options[SYNC_FILE_RANGE_OFFSET] = (options[SYNC_FILE_RANGE_OFFSET] & (~syncFileRangeMask))
        | (syncFileRange << SYNC_FILE_RANGE_SHIFT);
  }

}
