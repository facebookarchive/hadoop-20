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

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.fs.CreateOptions;
import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * This class manages a bitset which is used to pass common options for
 * Read/Write block headers. This bitset should be used whenever we need to add
 * an extra field in the Read/Write block headers.
 */
public class DataTransferHeaderOptions extends CreateOptions{
  // a bitmask of various options.
  // First long
  // 1. the value of first three bits denote the type of FADVISE(0-5). See
  // NativeIO.java to know what each individual value corresponds to.
  // 2. Bit 4 denotes overwrite, if true and a file exists, will overwrite that file.
  // 3. Bit 5 denotes profiling enabled.
  // 4. Bits 6-7 denote the class of service for ioprio_set
  // 5. Bits 8-10 denote the priority for ioprio_set
  // 6. Bits 11-13 denote bits for sync_file_range
  // 7. Bit 14 denotes Force Sync.

  // Second long
  // The threshold in milliseconds for logging profile data for slow write
  // packets.
  protected long[] options = new long[2];
  private static final long fadviseMask = 0x07;
  private static final int FADVISE_OFFSET = 0;
  private static final int IOPRIO_OFFSET = 0;
  private static final int IOPRIO_CLASS_SHIFT = 5;
  private static final int IOPRIO_DATA_SHIFT = 7;
  private static final long ioprioClassMask = (0x03 << IOPRIO_CLASS_SHIFT);
  private static final long ioprioDataMask = (0x07 << IOPRIO_DATA_SHIFT);
  private static final long enableProfileMask = 0x10;
  private static final int ENABLE_PROFILE_OFFSET = 0;

  public void setIfProfileEnabled(boolean profileEnabled) {
    if (profileEnabled) {
      options[ENABLE_PROFILE_OFFSET] |= enableProfileMask;
    } else {
      options[ENABLE_PROFILE_OFFSET] &= ~enableProfileMask;
    }
  }

  public boolean ifProfileEnabled() {
    return (options[ENABLE_PROFILE_OFFSET] & enableProfileMask) != 0;
  }

  public boolean isIoprioDisabled() {
    return (getIoprioClass() == 0 && getIoprioData() == 0);
  }

  public int getIoprioClass() {
    return (int) ((options[IOPRIO_OFFSET] & ioprioClassMask) >>> IOPRIO_CLASS_SHIFT);
  }

  public int getIoprioData() {
    return (int) ((options[IOPRIO_OFFSET] & ioprioDataMask) >>> IOPRIO_DATA_SHIFT);
  }

  public void setIoprio(int classOfService, int data) {
    NativeIO.validateIoprioSet(classOfService, data);
    options[IOPRIO_OFFSET] = options[IOPRIO_OFFSET] & ~ioprioClassMask
        | (classOfService << IOPRIO_CLASS_SHIFT);
    options[IOPRIO_OFFSET] = options[IOPRIO_OFFSET] & ~ioprioDataMask
        | (data << IOPRIO_DATA_SHIFT);
  }

  public int getFadvise() {
    return (int) (options[FADVISE_OFFSET] & (fadviseMask));
  }

  public void setFadvise(int advise) {
    NativeIO.validatePosixFadvise(advise);
    options[FADVISE_OFFSET] = options[FADVISE_OFFSET] & ~fadviseMask | advise;
  }

  public void write(DataOutput out)
      throws IOException {
    int size = (options == null) ? 0 : options.length;
    out.writeInt(size);
    for (long option : options) {
      out.writeLong(option);
    }
  }

  public void readFields(DataInput in)
      throws IOException {
    int size = in.readInt();
    options = (size > options.length) ? new long[size] : options;
    for (int i = 0; i < size; i++) {
      options[i] = in.readLong();
    }
  }
  
  /**
   * Gets specific bits of a specific number.
   * @param value The number to get bits from
   * @param start Index of the rightmost bit we want. 0-based. 
   * @param len How many bits we want.
   * @return The result.
   */
  protected static long getBits(long value, int start, int len){
    return ( (value >>> start) & ((1L<<len)-1) );
  }
  
  /**
   * Sets specific bits of a specific number.
   * @param num The number to set bits.
   * @param start Index of the rightmost bit we want to set. 0-based. 
   * @param len How many bits we want to set.
   * @param value The value we want to set to.
   * @return The number after changing specific bits. 
   */
  protected static long setBits(long num, int start, int len, long value){
    // Get rid of illegal bits of value:
    value = value & ((1L<<len)-1);
    
    long val_mask = value << start;
    long zero_mask = ~( ((1L << len) -1) << start );
    return ( num & zero_mask ) | val_mask;
  }

  public DataTransferHeaderOptions getValue(){
    return this;
  }

}
