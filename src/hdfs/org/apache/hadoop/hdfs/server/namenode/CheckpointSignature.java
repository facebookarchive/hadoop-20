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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

/**
 * A unique signature intended to identify checkpoint transactions.
 */
public class CheckpointSignature extends StorageInfo 
                      implements WritableComparable<CheckpointSignature> {
  private static final String FIELD_SEPARATOR = ":";
  MD5Hash imageDigest = null;
  long mostRecentCheckpointTxId;
  long curSegmentTxId;

  CheckpointSignature() {}

  CheckpointSignature(FSImage fsImage) throws IOException {
    super(fsImage.storage);
    mostRecentCheckpointTxId = fsImage.storage.getMostRecentCheckpointTxId();
    imageDigest = fsImage.getImageDigest(mostRecentCheckpointTxId);
    curSegmentTxId = fsImage.getEditLog().getCurSegmentTxId();
  }

  CheckpointSignature(String str) {
    String[] fields = str.split(FIELD_SEPARATOR);
    assert fields.length == 6 : "Must be 7 fields in CheckpointSignature";
    layoutVersion = Integer.valueOf(fields[0]);
    namespaceID = Integer.valueOf(fields[1]);
    cTime = Long.valueOf(fields[2]);
    imageDigest = new MD5Hash(fields[3]);
    mostRecentCheckpointTxId = Long.valueOf(fields[4]);
    curSegmentTxId = Long.valueOf(fields[5]);
  }
  
  public long getMostRecentCheckpointTxId() {
    return mostRecentCheckpointTxId;
  }

  /**
   * Get the MD5 image digest
   * @return the MD5 image digest
   */
  MD5Hash getImageDigest() {
    return imageDigest;
  }
  
  public String toString() {
    return String.valueOf(layoutVersion) + FIELD_SEPARATOR
         + String.valueOf(namespaceID) + FIELD_SEPARATOR
         + String.valueOf(cTime) + FIELD_SEPARATOR
         + imageDigest.toString() + FIELD_SEPARATOR
         + String.valueOf(mostRecentCheckpointTxId) + FIELD_SEPARATOR 
         + String.valueOf(curSegmentTxId);
  }

  void validateStorageInfo(StorageInfo si) throws IOException {
    if(layoutVersion != si.layoutVersion
        || namespaceID != si.namespaceID || cTime != si.cTime) {
      // checkpointTime can change when the image is saved - do not compare
      throw new IOException("Inconsistent checkpoint fileds. "
          + "LV = " + layoutVersion + " namespaceID = " + namespaceID
          + " cTime = " + cTime + ". Expecting respectively: "
          + si.layoutVersion + "; " + si.namespaceID + "; " + si.cTime);
    }
  }

  //
  // Comparable interface
  //
  public int compareTo(CheckpointSignature o) {
    
    return ComparisonChain.start()  
        .compare(layoutVersion, o.layoutVersion) 
        .compare(namespaceID, o.namespaceID) 
        .compare(cTime, o.cTime) 
        .compare(curSegmentTxId, o.curSegmentTxId) 
        .compare(imageDigest, o.imageDigest)
        .result();
    
    // for now don't compare most recent checkpoint txid, 
    // as it would be different when finalizing a previously failed checkpoint
  }

  public boolean equals(Object o) {
    if (!(o instanceof CheckpointSignature)) {
      return false;
    }
    return compareTo((CheckpointSignature)o) == 0;
  }

  public int hashCode() {
    return layoutVersion ^ namespaceID ^
            (int)(cTime ^ mostRecentCheckpointTxId ^ curSegmentTxId) ^
            imageDigest.hashCode();
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeInt(getLayoutVersion());
    out.writeInt(getNamespaceID());
    out.writeLong(getCTime());
    out.writeLong(mostRecentCheckpointTxId);
    out.writeLong(curSegmentTxId);
    imageDigest.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    layoutVersion = in.readInt();
    namespaceID = in.readInt();
    cTime = in.readLong();
    mostRecentCheckpointTxId = in.readLong();
    curSegmentTxId = in.readLong();
    imageDigest = new MD5Hash();
    imageDigest.readFields(in);
  }
}
