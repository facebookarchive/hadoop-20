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

package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class AvatarFailoverSnapshot implements Writable {
  private final OpenFilesInfo openFilesInfo;
  private final List<FileStatusExtended> sampledFiles;

  public AvatarFailoverSnapshot(OpenFilesInfo openFilesInfo,
      List<FileStatusExtended> sampledFiles) {
    this.openFilesInfo = openFilesInfo;
    this.sampledFiles = (sampledFiles != null) ? sampledFiles
        : new ArrayList<FileStatusExtended>();
  }

  public AvatarFailoverSnapshot() {
    openFilesInfo = new OpenFilesInfo();
    sampledFiles = new ArrayList<FileStatusExtended>();
  }

  public OpenFilesInfo getOpenFilesInfo() {
    return openFilesInfo;
  }

  public List<FileStatusExtended> getSampledFiles() {
    return sampledFiles;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    openFilesInfo.write(out);
    out.writeInt(sampledFiles.size());
    for (FileStatusExtended stat : sampledFiles) {
      stat.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    openFilesInfo.readFields(in);
    int nFiles = in.readInt();
    for (int i = 0; i < nFiles; i++) {
      FileStatusExtended stat = new FileStatusExtended();
      stat.readFields(in);
      sampledFiles.add(stat);
    }
  }
}
