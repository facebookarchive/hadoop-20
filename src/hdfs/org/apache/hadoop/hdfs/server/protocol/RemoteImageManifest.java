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
package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * An enumeration of images available on a remote location.
 */
public class RemoteImageManifest implements Writable {

  private List<RemoteImage> images;

  public RemoteImageManifest(List<RemoteImage> images) {
    // validate that the images are different
    Set<Long> txids = new HashSet<Long>();
    for (RemoteImage ri : images) {
      Preconditions.checkArgument(!txids.contains(ri.getTxId()));
      txids.add(ri.getTxId());
    }

    // store the list of images
    this.images = images;
  }

  public List<RemoteImage> getImages() {
    return images;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(images.size());
    for (RemoteImage log : images) {
      log.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numImages = in.readInt();
    images = Lists.newArrayList();
    for (int i = 0; i < numImages; i++) {
      RemoteImage log = new RemoteImage();
      log.readFields(in);
      images.add(log);
    }
  }
  
  @Override
  public String toString() {
    return "ImageManifest: [" + Joiner.on(",").join(images) + "]";
  }
}
