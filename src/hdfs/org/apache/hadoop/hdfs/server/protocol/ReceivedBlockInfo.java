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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A data structure to store Block and delHints together, used to
 * send received ACKs.
 */
public class ReceivedBlockInfo extends Block implements Writable{
  private String delHints;

  public final static String WILDCARD_HINT = "WILDCARD";
  public ReceivedBlockInfo(){
    super(0,0,0);
    delHints = null;
  }

  public ReceivedBlockInfo(Block blk, String delHints) {
    super(blk);
    this.setDelHints(delHints);
  }

  @Override
  public String getDelHints(){
    return this.delHints;
  }

  /**
   * Set this block's delHints
   * @param delHints
   */
  public void setDelHints(String delHints) {
    if (delHints == null || delHints.isEmpty()) {
      throw new IllegalArgumentException("DelHints is empty");
    }
    this.delHints = delHints;
  }
  
  public boolean equals(Object o){
    if (!((o instanceof ReceivedBlockInfo) || (o instanceof Block))) {
      return false;
    }
    //for lookups in maps/queues/etc when comparing RDBI to Block
    String otherDelHints = ((Block)o).getDelHints();
    return super.equals(o)
            && (this.delHints == otherDelHints ||
                (this.delHints != null && this.delHints.equals(otherDelHints)) ||
                WILDCARD_HINT.equals(this.delHints) ||
                WILDCARD_HINT.equals(this.delHints));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, this.delHints);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.setDelHints(Text.readString(in));
  }
}

