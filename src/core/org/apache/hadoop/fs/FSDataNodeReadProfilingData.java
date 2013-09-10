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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class FSDataNodeReadProfilingData implements Writable {
  
  public long readVersionAndOpCodeTime = 0L;
  public long readBlockHeaderTime = 0L;
  
  private long startTraceTime = 0L;
  
  public void startProfiling() {
    startTraceTime = System.nanoTime();
  }
  
  private long constructBlockSenderTime = 0L;
  public long getConstructBlockSenderTime() {
    return constructBlockSenderTime;
  }
  
  private long startSendBlockTime = 0L;
  
  public void startSendBlock() {
    startSendBlockTime = System.nanoTime();
    
    constructBlockSenderTime += startSendBlockTime - startTraceTime;
    
    startTraceTime = startSendBlockTime;
  }
  
  public void endSendBlock() {
    long curTime = System.nanoTime();
    sendBlockTime += curTime - startSendBlockTime;
    startSendBlockTime = curTime;
  }
  
  private long sendBlockTime = 0L;
  
  public long getSendBlockTime() {
    return sendBlockTime;
  }
  
  private long readChunkDataTime = 0L;
  public void recordReadChunkDataTime() {
    long curTime = System.nanoTime();
    readChunkDataTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getReadChunkDataTime() {
    return readChunkDataTime;
  }
  
  private long readChunkCheckSumTime = 0L;
  public void recordReadChunkCheckSumTime() {
    long curTime = System.nanoTime();
    readChunkCheckSumTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getReadChunkCheckSumTime() {
    return readChunkCheckSumTime;
  }
  
  private long updateChunkCheckSumTime = 0L;
  public void recordUpdateChunkCheckSumTime () {
    long curTime = System.nanoTime();
    updateChunkCheckSumTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getUpdateChunkCheckSumTime () {
    return updateChunkCheckSumTime;
  }
  
  private long verifyCheckSumTime = 0L;
  public void recordVerifyCheckSumTime () {
    long curTime = System.nanoTime();
    verifyCheckSumTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getVerifyCheckSumTime () {
    return verifyCheckSumTime;
  }
  
  private long sendChunkToClientTime = 0L;
  public void recordSendChunkToClientTime() {
    long curTime = System.nanoTime();
    sendChunkToClientTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getSendChunkToClientTime() {
    return sendChunkToClientTime;
  }
  
  private long transferChunkToClientTime = 0L;
  public void recordTransferChunkToClientTime() {
    long curTime = System.nanoTime();
    transferChunkToClientTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getTransferChunkToClientTime() {
    return transferChunkToClientTime;
  }
  
  // for inline checksum
  private long readChunkInlineTime = 0L;
  public void recordReadChunkInlineTime() {
    long curTime = System.nanoTime();
    readChunkInlineTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getReadChunkInlineTime() {
    return readChunkInlineTime;
  }
  
  private long copyChunkDataTime = 0L;
  public void recordCopyChunkDataTime() {
    long curTime = System.nanoTime();
    copyChunkDataTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getCopyChunkDataTime() {
    return copyChunkDataTime;
  }
  
  private long copyChunkChecksumTime = 0L;
  public void recordCopyChunkChecksumTime() {
    long curTime = System.nanoTime();
    copyChunkChecksumTime += curTime - startTraceTime;
    startTraceTime = curTime;
  }
  
  public long getCopyChunkChecksumTime() {
    return copyChunkChecksumTime;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("  Datanode_read_version_and_opcode_time: ").append(this.readVersionAndOpCodeTime)
      .append("\n  Datanode_read_block_header_time: ").append(this.readBlockHeaderTime)
      .append("\n  Datanode_construct_block_sender_time: ").append(this.constructBlockSenderTime)
      .append("\n  Datanode_send_block_total_time: ").append(this.sendBlockTime)
      .append("\n  Datanode_read_chunk_data: ").append(this.readChunkDataTime)
      .append("\n  Datanode_read_chunk_checksum: ").append(this.readChunkCheckSumTime)
      .append("\n  Datanode_update_chunk_checksum: ").append(this.updateChunkCheckSumTime)
      .append("\n  Datanode_verify_checksum: ").append(this.updateChunkCheckSumTime)
      .append("\n  Datanode_send_chunk_to_client: ").append(this.sendChunkToClientTime)
      .append("\n  Datanode_transfer_chunk_to_client: ").append(this.transferChunkToClientTime)
      .append("\n  Datanode_read_chunk_inline: ").append(this.readChunkInlineTime)
      .append("\n  Datanode_copy_chunk_data: ").append(this.copyChunkDataTime)
      .append("\n  Datanode_copy_chunk_checksum: ").append(this.copyChunkChecksumTime);
    
    return sb.toString();
  }
  
  private static final int NUM_COUNTERS = 13;
  
  private enum FieldCode {
    
    SEND_BLOCK ((byte) 1),
    READ_CHUNK_DATA ((byte) 2),
    READ_CHUNK_CHECKSUM ((byte) 3),
    UPDATE_CHUNK_CHECKSUM ((byte) 4),
    VERIFY_CHECKSUM ((byte) 5),
    SEND_CHUNK_TO_CLIENT ((byte) 6),
    TRANSFER_CHUNK_TO_CLIENT ((byte) 7),
    READ_CHUNK_INLINE ((byte) 8),
    COPY_CHUNK_DATA ((byte) 9),
    COPY_CHUNK_CHECKSUM ((byte) 10),
    READ_VERSION_AND_OPCODE ((byte) 11),
    READ_BLOCK_HEADER ((byte) 12),
    CONSTRUCT_BLOCK_SENDER ((byte) 13);
    
    private byte code;
    private FieldCode(byte code) {
      this.code = code;
    }
    
    public byte getCode() {
      return code;
    }
    
    private static final Map<Byte, FieldCode> byteToEnumMap =
        new HashMap<Byte, FieldCode> ();
    
    static {
      for (FieldCode code : values()) {
        byteToEnumMap.put(code.code, code);
      }
    }
    
    private static FieldCode fromByte(byte code) {
      return byteToEnumMap.get(code);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(NUM_COUNTERS);
    
    out.writeByte(FieldCode.SEND_BLOCK.getCode());
    out.writeLong(sendBlockTime);
    
    out.writeByte(FieldCode.READ_CHUNK_DATA.getCode());
    out.writeLong(readChunkDataTime);
    
    out.writeByte(FieldCode.READ_CHUNK_CHECKSUM.getCode());
    out.writeLong(readChunkCheckSumTime);
    
    out.writeByte(FieldCode.UPDATE_CHUNK_CHECKSUM.getCode());
    out.writeLong(updateChunkCheckSumTime);
    
    out.writeByte(FieldCode.VERIFY_CHECKSUM.getCode());
    out.writeLong(verifyCheckSumTime);
    
    out.writeByte(FieldCode.SEND_CHUNK_TO_CLIENT.getCode());
    out.writeLong(sendChunkToClientTime);
    
    out.writeByte(FieldCode.TRANSFER_CHUNK_TO_CLIENT.getCode());
    out.writeLong(transferChunkToClientTime);
    
    out.writeByte(FieldCode.READ_CHUNK_INLINE.getCode());
    out.writeLong(readChunkInlineTime);
    
    out.writeByte(FieldCode.COPY_CHUNK_DATA.getCode());
    out.writeLong(copyChunkDataTime);
    
    out.writeByte(FieldCode.COPY_CHUNK_CHECKSUM.getCode());
    out.writeLong(copyChunkChecksumTime);
    
    out.writeByte(FieldCode.READ_VERSION_AND_OPCODE.getCode());
    out.writeLong(readVersionAndOpCodeTime);
    
    out.writeByte(FieldCode.READ_BLOCK_HEADER.getCode());
    out.writeLong(readBlockHeaderTime);
    
    out.writeByte(FieldCode.CONSTRUCT_BLOCK_SENDER.getCode());
    out.writeLong(constructBlockSenderTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    
    int numOfCounts = in.readInt();

    for (int i = 0; i < numOfCounts; i++) {
      byte codeId = in.readByte();
      long value = in.readLong();
      FieldCode code = FieldCode.fromByte(codeId);
      switch (code) {
      case SEND_BLOCK:
        this.sendBlockTime = value;
        break;
      case READ_CHUNK_DATA:
        this.readChunkDataTime = value;
        break;
      case READ_CHUNK_CHECKSUM:
        this.readChunkCheckSumTime = value;
        break;
      case UPDATE_CHUNK_CHECKSUM:
        this.updateChunkCheckSumTime = value;
        break;
      case SEND_CHUNK_TO_CLIENT:
        this.sendChunkToClientTime = value;
        break;
      case TRANSFER_CHUNK_TO_CLIENT:
        this.transferChunkToClientTime = value;
        break;
      case READ_CHUNK_INLINE:
        this.readChunkInlineTime = value;
        break;
      case COPY_CHUNK_DATA:
        this.copyChunkDataTime = value;
        break;
      case COPY_CHUNK_CHECKSUM:
        this.copyChunkChecksumTime = value;
        break;
      case READ_VERSION_AND_OPCODE:
        this.readVersionAndOpCodeTime = value;
        break;
      case READ_BLOCK_HEADER:
        this.readBlockHeaderTime = value;
        break;
      case CONSTRUCT_BLOCK_SENDER:
        this.constructBlockSenderTime = value;
        break;
      default:
        break;
      }
    }
  }
}
