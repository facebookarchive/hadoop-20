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

import java.io.IOException;

import org.apache.hadoop.hdfs.profiling.DFSWriteProfilingData.WritePacketClientProfile;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;

/****************************************************************
 * Packet for DFSOutputStream to use to construct the packet to send to data
 * node.
 ****************************************************************/
class DFSOutputStreamPacketFactory {
  static DFSOutputStreamPacket getHeartbeatPacket(
      DFSOutputStream dfsOutputStream, boolean includePktVersion,
      int packetVersion) throws IOException {
    if (packetVersion == DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST) {
      return new DFSOutputStreamPacketNonInlineChecksum(dfsOutputStream);
    } else if (!includePktVersion) {
      throw new IOException(
          "Older version doesn't support inline checksum packet format.");
    } else {
      return new DFSOutputStreamPacketInlineChecksum(dfsOutputStream);
    }
  }

  static DFSOutputStreamPacket getPacket(DFSOutputStream dfsOutputStream,
      boolean includePktVersion, int packetVersion, int pktSize,
      int chunksPerPkt, long offsetInBlock, WritePacketClientProfile profile)
      throws IOException {
    if (packetVersion == DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST) {
      return new DFSOutputStreamPacketNonInlineChecksum(dfsOutputStream,
          pktSize, chunksPerPkt, offsetInBlock, profile);
    } else if (!includePktVersion) {
      throw new IOException(
          "Older version doesn't support inline checksum packet format.");
    } else {
      return new DFSOutputStreamPacketInlineChecksum(dfsOutputStream, pktSize,
          chunksPerPkt, offsetInBlock, profile);
    }
  }
}
