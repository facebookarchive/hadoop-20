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
package org.apache.hadoop.hdfs.protocol;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.*;

public class TestClientProxyResponses {
  final Random rnd = new Random(1337L);
  TMemoryBuffer transport;
  TCompactProtocol protocol;

  @Before
  public void setUp() {
    transport = new TMemoryBuffer(1024 * 1024);
    protocol = new TCompactProtocol(transport);
  }

  @After
  public void tearDown() {
    transport.close();
  }

  @Test
  public void testBlockLocationsResponse() throws Exception {
    verifyStruct(new BlockLocationsResponse(rndLocatedBlocksArr(rnd, 1)[0]));
  }

  @Test
  public void testOpenResponse() throws Exception {
    verifyStruct(new OpenResponse(rndLocatedBlocksWithMetaInfo(rnd)));
  }

  @Test
  public void testAppendResponse() throws Exception {
    verifyStruct(new AppendResponse(new LocatedBlockWithOldGS(rndBlock(rnd), rndDatanodeInfoArr(rnd,
        13), rnd.nextInt(1000), rnd.nextInt(5), rnd.nextInt(), rnd.nextInt(), rnd.nextInt())));
  }

  @Test
  public void testAddBlockResponse() throws Exception {
    verifyStruct(new AddBlockResponse(rndLocatedBlockWithMetaInfo(rnd)));
  }

  @Test
  public void testPartialListingResponse() throws Exception {
    verifyStruct(new PartialListingResponse(new DirectoryListing(rndHdfsFileStatusArr(rnd, 100),
        rnd.nextInt(100))));
    verifyStruct(new PartialListingResponse(null));
  }

  @Test
  public void testLocatedPartialListingResponse() throws Exception {
    verifyStruct(new LocatedPartialListingResponse(new LocatedDirectoryListing(rndHdfsFileStatusArr(
        rnd, 100), rndLocatedBlocksArr(rnd, 100), rnd.nextInt(100))));
    verifyStruct(new LocatedPartialListingResponse(null));
  }

  @Test
  public void testCorruptFileBlocksResponse() throws Exception {
    verifyStruct(new CorruptFileBlocksResponse(new CorruptFileBlocks(new String[]{rndString(rnd),
        rndString(rnd), rndString(rnd)}, rndString(rnd))));
  }

  @Test
  public void testFileInfoResponse() throws Exception {
    verifyStruct(new FileInfoResponse(rndHdfsFileStatusArr(rnd, 1)[0]));
  }

  @Test
  public void testContentSummaryResponse() throws Exception {
    verifyStruct(new ContentSummaryResponse(new ContentSummary(rnd.nextLong(), rnd.nextInt(),
        rnd.nextInt(), rnd.nextInt(), rnd.nextInt(), rnd.nextInt())));
  }

  @Test
  public void testBlockInfoResponse() throws Exception {
    verifyStruct(new BlockInfoResponse(new LocatedBlockWithFileName(rndBlock(rnd),
        rndDatanodeInfoArr(rnd, 13), rndString(rnd))));
  }

  @Test
  public void testStatsResponse() throws Exception {
    verifyStruct(new StatsResponse(rnd.nextInt(), rnd.nextInt(), rnd.nextInt(), rnd.nextInt(),
        rnd.nextInt(), rnd.nextInt(), rnd.nextInt()));
  }

  @Test
  public void testHardLinkedFilesResponse() throws Exception {
    verifyStruct(new HardLinkedFilesResponse(Arrays.asList(rndString(rnd), rndString(rnd),
        rndString(rnd))));
  }

  @Test
  public void testIterativeGetOpenFilesResponse() throws Exception {
    verifyStruct(new IterativeGetOpenFilesResponse(Arrays.asList(new OpenFileInfo(rndString(rnd),
        rnd.nextInt()), new OpenFileInfo(rndString(rnd), rnd.nextInt()), new OpenFileInfo(rndString(
        rnd), rnd.nextInt()))));
  }

  @Test
  public void testPingResponse() throws Exception {
    verifyStruct(new PingResponse(rnd.nextInt()));
  }

  /**
   * DatanodeID and DatanodeInfo were very problematic due to not well defined behaviour of
   * swift-codec, the next two tests isolate regression
   */
  @Test
  public void testDatanodeID() throws Exception {
    verifyStruct(rndDatanodeID(rnd));
  }

  @Test
  public void testDatanodeInfo() throws Exception {
    verifyStruct(rndDatanodeInfoArr(rnd, 1)[0]);
  }

  /** Another complicated inheritance hierarchy is rooted at Block */
  @Test
  public void testLocatedBlock() throws Exception {
    verifyStruct(rndLocatedBlockArr(rnd, 1)[0]);
  }

  @Test
  public void testVersionedLocatedBlock() throws Exception {
    verifyStruct(new VersionedLocatedBlock(new Block(rnd.nextInt(), rnd.nextInt(), rnd.nextInt()),
        rndDatanodeInfoArr(rnd, 2), rnd.nextInt(), rnd.nextInt()));
  }

  @Test
  public void testLocatedBlockWithMetaInfo() throws Exception {
    verifyStruct(new LocatedBlockWithMetaInfo(new Block(rnd.nextInt(), rnd.nextInt(),
        rnd.nextInt()), rndDatanodeInfoArr(rnd, 2), rnd.nextInt(), rnd.nextInt(), rnd.nextInt(),
        rnd.nextInt()));
  }

  @Test
  public void testLocatedBlockWithOldGS() throws Exception {
    verifyStruct(new LocatedBlockWithOldGS(new Block(rnd.nextInt(), rnd.nextInt(), rnd.nextInt()),
        rndDatanodeInfoArr(rnd, 2), rnd.nextInt(), rnd.nextInt(), rnd.nextInt(), rnd.nextInt(),
        rnd.nextInt()));
  }

  /** Helper verifier */
  private <T extends Writable> void verifyStruct(T object) throws Exception {
    @SuppressWarnings("unchecked") Class<T> clazz = (Class<T>) object.getClass();
    ThriftCodec<T> codec = new ThriftCodecManager().getCodec(clazz);
    codec.write(object, protocol);
    T thriftCopy = codec.read(protocol);
    assertEqualsVerbose(object, thriftCopy);
    T writableCopy = WritableUtils.clone(object, new Configuration());
    assertEqualsVerbose(object, writableCopy);
  }
}
