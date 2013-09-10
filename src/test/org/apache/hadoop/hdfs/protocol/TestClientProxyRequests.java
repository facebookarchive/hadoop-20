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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.assertEqualsVerbose;
import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.rndBlock;
import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.rndByteArr;
import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.rndDatanodeID;
import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.rndDatanodeInfoArr;
import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.rndFsPermission;
import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.rndLocatedBlockArr;
import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.rndPInt;
import static org.apache.hadoop.hdfs.protocol.RandomObjectsGenerators.rndString;

public class TestClientProxyRequests {
  final Random rnd = new Random(1337L);
  final UnixUserGroupInformation ugi = new UnixUserGroupInformation("testuser", Arrays.asList(
      "testgroup1", "testgroup2", "testgroup3"));
  final RequestMetaInfo metaInfo = new RequestMetaInfo(37, "resource1", 1337, 31337, ugi);
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
  public void testResourceMetaInfo() throws Exception {
    verifyStruct(new RequestMetaInfo(rndPInt(rnd), rndString(rnd), rndPInt(rnd), rndPInt(rnd),
        ugi));
    verifyStruct(new RequestMetaInfo(RequestMetaInfo.NO_CLUSTER_ID, rndString(rnd), rnd.nextInt(
        100), rndPInt(rnd), ugi));
    verifyStruct(new RequestMetaInfo(rndPInt(rnd), null, rndPInt(rnd), rndPInt(rnd), ugi));
    verifyStruct(new RequestMetaInfo(rndPInt(rnd), rndString(rnd), RequestMetaInfo.NO_NAMESPACE_ID,
        rndPInt(rnd), ugi));
    verifyStruct(new RequestMetaInfo(rndPInt(rnd), rndString(rnd), rndPInt(rnd),
        RequestMetaInfo.NO_APPLICATION_ID, ugi));
    verifyStruct(new RequestMetaInfo(rndPInt(rnd), rndString(rnd), rndPInt(rnd), rndPInt(rnd),
        null));

    verifyStruct(new RequestMetaInfo(rndPInt(rnd), null, RequestMetaInfo.NO_NAMESPACE_ID,
        rnd.nextInt(), ugi));
    verifyStruct(new RequestMetaInfo(RequestMetaInfo.NO_CLUSTER_ID, null, rnd.nextInt(),
        rnd.nextInt(), null));
    verifyStruct(new RequestMetaInfo(rndPInt(rnd), rndString(rnd), RequestMetaInfo.NO_NAMESPACE_ID,
        RequestMetaInfo.NO_APPLICATION_ID, null));
    verifyStruct(new RequestMetaInfo(RequestMetaInfo.NO_CLUSTER_ID, rndString(rnd), rnd.nextInt(),
        RequestMetaInfo.NO_APPLICATION_ID, null));

    verifyStruct(new RequestMetaInfo(RequestMetaInfo.NO_CLUSTER_ID, null,
        RequestMetaInfo.NO_NAMESPACE_ID, RequestMetaInfo.NO_APPLICATION_ID, null));
  }

  @Test
  public void testGetBlockLocationRequest() throws Exception {
    verifyStruct(new GetBlockLocationsRequest(metaInfo, rndString(rnd), rndPInt(rnd),
        rnd.nextLong()));
  }

  @Test
  public void testOpenRequest() throws Exception {
    verifyStruct(new OpenRequest(metaInfo, rndString(rnd), rndPInt(rnd), rnd.nextLong()));
  }

  @Test
  public void testCreateRequest() throws Exception {
    verifyStruct(new CreateRequest(metaInfo, rndString(rnd), rndString(rnd), rndFsPermission(rnd),
        rnd.nextBoolean(), rnd.nextBoolean(), (short) rnd.nextInt(5), rnd.nextLong()));
  }

  @Test
  public void testAppendRequest() throws Exception {
    verifyStruct(new AppendRequest(metaInfo, rndString(rnd), rndString(rnd)));
  }

  @Test
  public void testRecoverLease() throws Exception {
    verifyStruct(new RecoverLeaseRequest(metaInfo, rndString(rnd), rndString(rnd)));
  }

  @Test
  public void testCloseRecoverLease() throws Exception {
    verifyStruct(new CloseRecoverLeaseRequest(metaInfo, rndString(rnd), rndString(rnd),
        rnd.nextBoolean()));
  }

  @Test
  public void testSetReplicationRequest() throws Exception {
    verifyStruct(new SetReplicationRequest(metaInfo, rndString(rnd), (short) rnd.nextInt(5)));
  }

  @Test
  public void testSetPermissionRequest() throws Exception {
    verifyStruct(new SetPermissionRequest(metaInfo, rndString(rnd), new FsPermission(
        (short) rnd.nextInt())));
  }

  @Test
  public void testSetOwnerRequest() throws Exception {
    verifyStruct(new SetOwnerRequest(metaInfo, rndString(rnd), rndString(rnd),
        "group" + rnd.nextInt()));
  }

  @Test
  public void testAbandonBlockRequest() throws Exception {
    verifyStruct(new AbandonBlockRequest(metaInfo, rndString(rnd), rndString(rnd), rndBlock(rnd)));
  }

  @Test
  public void testAbandonFileRequest() throws Exception {
    verifyStruct(new AbandonFileRequest(metaInfo, rndString(rnd), rndString(rnd)));
  }

  @Test
  public void testAddBlockRequest() throws Exception {
    verifyStruct(new AddBlockRequest(metaInfo, rndString(rnd), rndString(rnd), Arrays.asList(
        rndDatanodeInfoArr(rnd, 10)), Arrays.asList(rndDatanodeInfoArr(rnd, 10)), rnd.nextInt(),
        rndBlock(rnd)));
  }

  @Test
  public void testCompleteRequest() throws Exception {
    verifyStruct(new CompleteRequest(metaInfo, rndString(rnd), rndString(rnd), rnd.nextInt(),
        rndBlock(rnd)));
  }

  @Test
  public void testReportBadBlockRequest() throws Exception {
    verifyStruct(new ReportBadBlocksRequest(metaInfo, Arrays.asList(rndLocatedBlockArr(rnd, 100))));
  }

  @Test
  public void testHardLinkRequest() throws Exception {
    verifyStruct(new HardLinkRequest(metaInfo, rndString(rnd), rndString(rnd)));
  }

  @Test
  public void testGetHardLinkedFilesRequest() throws Exception {
    verifyStruct(new GetHardLinkedFilesRequest(metaInfo, rndString(rnd)));
  }

  @Test
  public void testRenameRequest() throws Exception {
    verifyStruct(new RenameRequest(metaInfo, rndString(rnd), rndString(rnd)));
  }

  @Test
  public void testConcatRequest() throws Exception {
    verifyStruct(new ConcatRequest(metaInfo, "trg1", Arrays.asList("src1", "src2", "src3"),
        rnd.nextBoolean()));
  }

  @Test
  public void testDeleteRequest() throws Exception {
    verifyStruct(new DeleteRequest(metaInfo, rndString(rnd), rnd.nextBoolean()));
  }

  @Test
  public void testMkdirsRequest() throws Exception {
    verifyStruct(new MkdirsRequest(metaInfo, rndString(rnd), rndFsPermission(rnd)));
  }

  @Test
  public void testIterativeGetOpenFilesRequest() throws Exception {
    verifyStruct(new IterativeGetOpenFilesRequest(metaInfo, rndString(rnd), rnd.nextInt(),
        rndString(rnd)));
  }

  @Test
  public void testGetPartialFileStatusRequest() throws Exception {
    verifyStruct(new GetPartialListingRequest(metaInfo, rndString(rnd), rndByteArr(rnd, rnd.nextInt(
        37))));
  }

  @Test
  public void testGetLocatedPartialFileStatusRequest() throws Exception {
    verifyStruct(new GetLocatedPartialListingRequest(metaInfo, rndString(rnd), rndByteArr(rnd,
        rnd.nextInt(37))));
  }

  @Test
  public void testRenewLeaseRequest() throws Exception {
    verifyStruct(new RenewLeaseRequest(metaInfo, rndString(rnd)));
  }

  @Test
  public void testGetStatsRequest() throws Exception {
    verifyStruct(new GetStatsRequest(metaInfo));
  }

  @Test
  public void testGetPreferredBlockSizeRequest() throws Exception {
    verifyStruct(new GetPreferredBlockSizeRequest(metaInfo, rndString(rnd)));
  }

  @Test
  public void testListCorruptFileBlocksRequest() throws Exception {
    verifyStruct(new ListCorruptFileBlocksRequest(metaInfo, rndString(rnd), rndString(rnd)));
  }

  @Test
  public void testGetFileInfoRequest() throws Exception {
    verifyStruct(new GetFileInfoRequest(metaInfo, rndString(rnd)));
  }

  @Test
  public void testContentSummaryRequest() throws Exception {
    verifyStruct(new GetContentSummaryRequest(metaInfo, rndString(rnd)));
  }

  @Test
  public void testFSyncRequest() throws Exception {
    verifyStruct(new FSyncRequest(metaInfo, rndString(rnd), rndString(rnd)));
  }

  @Test
  public void testSetTimesRequest() throws Exception {
    verifyStruct(new SetTimesRequest(metaInfo, rndString(rnd), rnd.nextLong(), rnd.nextLong()));
  }

  @Test
  public void testUpdatePipelineRequest() throws Exception {
    verifyStruct(new UpdatePipelineRequest(metaInfo, rndString(rnd), rndBlock(rnd), rndBlock(rnd),
        Arrays.asList(rndDatanodeID(rnd), rndDatanodeID(rnd))));
  }

  @Test
  public void testGetDataTransferProtocolVersionRequest() throws Exception {
    verifyStruct(new GetDataTransferProtocolVersionRequest(metaInfo));
  }

  @Test
  public void testGetBlockInfoRequest() throws Exception {
    verifyStruct(new GetBlockInfoRequest(metaInfo, rnd.nextInt()));
  }

  @Test
  public void testRaidFileRequest() throws Exception {
    verifyStruct(new RaidFileRequest(metaInfo, rndString(rnd), rndString(rnd),
        (short) rnd.nextInt()));
  }

  @Test
  public void testPingRequest() throws Exception {
    verifyStruct(new PingRequest(metaInfo));
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
