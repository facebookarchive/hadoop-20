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
package org.apache.hadoop.hdfs.storageservice.server;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.swift.service.ThriftClientManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.protocol.ClientProxyProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.CreateRequest;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.GetPartialListingRequest;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.RequestMetaInfo;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.PartialListingResponse;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.TClientProxyProtocol;
import org.apache.hadoop.hdfs.storageservice.StorageServiceConfigKeys;
import org.apache.hadoop.hdfs.storageservice.server.ClientProxyService.ClientProxyCommons;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestClientProxyService {
  MiniAvatarCluster cluster;
  FileSystem fs;

  ClientProxyService proxy;

  ThriftClientManager clientManager;
  TClientProxyProtocol clientThrift;
  ClientProxyProtocol clientRPC;

  RequestMetaInfo metaInfo;

  @BeforeClass
  public static void setUpClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @Before
  public void setUp() throws Exception {
    try {
      Configuration conf = new Configuration();
      // Bind ports automatically
      conf.setInt(StorageServiceConfigKeys.PROXY_THRIFT_PORT_KEY, 0);
      conf.setInt(StorageServiceConfigKeys.PROXY_RPC_PORT_KEY, 0);

      cluster = new MiniAvatarCluster(conf, 2, true, null, null, 1, true);
      fs = cluster.getFileSystem(0);

      metaInfo = new RequestMetaInfo(conf.getInt(FSConstants.DFS_CLUSTER_ID,
          RequestMetaInfo.NO_CLUSTER_ID), cluster.getNameNode(0).getNameserviceId(),
          RequestMetaInfo.NO_NAMESPACE_ID, RequestMetaInfo.NO_APPLICATION_ID,
          (UnixUserGroupInformation) UserGroupInformation.getUGI(conf));

      proxy = new ClientProxyService(new ClientProxyCommons(conf, conf.get(
          FSConstants.DFS_CLUSTER_NAME)));
      conf.setInt(StorageServiceConfigKeys.PROXY_THRIFT_PORT_KEY, proxy.getThriftPort());
      conf.setInt(StorageServiceConfigKeys.PROXY_RPC_PORT_KEY, proxy.getRPCPort());

      clientManager = new ThriftClientManager();
      FramedClientConnector connector = new FramedClientConnector(
          StorageServiceConfigKeys.getProxyThriftAddress(conf));
      clientThrift = clientManager.createClient(connector, TClientProxyProtocol.class).get();

      clientRPC = RPC.getProxy(ClientProxyProtocol.class, ClientProxyProtocol.versionID,
          StorageServiceConfigKeys.getProxyRPCAddress(conf), conf);
    } catch (IOException e) {
      tearDown();
      throw e;
    }
  }

  @After
  public void tearDown() {
    RPC.stopProxy(clientRPC);
    IOUtils.cleanup(null, clientThrift, clientManager, proxy, fs);
    try {
      if (cluster != null) {
        cluster.shutDown();
      }
    } catch (Exception e) {
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @TestImplementation(name = "create")
  public void create0(Path path) throws RemoteException {
    clientThrift.create(new CreateRequest(metaInfo, path.toString(),
        metaInfo.getOrigCaller().getUserName(), FsPermission.getDefault(), true, true, (short) 1,
        1024));
  }

  @TestImplementation(name = "create")
  public void create1(Path path) throws IOException {
    clientRPC.create(new CreateRequest(metaInfo, path.toString(),
        metaInfo.getOrigCaller().getUserName(), FsPermission.getDefault(), true, true, (short) 1,
        1024));
  }

  @Test
  public void testCreate() throws Exception {
    for (Method method : getTestImplementations("create", 2)) {
      Path path = new Path("/newfile");
      assertFalse(fs.exists(path));
      method.invoke(this, path);
      assertTrue(fs.exists(path));
      fs.delete(path, true, true);
    }
  }

  @TestImplementation(name = "getPartialListing")
  public PartialListingResponse getPartialListing0(Path path) throws RemoteException {
    return clientThrift.getPartialListing(new GetPartialListingRequest(metaInfo, path.toString(),
        new byte[0]));
  }

  @TestImplementation(name = "getPartialListing")
  public PartialListingResponse getPartialListing1(Path path) throws IOException {
    return clientRPC.getPartialListing(new GetPartialListingRequest(metaInfo, path.toString(),
        new byte[0]));
  }

  @Test
  public void testGetPartialListing() throws Exception {
    for (Method method : getTestImplementations("getPartialListing", 2)) {
      Path path = new Path("/newdir/");
      fs.mkdirs(path);
      for (int i = 0; i < 3; i++) {
        fs.create(new Path(path, "newfile" + i));
        assertTrue(fs.exists(new Path(path, "newfile" + i)));
      }
      PartialListingResponse res = (PartialListingResponse) method.invoke(this, path);
      assertEquals(3, res.getListing().getFileStatusList().size());
      int i = 0;
      for (HdfsFileStatus file : res.getListing().getFileStatusList()) {
        assertEquals("newfile" + i++, file.getLocalName());
      }
      fs.delete(path, true, true);
    }
  }
  // TODO: more tests to come

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private static @interface TestImplementation {
    public String name();
  }

  private static Iterable<Method> getTestImplementations(String call, int count) {
    List<Method> subTests = new ArrayList<Method>();
    for (Method method : TestClientProxyService.class.getMethods()) {
      TestImplementation annotation = method.getAnnotation(TestImplementation.class);
      if (annotation != null && annotation.name().matches(call + "[0-9]*")) {
        subTests.add(method);
      }
    }
    assertFalse("TestImplementations for call: " + call + " not found", subTests.isEmpty());
    assertEquals("Wrong number of TestImplementations for call: " + call, count, subTests.size());
    return subTests;
  }
}
