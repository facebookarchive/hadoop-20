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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/** Tests handling of identity of original caller when calls are done via NameNode proxy */
public class TestOriginalCaller {
  static final String ADDRESS = "0.0.0.0";
  static final Configuration conf = new Configuration();

  static final UnixUserGroupInformation callerUgi = new UnixUserGroupInformation("calleru",
      new String[]{"callerg"});
  static final UnixUserGroupInformation originalUgi = new UnixUserGroupInformation("originalu",
      new String[]{"originalg"});

  interface TestProtocol extends VersionedProtocol {
    static final long versionID = 1L;

    void directCall() throws IOException;

    void proxyCall(UserGroupInformation origUGI) throws IOException;

    public void superuserCall() throws IOException;
  }

  class TestProtocolImpl implements TestProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws RPC
        .VersionIncompatible, IOException {
      return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
        int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion,
          clientMethodsHash);
    }

    @Override
    public void directCall() throws IOException {
      try {
        // Direct call's UGIs are the same
        assertEquals(callerUgi, Server.getCurrentUGI());
        assertEquals(callerUgi, FSNamesystem.getCurrentUGI());
        assertProxySubject();
      } catch (Throwable e) {
        throw new IOException(e);
      }
    }

    @Override
    public void proxyCall(final UserGroupInformation origUGI) throws IOException {
      try {
        // Before we know orignal caller, we operate in proxy context
        assertEquals(callerUgi, Server.getCurrentUGI());
        assertEquals(callerUgi, FSNamesystem.getCurrentUGI());
        // After deserializing original caller's UGI:
        Server.setOrignalCaller(origUGI);
        assertEquals(originalUgi, Server.getCurrentUGI());
        assertEquals(originalUgi, FSNamesystem.getCurrentUGI());
        assertProxySubject();
      } catch (Throwable e) {
        throw new IOException(e);
      }
    }

    @Override
    public void superuserCall() throws IOException {
      try {
        assertEquals(callerUgi, Server.getCurrentUGI());
        assertEquals(callerUgi, FSNamesystem.getCurrentUGI());
        // Proxy caller is not superuser, original caller is - until we determine original caller's
        // UGI we cannot get superuser privileges
        assertFSSuperuser(false, originalUgi);
        // After deserializing original caller's UGI:
        Server.setOrignalCaller(originalUgi);
        assertEquals(originalUgi, Server.getCurrentUGI());
        assertEquals(originalUgi, FSNamesystem.getCurrentUGI());
        // Original caller is set and we have superuser privileges
        assertFSSuperuser(true, originalUgi);
        FSPermissionChecker.checkSuperuserPrivilege(originalUgi, originalUgi.getGroupNames()[0]);
      } catch (Throwable e) {
        throw new IOException(e);
      }
    }
  }

  @BeforeClass
  public static void setUpClass() {
    assertFalse(callerUgi.equals(originalUgi));
    final UserGroupInformation user = UserGroupInformation.getCurrentUGI();
    if (user != null) {
      assertFalse(user.getUserName().equals(callerUgi.getUserName()));
      assertFalse(Arrays.asList(user.getGroupNames()).contains(callerUgi.getGroupNames()[0]));
      assertFalse(user.getUserName().equals(originalUgi.getUserName()));
      assertFalse(Arrays.asList(user.getGroupNames()).contains(originalUgi.getGroupNames()[0]));
    }
    UnixUserGroupInformation.saveToConf(conf, UnixUserGroupInformation.UGI_PROPERTY_NAME,
        callerUgi);
    conf.setBoolean("fs.security.ugi.getFromConf", false);
  }

  Server server;
  InetSocketAddress addr;
  TestProtocol proxy;

  @Before
  public void setUp() throws Exception {
    try {
      server = RPC.getServer(new TestProtocolImpl(), ADDRESS, 0, 2, false, conf);
      server.start();
      addr = NetUtils.getConnectAddress(server);
      proxy = RPC.getProxy(TestProtocol.class, TestProtocol.versionID, addr, conf);
    } catch (IOException e) {
      tearDown();
      throw e;
    }
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.stop();
    }
    if (proxy != null) {
      RPC.stopProxy(proxy);
    }
  }

  @Test
  public void testNoProxy0() throws Exception {
    // Direct call simulates normal call issued to NameNode directly
    proxy.directCall();
  }

  @Test
  public void testProxy0() throws Exception {
    // Call issued through proxy layer, caller's UGI != actual UGI
    proxy.proxyCall(originalUgi);
  }

  @Test
  public void testSuperuser() throws Exception {
    // Regression check, checkSuperuserPrivilege() in FSNamesystem should use FSPermissionChecker
    proxy.superuserCall();
  }

  static void assertProxySubject() throws Exception {
    // Since we do not issue doAs() with original caller's credentials, at all times during RPC
    // call the actual UGI of calling client should not be spoofed also if the call goes through
    // proxy.
    assertEquals(callerUgi, UserGroupInformation.getCurrentUGI());
    assertEquals(callerUgi, UserGroupInformation.getUGI(conf));
  }

  private static void assertFSSuperuser(boolean isSuper, UserGroupInformation superUGI)
      throws AccessControlException, NoSuchFieldException, IllegalAccessException {
    try {
      FSNamesystem testns = new FSNamesystem();
      setHiddenField(testns, "fsOwner", superUGI);
      setHiddenField(testns, "supergroup", superUGI.getGroupNames()[0]);
      setHiddenField(testns, "isPermissionEnabled", Boolean.TRUE);
      testns.checkSuperuserPrivilege();
      if (!isSuper) {
        fail();
      }
    } catch (AccessControlException e) {
      if (isSuper) {
        throw e;
      }
    }
  }

  private static void setHiddenField(Object object, String fieldName, Object value) throws
      IllegalAccessException {
    for (Field field : object.getClass().getDeclaredFields()) {
      if (fieldName.equals(field.getName())) {
        field.setAccessible(true);
        field.set(object, value);
      }
    }
  }
}
