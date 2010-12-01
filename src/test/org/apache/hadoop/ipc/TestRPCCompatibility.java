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

package org.apache.hadoop.ipc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.net.NetUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit test for supporting across-version RPCs. */
public class TestRPCCompatibility {
  private static final String ADDRESS = "0.0.0.0";
  private static InetSocketAddress addr;
  private static Server server;
  private VersionedProtocol proxy;

  public static final Log LOG =
    LogFactory.getLog(TestRPCCompatibility.class);
  
  private static Configuration conf = new Configuration();

  public interface TestProtocol0 extends VersionedProtocol {
    public static final long versionID = 0L;
    void ping() throws IOException;    
  }
  
  public interface TestProtocol1 extends TestProtocol0 {
    public static final long versionID = 1L;
    
    String echo(String value) throws IOException;
  }

  public interface TestProtocol2 extends TestProtocol1 {
    public static final long versionID = 2L;
    int add(int v1, int v2);
  }
  
  public interface TestProtocol3 extends TestProtocol2 {
    public static final long versionID = 3L;
    int echo(int value)  throws IOException;
  }
  
  public static class TestImpl implements TestProtocol2 {
    int fastPingCounter = 0;
    
    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
    throws RPC.VersionIncompatible, IOException {
      // Although version 0 is compatible but it is too old 
      // so disallow this version of client
      if (clientVersion == TestProtocol0.versionID ) {
        throw new RPC.VersionIncompatible(
            this.getClass().getName(), clientVersion, versionID);
      }
      return TestProtocol2.versionID;
    }

    @Override
    public String echo(String value) { return value; }

    @Override
    public int add(int v1, int v2) { return v1 + v2; }

    @Override
    public void ping() { return; }
  }

  @BeforeClass
  public static void setup() throws IOException {
    // create a server with two handlers
    server = RPC.getServer(new TestImpl(), ADDRESS, 0, 2, false, conf);
    server.start();
    addr = NetUtils.getConnectAddress(server);
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    if (server != null) {
      server.stop();
    }
  }
  
  @After
  public void shutdownProxy() {
    if (proxy != null) {
      RPC.stopProxy(proxy);
    }
  }
  
  @Test
  public void testIncompatibleOldClient() throws Exception {
    try  {
      proxy = RPC.getProxy(
        TestProtocol1.class, TestProtocol0.versionID, addr, conf);
      fail("Should not be able to connect to the server");
    } catch (RemoteException re) {
      assertEquals(RPC.VersionIncompatible.class.getName(), re.getClassName());
    }
  }
  
  @Test
  public void testCompatibleOldClient() throws Exception {
    try {
      proxy = RPC.getProxy(
          TestProtocol1.class, TestProtocol1.versionID, addr, conf);
      fail("Expect to get a version mismatch exception");
    } catch(RPC.VersionMismatch e) {
      assertEquals(TestProtocol2.versionID, e.getServerVersion());
      proxy = e.getProxy();
    }

    TestProtocol1 proxy1 = (TestProtocol1)proxy;
    assertEquals("hello", proxy1.echo("hello")); // test equal
  }
  
  @Test
  public void testEqualVersionClient() throws Exception {
    proxy = RPC.getProxy(
        TestProtocol2.class, TestProtocol2.versionID, addr, conf);

    TestProtocol2 proxy2 = (TestProtocol2)proxy;
    assertEquals(3, proxy2.add(1, 2));
    assertEquals("hello", proxy2.echo("hello"));
    proxy2.ping();
  }

  private class Version3Client implements TestProtocol3 {

    private TestProtocol3 proxy3;
    private long serverVersion = versionID;
    
    private Version3Client() throws IOException {
      try {
        proxy =  RPC.getProxy(
          TestProtocol3.class, TestProtocol3.versionID, addr, conf);
      } catch (RPC.VersionMismatch e) {
        serverVersion = e.getServerVersion();
        if (serverVersion != TestProtocol2.versionID) {
          throw new RPC.VersionIncompatible(TestProtocol3.class.getName(),
              versionID, serverVersion);
        }
        proxy = e.getProxy();
      }
      proxy3 = (TestProtocol3) proxy;
    }
    
    @Override
    public int echo(int value) throws IOException, NumberFormatException {
      if (serverVersion == versionID) { // same version
        return proxy3.echo(value);  // use version 3 echo int
      } else { // server is version 2
        return Integer.parseInt(proxy3.echo(String.valueOf(value)));
      }
    }

    @Override
    public int add(int v1, int v2) {
      // TODO Auto-generated method stub
      return proxy3.add(v1, v2);
    }

    @Override
    public String echo(String value) throws IOException {
      return proxy3.echo(value);
    }

    @Override
    public void ping() throws IOException {
      proxy3.ping();
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws VersionIncompatible, IOException {
      return versionID;
    }
  }

  @Test
  public void testCompatibleNewClient() throws Exception {
    Version3Client client = new Version3Client();
    assertEquals(3, client.add(1, 2));
    assertEquals("hello", client.echo("hello"));
    assertEquals(3, client.echo(3));
    client.ping();
  }
}