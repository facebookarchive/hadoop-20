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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.*;

import org.apache.commons.logging.*;
import org.apache.commons.logging.impl.Log4JLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.InjectionEventCore;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.log4j.Level;

import org.junit.Test;
import static org.junit.Assert.*;

/** Unit tests for RPC. */
public class TestRPC {
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog(TestRPC.class);
  {
    ((Log4JLogger)Client.CLIENT_TRACE_LOG).getLogger().setLevel(Level.ALL);
  }

  private static Configuration conf = new Configuration();

  int datasize = 1024*100;
  int numThreads = 50;
	
  public interface TestProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
    
    void ping() throws IOException;
    void slowPing(boolean shouldSlow) throws IOException;
    String echo(String value) throws IOException;
    String[] echo(String[] value) throws IOException;
    Writable echo(Writable value) throws IOException;
    int add(int v1, int v2) throws IOException;
    int add(int[] values) throws IOException;
    int error() throws IOException;
    void testServerGet() throws IOException;
    int[] exchange(int[] values) throws IOException;
    BooleanWritable test(BooleanWritable e) throws IOException;
  }

  public class TestImpl implements TestProtocol {
    int fastPingCounter = 0;
    
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TestProtocol.versionID;
    }
    
    public void ping() {}

    public synchronized void slowPing(boolean shouldSlow) {
      if (shouldSlow) {
        while (fastPingCounter < 2) {
          try {
          wait();  // slow response until two fast pings happened
          } catch (InterruptedException ignored) {}
        }
        fastPingCounter -= 2;
      } else {
        fastPingCounter++;
        notify();
      }
    }
    
    public String echo(String value) throws IOException { LOG.info("-----value: " + value); return value; }

    public String[] echo(String[] values) throws IOException { return values; }

    public Writable echo(Writable writable) {
      return writable;
    }
    public int add(int v1, int v2) {
      return v1 + v2;
    }

    public int add(int[] values) {
      int sum = 0;
      for (int i = 0; i < values.length; i++) {
        sum += values[i];
      }
      return sum;
    }

    public int error() throws IOException {
      throw new IOException("bobo");
    }

    public void testServerGet() throws IOException {
      if (!(Server.get() instanceof RPC.Server)) {
        throw new IOException("Server.get() failed");
      }
    }

    public int[] exchange(int[] values) {
      for (int i = 0; i < values.length; i++) {
        values[i] = i;
      }
      return values;
    }

    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }

    @Override
    public BooleanWritable test(BooleanWritable e) throws IOException {
      return e;
    }
  }

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    int datasize;
    TestProtocol proxy;

    Transactions(TestProtocol proxy, int datasize) {
      this.proxy = proxy;
      this.datasize = datasize;
    }

    // do two RPC that transfers data.
    public void run() {
      int[] indata = new int[datasize];
      int[] outdata = null;
      int val = 0;
      try {
        outdata = proxy.exchange(indata);
        val = proxy.add(1,2);
      } catch (IOException e) {
        assertTrue("Exception from RPC exchange() "  + e, false);
      }
      assertEquals(indata.length, outdata.length);
      assertEquals(val, 3);
      for (int i = 0; i < outdata.length; i++) {
        assertEquals(outdata[i], i);
      }
    }
  }

  //
  // A class that does an RPC but does not read its response.
  //
  static class SlowRPC implements Runnable {
    private TestProtocol proxy;
    private volatile boolean done;
   
    SlowRPC(TestProtocol proxy) {
      this.proxy = proxy;
      done = false;
    }

    boolean isDone() {
      return done;
    }

    public void run() {
      try {
        proxy.slowPing(true);   // this would hang until two fast pings happened
        done = true;
      } catch (IOException e) {
        assertTrue("SlowRPC ping exception " + e, false);
      }
    }
  }

  @Test
  public void testSlowRpc() throws Exception {
    System.out.println("Testing Slow RPC"); 
    // create a server with two handlers
    Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, 2, false, conf);
    TestProtocol proxy = null;
    
    try {
    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);

    // create a client
    proxy = (TestProtocol)RPC.getProxy(
        TestProtocol.class, TestProtocol.versionID, addr, conf);

    SlowRPC slowrpc = new SlowRPC(proxy);
    Thread thread = new Thread(slowrpc, "SlowRPC");
    thread.start(); // send a slow RPC, which won't return until two fast pings
    assertTrue("Slow RPC should not have finished1.", !slowrpc.isDone());

    proxy.slowPing(false); // first fast ping
    
    // verify that the first RPC is still stuck
    assertTrue("Slow RPC should not have finished2.", !slowrpc.isDone());

    proxy.slowPing(false); // second fast ping
    
    // Now the slow ping should be able to be executed
    while (!slowrpc.isDone()) {
      System.out.println("Waiting for slow RPC to get done.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      System.out.println("Down slow rpc testing");
    }
  }
  
  @Test
  public void testNRpcClients() throws Exception {
    testMultiRpc(100);
  }
  
  @Test
  public void test1RpcClient() throws Exception {
    testMultiRpc(1);
  }

  private void testMultiRpc(int clientCount) throws Exception {
    System.out.println("Testing RPC");
    // create a server with two handlers
    Server server = null;

    try {
      server = RPC.getServer(new TestImpl(), ADDRESS, 0, 3, false, conf);
      server.start();

      InetSocketAddress addr = NetUtils.getConnectAddress(server);

      // create a client
      List<Thread> clientThreads = new ArrayList<Thread>();
      List<RPCClient> clients = new ArrayList<RPCClient>();
      long start = System.currentTimeMillis();

      for (int i = 0; i < clientCount; i++) {
        RPCClient cli = new RPCClient(addr);
        Thread t = new Thread(cli);
        clients.add(cli);
        clientThreads.add(t);
        t.start();
      }
      for (Thread t : clientThreads) {
        t.join();
      }
      long stop = System.currentTimeMillis();
      System.out.println("----------- time taken : " + (stop-start));
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }
  
  class RPCClient implements Runnable {
    TestProtocol proxy = null;
    volatile boolean running = true;

    public RPCClient(InetSocketAddress addr) throws IOException {
      // create a client
      proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
          TestProtocol.versionID, addr, conf);
    }

    public void stop() {
      running = false;
    }

    @Override
    public void run() {
      try {
        BooleanWritable e = new BooleanWritable();
        for (int i = 0; i < 1000; i++) {
          proxy.ping();
          proxy.test(e);
        }
      } catch (IOException e) {
        System.out.println("Exception: " + e);
      } finally {
        if (proxy != null) {
          RPC.stopProxy(proxy);
        }
      }
    }
  }

  @Test
  public void testDnsUpdate() throws Exception {
    int oldMaxRetry = conf.getInt("ipc.client.connect.max.retries", 10);
    int oldmaxidletime = conf.getInt("ipc.client.connection.maxidletime", 10000);
    InetAddress oldAddr = null;
    InetSocketAddress addr = null;
    
    Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, conf);
    TestProtocol proxy = null;
    try {
      server.start();

      addr = NetUtils.getConnectAddress(server);
      oldAddr = addr.getAddress();

      conf.setInt("ipc.client.connect.max.retries", 0);
      conf.setInt("ipc.client.connection.maxidletime", 500);
      proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
          TestProtocol.versionID, addr, conf);

      proxy.ping();
      
      // Since we change the addr object. We have to wait for the
      // connection cache to expire before we issue issue RPC.
      // In real case it is not necessary since addr won't change.
      Thread.sleep(1000);
      
      Field addressField = addr.getClass().getDeclaredField("addr");
      addressField.setAccessible(true);
      addressField.set(addr, InetAddress.getByName("facebook.com"));
      Field hostField = addr.getClass().getDeclaredField("hostname");
      hostField.setAccessible(true);
      hostField.set(addr, "localhost");

      try {
        proxy.echo("foo");
        TestCase.fail();
      } catch (IOException e) {
        LOG.info("Caught " + e);
      }

      String stringResult = proxy.echo("foo");
      TestCase.assertEquals("foo", stringResult);

      try {
        proxy.error();
        TestCase.fail();
      } catch (IOException e) {
        LOG.debug("Caught " + e);
      }
    } finally {
      server.stop();
      if (proxy != null)
        RPC.stopProxy(proxy);

      conf.setInt("ipc.client.connect.max.retries", oldMaxRetry);
      conf.setInt("ipc.client.connection.maxidletime", oldmaxidletime);
    }
  }

  @Test
  public void testCalls() throws Exception {
    testCallsInternal(true);
    testCallsInternal(false);
  }
  
  private void testCallsInternal(boolean supportOldJobConf) throws Exception {
    Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, 1, false, conf,
        supportOldJobConf);
    TestProtocol proxy = null;
    try {
    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    proxy = (TestProtocol)RPC.getProxy(
        TestProtocol.class, TestProtocol.versionID, addr, conf);
      
    proxy.ping();

    String stringResult = proxy.echo("foo");
    assertEquals(stringResult, "foo");

    stringResult = proxy.echo((String)null);
    assertEquals(stringResult, null);

    String[] stringResults = proxy.echo(new String[]{"foo","bar"});
    assertTrue(Arrays.equals(stringResults, new String[]{"foo","bar"}));

    stringResults = proxy.echo((String[])null);
    assertTrue(Arrays.equals(stringResults, null));

    UTF8 utf8Result = (UTF8)proxy.echo(new UTF8("hello world"));
    assertEquals(utf8Result, new UTF8("hello world"));

    utf8Result = (UTF8)proxy.echo((UTF8)null);
    assertEquals(utf8Result, null);

    int intResult = proxy.add(1, 2);
    assertEquals(intResult, 3);

    intResult = proxy.add(new int[] {1, 2});
    assertEquals(intResult, 3);

    boolean caught = false;
    try {
      proxy.error();
    } catch (IOException e) {
      LOG.debug("Caught " + e);
      caught = true;
    }
    assertTrue(caught);

    proxy.testServerGet();

    // create multiple threads and make them do large data transfers
    System.out.println("Starting multi-threaded RPC test...");
    server.setSocketSendBufSize(1024);
    Thread threadId[] = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      Transactions trans = new Transactions(proxy, datasize);
      threadId[i] = new Thread(trans, "TransactionThread-" + i);
      threadId[i].start();
    }

    // wait for all transactions to get over
    System.out.println("Waiting for all threads to finish RPCs...");
    for (int i = 0; i < numThreads; i++) {
      try {
        threadId[i].join();
      } catch (InterruptedException e) {
        i--;      // retry
      }
    }

    // try some multi-calls
    Method echo =
      TestProtocol.class.getMethod("echo", new Class[] { String.class });
    String[] strings = (String[])RPC.call(echo, new String[][]{{"a"},{"b"}},
                                          new InetSocketAddress[] {addr, addr}, conf);
    assertTrue(Arrays.equals(strings, new String[]{"a","b"}));

    Method ping = TestProtocol.class.getMethod("ping", new Class[] {});
    Object[] voids = (Object[])RPC.call(ping, new Object[][]{{},{}},
                                        new InetSocketAddress[] {addr, addr}, conf);
    assertEquals(voids, null);
    } finally {
      server.stop();
      if(proxy!=null) RPC.stopProxy(proxy);
    }
  }
 
  @Test
  public void testIOSetupFailure() throws Exception {
    SecurityUtil.setPolicy(new ConfiguredPolicy(conf, new TestPolicyProvider()));
    
    Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, 5, true, conf);

    TestProtocol proxy = null;

    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    
    final AtomicReference<Hashtable> hashtable = new AtomicReference<Hashtable>(
        null);
    try {
      InjectionHandler.set(new InjectionHandler() {
        @Override
        protected void _processEvent(InjectionEventI event, Object... args) {
          if (event == InjectionEventCore.RPC_CLIENT_SETUP_IO_STREAM_FAILURE) {
            hashtable.set((Hashtable) args[0]);
            throw new RuntimeException("testIOSetupFailure");
          }
        }
      });
      
      try {
        proxy = (TestProtocol)RPC.getProxy(
            TestProtocol.class, TestProtocol.versionID, addr, conf);
        proxy.ping();
        TestCase.fail();
      } catch (RuntimeException e) {
        if (!e.getMessage().equals("testIOSetupFailure")) {
          throw e;
        }
      }
      InjectionHandler.clear();
      TestCase
          .assertNotNull("inject handler is not triggered", hashtable.get());
      TestCase.assertTrue("Connection is not cleared.", hashtable.get()
          .isEmpty());
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }
  
  @Test
  public void testStandaloneClient() throws IOException {
    try {
      RPC.waitForProxy(TestProtocol.class,
        TestProtocol.versionID, new InetSocketAddress(ADDRESS, 20), conf, 15000L);
      fail("We should not have reached here");
    } catch (ConnectException ioe) {
      //this is what we expected
    }
  }
  
  private static final String ACL_CONFIG = "test.protocol.acl";
  
  private static class TestPolicyProvider extends PolicyProvider {

    @Override
    public Service[] getServices() {
      return new Service[] { new Service(ACL_CONFIG, TestProtocol.class) };
    }
    
  }
  
  private void doRPCs(Configuration conf, boolean expectFailure) throws Exception {
    SecurityUtil.setPolicy(new ConfiguredPolicy(conf, new TestPolicyProvider()));
    
    Server server = RPC.getServer(new TestImpl(), ADDRESS, 0, 5, true, conf);

    TestProtocol proxy = null;

    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    
    try {
      proxy = (TestProtocol)RPC.getProxy(
          TestProtocol.class, TestProtocol.versionID, addr, conf);
      proxy.ping();

      if (expectFailure) {
        fail("Expect RPC.getProxy to fail with AuthorizationException!");
      }
    } catch (RemoteException e) {
      if (expectFailure) {
        assertTrue(e.unwrapRemoteException() instanceof AuthorizationException);
      } else {
        throw e;
      }
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }
  
  @Test
  public void testAuthorization() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(
        ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, true);
    
    // Expect to succeed
    conf.set(ACL_CONFIG, "*");
    doRPCs(conf, false);
    
    // Reset authorization to expect failure
    conf.set(ACL_CONFIG, "invalid invalid");
    doRPCs(conf, true);
  }
  
  @Test
  public void testRPCInterrupted() throws IOException, InterruptedException {
    final MiniDFSCluster cluster;
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 3, true, null);
    final AtomicBoolean passed = new AtomicBoolean(false);
    try {
      cluster.waitActive();
      Thread rpcThread = new Thread(new Runnable() {
        @Override
        public void run() {
          FileSystem fs = null;
          try {
          fs = cluster.getUniqueFileSystem();
            int i = 0;
            while (true) {
              fs.create(new Path(String.format("/file-%09d", i++)));
              fs.listStatus(new Path("/"));
            }
          } catch (IOException e) {
            if (e instanceof InterruptedIOException
                || e.getCause() instanceof InterruptedException) {
              // the underlying InterruptedException should be wrapped to an
              // IOException and end up here
              passed.set(true);
            } else {
              passed.set(false);
              LOG.error(e);
              fail(e.getMessage());
            }
          } finally {
            if (fs != null) {
              try {
                fs.close();
              } catch (IOException e) {
                passed.set(false);
                LOG.error(e);
                fail(e.toString());
              }
            }
          }
        }
      });
      FileSystem fs2 = cluster.getUniqueFileSystem();

      rpcThread.start();
      Thread.sleep(1000);
      rpcThread.interrupt();
      rpcThread.join();
      FileStatus[] statuses = fs2.listStatus(new Path("/"));
      assertTrue("expect at least 1 file created", statuses.length > 0);
      assertTrue("error in writing thread, see above", passed.get());
    } finally {
      cluster.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    new TestRPC().testCalls();
  }
}
