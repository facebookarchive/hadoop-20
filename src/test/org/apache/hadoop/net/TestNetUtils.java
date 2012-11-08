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
package org.apache.hadoop.net;

import org.junit.Test;
import org.mortbay.log.Log;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;

public class TestNetUtils {

  /**
   * Test that we can't accidentally connect back to the connecting socket due
   * to a quirk in the TCP spec.
   *
   * This is a regression test for HADOOP-6722.
   */
  @Test
  public void testAvoidLoopbackTcpSockets() throws Exception {
    Configuration conf = new Configuration();

    Socket socket = NetUtils.getDefaultSocketFactory(conf)
      .createSocket();
    socket.bind(new InetSocketAddress("127.0.0.1", 0));
    System.err.println("local address: " + socket.getLocalAddress());
    System.err.println("local port: " + socket.getLocalPort());
    try {
      NetUtils.connect(socket,
        new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort()),
        20000);
      socket.close();
      fail("Should not have connected");
    } catch (ConnectException ce) {
      System.err.println("Got exception: " + ce);
      assertTrue(ce.getMessage().contains("resulted in a loopback"));
    } catch (SocketException se) {
      System.err.println("Got exception: " + se);
      assertTrue(se.getMessage().startsWith("Invalid argument"));
    }
  }
  
  /**
   * Test local host check.
   */
  @Test
  public void testLocalhostCheck() throws Exception {
    assertTrue(NetUtils.isLocalAddress(InetAddress.getLocalHost()));
    Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
    while (ifaces.hasMoreElements()) {
      NetworkInterface iface = ifaces.nextElement();
      Enumeration<InetAddress> addresses = iface.getInetAddresses();
      while (addresses.hasMoreElements()) {
        InetAddress addr = addresses.nextElement();
        assertTrue(NetUtils.isLocalAddress(addr));
      }
    }
    
    assertTrue(NetUtils.isLocalAddress(InetAddress.getByName("localhost")));
    assertTrue(NetUtils.isLocalAddress(InetAddress.getByName("127.0.0.1")));
    assertFalse(NetUtils.isLocalAddress(InetAddress.getByName("google.com")));    
  }
  
  /**
   * Test local host check.
   */
  @Test
  public void testLocalhostCheckWithCaching() throws Exception {
    assertTrue(NetUtils.isLocalAddressWithCaching(InetAddress.getLocalHost()));
    Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
    while (ifaces.hasMoreElements()) {
      NetworkInterface iface = ifaces.nextElement();
      Enumeration<InetAddress> addresses = iface.getInetAddresses();
      while (addresses.hasMoreElements()) {
        InetAddress addr = addresses.nextElement();
        assertTrue(NetUtils.isLocalAddressWithCaching(addr));
      }
    }
    
    assertTrue(NetUtils.knownLocalAddrs.containsKey(InetAddress.getLocalHost()));
    while (ifaces.hasMoreElements()) {
      NetworkInterface iface = ifaces.nextElement();
      Enumeration<InetAddress> addresses = iface.getInetAddresses();
      while (addresses.hasMoreElements()) {
        InetAddress addr = addresses.nextElement();
        assertTrue(NetUtils.knownLocalAddrs.containsKey(NetUtils.isLocalAddressWithCaching(addr)));
      }
    }
    
    assertTrue(NetUtils.isLocalAddressWithCaching(InetAddress.getByName("localhost")));
    assertTrue(NetUtils.isLocalAddressWithCaching(InetAddress.getByName("127.0.0.1")));
    assertFalse(NetUtils.isLocalAddressWithCaching(InetAddress.getByName("facebook.com")));    
 
  }
}
