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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class TestIPv6AddressTruncationMapping {
  private DNSToSwitchMapping theMap;
  private static final String errMsg = "Truncated IPv6 address is not correct.";
  private static final Random r = new Random();

  @Before
  public void initialize() {
    theMap = new IPv6AddressTruncationMapping();
  }

  @Test(timeout = 1000)
  public void testIPv6Truncation() {
    List<String> names = Arrays.asList("2401:db00:20:7014:face:0:15:0", "fe80::202:c9ff:fec7:1adc",
        "::1");
    List<String> rackNames = theMap.resolve(names);
    assertNotNull(errMsg, rackNames);
    assertEquals(errMsg, 3, rackNames.size());
    assertEquals(errMsg, "/2401:db00:0020:7014::", rackNames.get(0));
    assertEquals(errMsg, "/fe80:0000:0000:0000::", rackNames.get(1));
    assertEquals(errMsg, "/0000:0000:0000:0000::", rackNames.get(2));
  }

  @Test(timeout = 40000)
  public void testInvalidIpv6Addresses() {
    List<String> invalidIpAddresses = generateRandomHostnames(2);
    List<String> resolvedRacks = theMap.resolve(invalidIpAddresses);
    assertArrayEquals(errMsg, new String[] { NetworkTopology.DEFAULT_RACK,
        NetworkTopology.DEFAULT_RACK }, resolvedRacks.toArray());
  }

  private static List<String> generateRandomHostnames(int num) {
    List<String> hostnames = new LinkedList<String>();
    while (num-- > 0) {
      int charsToWrite = r.nextInt(10);
      String hostname = "";
      while (charsToWrite-- > 0) {
        hostname = hostname + IPv6AddressTruncationMapping.getTwoHexDigits((byte) r.nextInt());
      }
      hostname = hostname + ".invalid";
      hostnames.add(hostname);
    }
    return hostnames;
  }

  @Test(timeout = 1000)
  public void testIpv4Addresses() {
    List<String> input = Arrays.asList("127.0.0.1", "10.5.148.41");
    List<String> output = theMap.resolve(input);
    assertArrayEquals(new String[] { "/127.0.0", "/10.5.148" }, output.toArray());
  }
}
