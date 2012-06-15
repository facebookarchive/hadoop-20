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

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

public class TestIPv4AddressTruncationMapping extends TestCase {
  DNSToSwitchMapping map = new IPv4AddressTruncationMapping();
  /*
   * Verify that the IP truncation based rack mapping works as expected.
   */
  public void testIPv4Truncation() {
    List<String> names = Arrays.asList("localhost",
                                       "localhost",
                                       "localhost");
    
    List<String> rackNames = map.resolve(names);
    
    String errMsg = "Truncated IPv4 address is not correct.";
    assertEquals(errMsg, IPv4AddressTruncationMapping.RACK_HEADER +
                 "127.0.0", rackNames.get(0));
    assertEquals(errMsg, IPv4AddressTruncationMapping.RACK_HEADER +
                 "127.0.0", rackNames.get(1));
    assertEquals(errMsg, IPv4AddressTruncationMapping.RACK_HEADER +
                 "127.0.0", rackNames.get(2));
    for (int i = 0; i < names.size(); i++) {
      System.out.println(names.get(i) + " is mapped to rack " +
                         rackNames.get(i));
    }
  }
}
