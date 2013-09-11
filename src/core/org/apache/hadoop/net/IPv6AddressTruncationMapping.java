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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class that maps IPv6 addresses to Rack IDs.
 * Uses first 64 bits.
 * 
 */
public class IPv6AddressTruncationMapping extends CachedDNSToSwitchMapping {
  private static final Log LOG = LogFactory.getLog(IPv6AddressTruncationMapping.class);
  private static final IPv4AddressTruncationMapping ipv4Mapper = new IPv4AddressTruncationMapping();

  public IPv6AddressTruncationMapping() {
    super(new IPv6Truncator());
  }

  private static class IPv6Truncator implements DNSToSwitchMapping {
    @Override
    public List<String> resolve(List<String> namesToResolve) {
      List<String> resolvedNames = new ArrayList<String>(namesToResolve.size());
      for (int i = 0; i < namesToResolve.size(); i++) {
        String toResolve = namesToResolve.get(i);
        try {
          String rackId = getTruncatedAddress(toResolve);
          if (rackId == null) {
            rackId = NetworkTopology.DEFAULT_RACK;
          }
          resolvedNames.add(rackId);
          if (LOG.isInfoEnabled()) {
            LOG.info("Rack id for : " + toResolve + " is : " + resolvedNames.get(i));
          }
        } catch (UnknownHostException uhe) {
          LOG.warn("Host unresolvable: " + toResolve);
          resolvedNames.add(NetworkTopology.DEFAULT_RACK);
        }
      }
      return resolvedNames;
    }

    private String getTruncatedAddress(String string) throws UnknownHostException {
      InetAddress ipAddress = InetAddress.getByName(string);
      if (ipAddress instanceof Inet4Address) {
        if (LOG.isErrorEnabled()) {
          LOG.error(IPv6AddressTruncationMapping.class.getName()
              + " is being used when the stack is working on IPv4 addresses.\n"
              + "Check flags: -Djava.net.preferIPv4Stack  -Djava.net.preferIPv6Addresses");
        }
        return ipv4Mapper.resolve(Arrays.asList(string)).get(0);
      } else if (ipAddress instanceof Inet6Address) {
        String rackId = "";
        byte[] rawAddress = ipAddress.getAddress();
        // for IPv6 addresses, we are using /64 as a rack identifier.
        // First 64 bits = First 8 bytes = 16 hex digits = 4 hex chunks
        final int hexChunksToConcatenate = 4;
        for (int i = 0; i < hexChunksToConcatenate; i++) {
          rackId += getTwoHexDigits(rawAddress[i * 2]) + getTwoHexDigits(rawAddress[i * 2 + 1])
              + ":";
        }
        return IPv4AddressTruncationMapping.RACK_HEADER + rackId + ":";
      }
      return null;
    }
  }

  static String getTwoHexDigits(byte b) {
    return Integer.toHexString(((b >> 4) & 0xf)) + Integer.toHexString(b & 0xf);
  }
}
