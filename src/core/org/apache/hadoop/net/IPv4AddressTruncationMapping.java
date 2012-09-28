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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/*
 * This class resolves the rack id from the IPv4 address by simply truncating
 * the fourth octet of the IPv4 address.
 */
public class IPv4AddressTruncationMapping extends CachedDNSToSwitchMapping {

  private static final Pattern IPV4_FORMAT =
    Pattern.compile("^([0-9-]+\\.[0-9-]+\\.[0-9-]+)\\.[0-9-]+$");
  public static final Log LOG =
    LogFactory.getLog(IPv4AddressTruncationMapping.class);
  
  // The header that will be added in front of the rack id.
  public static final String RACK_HEADER =
    (new Configuration()).get("topology.node.rackid.header", "/");

  public IPv4AddressTruncationMapping() {
    super(new IPv4AddressTruncationMappingInternal());
  }

  static private class IPv4AddressTruncationMappingInternal implements DNSToSwitchMapping {
    @Override
    public List<String> resolve(List<String> names) {
      List<String> result = new ArrayList<String>();
      for (String name : names) {
        String rackName = NetworkTopology.DEFAULT_RACK;
        try {
          Matcher nameMatcher = IPV4_FORMAT.matcher(name);
          // If name is already an IPv4 address, just use it.
          if (nameMatcher.find()) {
            rackName = RACK_HEADER + nameMatcher.group(1);
          } else {
            // If name is not already an IPv4 address, resolve it.
            String ip = InetAddress.getByName(name).getHostAddress();
            LOG.debug("name: " + name + " ip: " + ip);
            Matcher m = IPV4_FORMAT.matcher(ip);
            boolean mat = m.find();
            if (mat) {
              rackName = RACK_HEADER + m.group(1);
            } else {
              LOG.warn("Default rack is used for name: " + name + " ip: " + ip +
                       " because of pattern mismatch.");
            }
          }
        } catch (UnknownHostException e) {
          LOG.warn("Default rack is used for name: " + name, e);
        }
        LOG.debug("Mapping " + name + " to rack " + rackName);
        result.add(rackName);
      }
      return result;
    }
  }
}
