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

package org.apache.hadoop.hdfs;

import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.UserGroupInformation;

public class DFSUtil {
  /**
   * Compartor for sorting DataNodeInfo[] based on decommissioned states.
   * Decommissioned nodes are moved to the end of the array on sorting with
   * this compartor.
   */
  public static final Comparator<DatanodeInfo> DECOM_COMPARATOR = 
    new Comparator<DatanodeInfo>() {
    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      return a.isDecommissioned() == b.isDecommissioned() ? 0 :
        a.isDecommissioned() ? 1 : -1;
    }
  };
  
  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2String(byte[][] pathComponents) {
    if (pathComponents.length == 0)
      return "";
    if (pathComponents.length == 1 && pathComponents[0].length == 0) {
      return Path.SEPARATOR;
    }
    try {
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < pathComponents.length; i++) {
        result.append(new String(pathComponents[i], "UTF-8"));
        if (i < pathComponents.length - 1) {
          result.append(Path.SEPARATOR_CHAR);
        }
      }
      return result.toString();
    } catch (UnsupportedEncodingException ex) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Splits the array of bytes into array of arrays of bytes on byte separator
   * 
   * @param bytes
   *          the array of bytes to split
   * @param separator
   *          the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes, byte separator) {
    return bytes2byteArray(bytes, bytes.length, separator);
  }

   /**
    * Converts a byte array to a string using UTF8 encoding.
    */
   public static String bytes2String(byte[] bytes) {
     try {
       return new String(bytes, "UTF8");
     } catch(UnsupportedEncodingException e) {
       assert false : "UTF8 encoding is not supported ";
     }
     return null;
   }

   /**
    * Converts a string to a byte array using UTF8 encoding.
    */
   public static byte[] string2Bytes(String str) {
     try {
       return str.getBytes("UTF8");
     } catch(UnsupportedEncodingException e) {
       assert false : "UTF8 encoding is not supported ";
     }
     return null;
   }

  /**
   * Splits first len bytes in bytes to array of arrays of bytes on byte
   * separator
   * 
   * @param bytes
   *          the byte array to split
   * @param len
   *          the number of bytes to split
   * @param separator
   *          the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes, int len, byte separator) {
    assert len <= bytes.length;
    int splits = 0;
    if (len == 0) {
      return new byte[][] { null };
    }
    // Count the splits. Omit multiple separators and the last one
    for (int i = 0; i < len; i++) {
      if (bytes[i] == separator) {
        splits++;
      }
    }
    int last = len - 1;
    while (last > -1 && bytes[last--] == separator) {
      splits--;
    }
    if (splits == 0 && bytes[0] == separator) {
      return new byte[][] { null };
    }
    splits++;
    byte[][] result = new byte[splits][];
    int startIndex = 0;
    int nextIndex = 0;
    int index = 0;
    // Build the splits
    while (index < splits) {
      while (nextIndex < len && bytes[nextIndex] != separator) {
        nextIndex++;
      }
      result[index] = new byte[nextIndex - startIndex];
      System.arraycopy(bytes, startIndex, result[index], 0, nextIndex
          - startIndex);
      index++;
      startIndex = nextIndex + 1;
      nextIndex = startIndex;
    }
    return result;
  }

  /**
   * Convert a LocatedBlocks to BlockLocations[]
   * @param blocks a LocatedBlocks
   * @return an array of BlockLocations
   */
  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.locatedBlockCount();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    if (nrBlocks == 0) {
      return blkLocations;
    }
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] names = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        names[hCnt] = locations[hCnt].getName();
        NodeBase node = new NodeBase(names[hCnt],
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] = new BlockLocation(names, hosts, racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize(),
                                            blk.isCorrupt());
      idx++;
    }
    return blkLocations;
  }

  /**
   * @return all corrupt files in dfs
   */
  public static String[] getCorruptFiles(DistributedFileSystem dfs)
    throws IOException {
    return getCorruptFiles(dfs, "/");
  }

  /**
   * @return all corrupt files in dfs under a path.
   */
  public static String[] getCorruptFiles(DistributedFileSystem dfs, String path)
      throws IOException {
    Set<String> corruptFiles = new HashSet<String>();
    RemoteIterator<Path> cfb = dfs.listCorruptFileBlocks(new Path(path));
    while (cfb.hasNext()) {
      corruptFiles.add(cfb.next().toUri().getPath());
    }

    return corruptFiles.toArray(new String[corruptFiles.size()]);
  }
  
  /**
   * Check if it is a deleted block or not
   */
  private final static long DELETED = Long.MAX_VALUE - 1;
  public static boolean isDeleted(Block block) {
    return block.getNumBytes() == DELETED;
  }
  
  public static void markAsDeleted(Block block) {
    block.setNumBytes(DELETED);
  }

  /**
   * Returns collection of nameservice Ids from the configuration.
   * @param conf configuration
   * @return collection of nameservice Ids
   */
  public static Collection<String> getNameServiceIds(Configuration conf) {
    return conf.getStringCollection(FSConstants.DFS_FEDERATION_NAMESERVICES);
  }

  /**
   * Given a list of keys in the order of preference, returns a value
   * for the key in the given order from the configuration.
   * @param defaultValue default value to return, when key was not found
   * @param keySuffix suffix to add to the key, if it is not null
   * @param conf Configuration
   * @param keys list of keys in the order of preference
   * @return value of the key or default if a key was not found in configuration
   */
  private static String getConfValue(String defaultValue, String keySuffix,
      Configuration conf, String... keys) {
    String value = null;
    for (String key : keys) {
      if (keySuffix != null) {
        key += "." + keySuffix;
      }
      value = conf.get(key);
      if (value != null) {
        break;
      }
    }
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }
  
  /**
   * Returns list of InetSocketAddress for a given set of keys.
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found
   * @param keys Set of keys to look for in the order of preference
   * @return list of InetSocketAddress corresponding to the key
   */
  public static List<InetSocketAddress> getAddresses(Configuration conf,
      String defaultAddress, String... keys) {
    return getAddresses(conf, getNameServiceIds(conf), defaultAddress, keys);
  }
  
  /**
   * Set the configuration based on the service id given in the argv
   * @param argv argument list
   * @param conf configuration
   * @return argument list without service name argument
   */
  public static String[] setGenericConf(String[] argv, Configuration conf) {
    String[] serviceId = new String[1];
    serviceId[0] = "";
    String[] filteredArgv = getServiceName(argv, serviceId);
    if (!serviceId[0].equals("")) {
      if (!NameNode.validateServiceName(conf, serviceId[0])) {
        throw new IllegalArgumentException("Service Id doesn't match the config");
      }
      setGenericConf(conf, serviceId[0], NameNode.NAMESERVICE_SPECIFIC_KEYS);
      NameNode.setupDefaultURI(conf);
    }
    return filteredArgv;
  }
  
  /**
   * Get the service name arguments and return the filtered argument list
   * @param argv argument list 
   * @param serviceId[0] is the service id if it's given in the argv, "" otherwise
   * @return argument list without service name argument 
   */
  public static String[] getServiceName(String[] argv, String[] serviceId) 
      throws IllegalArgumentException {
    ArrayList<String> newArgvList = new ArrayList<String>();
    for (int i = 0; i < argv.length; i++) {
      if ("-service".equals(argv[i])) {
        if (i+1 == argv.length ) {
          throw new IllegalArgumentException("Doesn't have service id");
        }
        serviceId[0] = argv[++i];
      } else {
        newArgvList.add(argv[i]);
      }
    }
    String[] newArgvs = new String[newArgvList.size()];
    newArgvList.toArray(newArgvs);
    return newArgvs;
  }
  
  /**
   * Return list of InetSocketAddress for a given set of services
   * 
   * @param conf configuration
   * @param serviceIds services ids
   * @param defaultAddress default address
   * @param keys set of keys
   * @return list of InetSocketAddress
   */
  public static List<InetSocketAddress> getAddresses(Configuration conf,
      Collection<String> serviceIds, String defaultAddress, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    List<InetSocketAddress> isas = new ArrayList<InetSocketAddress>();

    // Configuration with a single namenode
    if (nameserviceIds == null || nameserviceIds.isEmpty()) {
      String address = getConfValue(defaultAddress, null, conf, keys);
      if (address == null) {
        return null;
      }
      isas.add(NetUtils.createSocketAddr(address));
    } else {
      // Get the namenodes for all the configured nameServiceIds
      for (String nameserviceId : nameserviceIds) {
        String address = getConfValue(null, nameserviceId, conf, keys);
        if (address == null) {
          return null;
        }
        isas.add(NetUtils.createSocketAddr(address));
      }
    }
    return isas;
  }
  
  /**
   * Returns list of InetSocketAddresses corresponding to namenodes from the
   * configuration. Note this is to be used by clients to get the list of
   * namenode addresses to talk to.
   * 
   * Returns namenode address specifically configured for clients (using
   * service ports)
   * 
   * @param conf configuration
   * @return list of InetSocketAddress
   * @throws IOException on error
   */
  public static List<InetSocketAddress> getClientRpcAddresses(
      Configuration conf) throws IOException {
    // Use default address as fall back
    String defaultAddress;
    try {
      defaultAddress = NameNode.getHostPortString(NameNode.getAddress(conf));
    } catch (IllegalArgumentException e) {
      defaultAddress = null;
    }
    
    List<InetSocketAddress> addressList = getAddresses(conf, defaultAddress,
        FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList == null) {
      throw new IOException("Incorrect configuration: namenode address "
          + FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY
          + " is not configured.");
    }
    return addressList;
  }
  
  /**
   * Returns list of InetSocketAddresses corresponding to namenodes from the
   * configuration. Note this is to be used by datanodes to get the list of
   * namenode addresses to talk to.
   * 
   * Returns namenode address specifically configured for datanodes (using
   * service ports), if found. If not, regular RPC address configured for other
   * clients is returned.
   * 
   * @param conf configuration
   * @return list of InetSocketAddress
   * @throws IOException on error
   */
  public static List<InetSocketAddress> getNNServiceRpcAddresses(
      Configuration conf) throws IOException {
    // Use default address as fall back
    String defaultAddress;
    try {
      defaultAddress = NameNode.getHostPortString(NameNode.getAddress(conf));
    } catch (IllegalArgumentException e) {
      defaultAddress = null;
    }
    
    List<InetSocketAddress> addressList = getAddresses(conf, defaultAddress,
        NameNode.DATANODE_PROTOCOL_ADDRESS, FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList == null) {
      throw new IOException("Incorrect configuration: namenode address "
          + NameNode.DATANODE_PROTOCOL_ADDRESS + " or "  
          + FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY
          + " is not configured.");
    }
    return addressList;
  }
  
  /**
   * Given the InetSocketAddress for any configured communication with a 
   * namenode, this method returns the corresponding nameservice ID,
   * by doing a reverse lookup on the list of nameservices until it
   * finds a match.
   * If null is returned, client should try {@link #isDefaultNamenodeAddress}
   * to check pre-Federated configurations.
   * Since the process of resolving URIs to Addresses is slightly expensive,
   * this utility method should not be used in performance-critical routines.
   * 
   * @param conf - configuration
   * @param address - InetSocketAddress for configured communication with NN.
   *     Configured addresses are typically given as URIs, but we may have to
   *     compare against a URI typed in by a human, or the server name may be
   *     aliased, so we compare unambiguous InetSocketAddresses instead of just
   *     comparing URI substrings.
   * @param keys - list of configured communication parameters that should
   *     be checked for matches.  For example, to compare against RPC addresses,
   *     provide the list DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
   *     DFS_NAMENODE_RPC_ADDRESS_KEY.  Use the generic parameter keys,
   *     not the NameServiceId-suffixed keys.
   * @return nameserviceId, or null if no match found
   */
  public static String getNameServiceIdFromAddress(Configuration conf, 
      InetSocketAddress address, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);

    // Configuration with a single namenode and no nameserviceId
    if (nameserviceIds == null || nameserviceIds.isEmpty()) {
      // client should try {@link isDefaultNamenodeAddress} instead
      return null;
    }
    // Get the candidateAddresses for all the configured nameServiceIds
    for (String nameserviceId : nameserviceIds) {
      for (String key : keys) {
        String candidateAddress = conf.get(
            getNameServiceIdKey(key, nameserviceId));
        if (candidateAddress != null
            && address.equals(NetUtils.createSocketAddr(candidateAddress)))
          return nameserviceId;
      }
    }
    // didn't find a match
    // client should try {@link isDefaultNamenodeAddress} instead
    return null;
  }

  /**
   * return server http address from the configuration
   * @param conf
   * @param namenode - namenode address
   * @return server http
   */
  public static String getInfoServer(
      InetSocketAddress namenode, Configuration conf) {
    String httpAddressDefault = 
        NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                                  "dfs.info.port", "dfs.http.address"); 
    String httpAddress = null;
    if(namenode != null) {
      // if non-default namenode, try reverse look up 
      // the nameServiceID if it is available
      String nameServiceId = DFSUtil.getNameServiceIdFromAddress(
          conf, namenode,
          FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);

      if (nameServiceId != null) {
        httpAddress = conf.get(DFSUtil.getNameServiceIdKey(
            FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY, nameServiceId));
      }
    }
    // else - Use non-federation style configuration
    if (httpAddress == null) {
      httpAddress = conf.get("dfs.http.address", httpAddressDefault);
    }

    return httpAddress;
  }
  
  /**
   * Given the InetSocketAddress for any configured communication with a 
   * namenode, this method determines whether it is the configured
   * communication channel for the "default" namenode.
   * It does a reverse lookup on the list of default communication parameters
   * to see if the given address matches any of them.
   * Since the process of resolving URIs to Addresses is slightly expensive,
   * this utility method should not be used in performance-critical routines.
   * 
   * @param conf - configuration
   * @param address - InetSocketAddress for configured communication with NN.
   *     Configured addresses are typically given as URIs, but we may have to
   *     compare against a URI typed in by a human, or the server name may be
   *     aliased, so we compare unambiguous InetSocketAddresses instead of just
   *     comparing URI substrings.
   * @param keys - list of configured communication parameters that should
   *     be checked for matches.  For example, to compare against RPC addresses,
   *     provide the list DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
   *     DFS_NAMENODE_RPC_ADDRESS_KEY
   * @return - boolean confirmation if matched generic parameter
   */
  public static boolean isDefaultNamenodeAddress(Configuration conf,
      InetSocketAddress address, String... keys) {
    for (String key : keys) {
      String candidateAddress = conf.get(key);
      if (candidateAddress != null
          && address.equals(NetUtils.createSocketAddr(candidateAddress)))
        return true;
    }
    return false;
  }
  
  /**
   * @return key specific to a nameserviceId from a generic key
   */
  public static String getNameServiceIdKey(String key, String nameserviceId) {
    return key + "." + nameserviceId;
  }
  
  /** 
   * Sets the node specific setting into generic configuration key. Looks up
   * value of "key.nameserviceId" and if found sets that value into generic key 
   * in the conf. Note that this only modifies the runtime conf.
   * 
   * @param conf
   *          Configuration object to lookup specific key and to set the value
   *          to the key passed. Note the conf object is modified.
   * @param nameserviceId
   *          nameservice Id to construct the node specific key.
   * @param keys
   *          The key for which node specific value is looked up
   */
  public static void setGenericConf(Configuration conf,
      String nameserviceId, String... keys) {
    for (String key : keys) {
      String value = conf.get(getNameServiceIdKey(key, nameserviceId));
      if (value != null) {
        conf.set(key, value);
      }
    }
  }

 /**
  * @param address address of format host:port
  * @return InetSocketAddress for the address
  */
  public static InetSocketAddress getSocketAddress(String address) {
    int colon = address.indexOf(":");
    if (colon < 0) {
      return new InetSocketAddress(address, 0);
    }
    return new InetSocketAddress(address.substring(0, colon),
      Integer.parseInt(address.substring(colon + 1)));
  }
}
