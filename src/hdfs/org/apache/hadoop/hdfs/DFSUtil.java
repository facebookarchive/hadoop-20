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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockAndLocation;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlockFlags;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;

public class DFSUtil {
  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };
  
  /** @return a pseudo random number generator. */
  public static Random getRandom() {
    return RANDOM.get();
  }
  
  /**
   * Compartor for sorting DataNodeInfo[] based on decommissioned states.
   * Decommissioned nodes are moved to the end of the array on sorting with
   * this compartor.
   */
  public static final Comparator<DatanodeInfo> DECOM_COMPARATOR = 
    new Comparator<DatanodeInfo>() {
    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      if (a.isDecommissioned() != b.isDecommissioned()) {
        return a.isDecommissioned() ? 1 : -1;
      } else if (a.isSuspectFail() != b.isSuspectFail()) {
        return a.isSuspectFail() ? 1 : -1;
      } else {
        return 0;
      }
      
    }
  };
  
  private static final String utf8charsetName = "UTF8";
  
  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2String(byte[][] pathComponents) {
    if (pathComponents.length == 0)
      return "";
    if (pathComponents.length == 1 && pathComponents[0].length == 0) {
      return Path.SEPARATOR;
    }

    StringBuilder result = new StringBuilder();
    for (int i = 0; i < pathComponents.length; i++) {
      String converted = bytes2String(pathComponents[i]);
      if (converted == null)
        return null;
      result.append(converted);
      if (i < pathComponents.length - 1) {
        result.append(Path.SEPARATOR_CHAR);
      }
    }
    return result.toString();
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
      final int len = bytes.length;
      char[] charArray = UTF8.getCharArray(len);
      for (int i = 0; i < bytes.length; i++) {
        if (bytes[i] < UTF8.MIN_ASCII_CODE) {
          // non-ASCII codepoints' higher bytes
          // are of the form (10xxxxxx), hence the bytes
          // represent a non-ASCII string
          // do expensive conversion
          return new String(bytes, utf8charsetName);
        }
        // copy to temporary array
        charArray[i] = (char) bytes[i];
      }
      // only ASCII bytes, do fast conversion
      // using bytes as actual characters
      return new String(charArray, 0, len);
    } catch (UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }
   
  /**
   * Converts a string to a byte array using UTF8 encoding. 
   */
  public static byte[] string2Bytes(String str) {
    try {
      final int len = str.length();
      // if we can, we will use it to return the bytes
      byte[] rawBytes = new byte[len];
      
      // get all chars of the given string
      char[] charArray = UTF8.getCharArray(len);
      str.getChars(0, len, charArray, 0);
      
      for (int i = 0; i < len; i++) {
        if (charArray[i] > UTF8.MAX_ASCII_CODE) {
          // non-ASCII chars present
          // do expensive conversion
          return str.getBytes(utf8charsetName);
        }
        // copy to output array
        rawBytes[i] = (byte) charArray[i];
      }
      // only ASCII present - return raw bytes
      return rawBytes;
    } catch (UnsupportedEncodingException e) {
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
      return new byte[][] { new byte[0] };
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
   * An implementation of the String.split(String);
   */
  public static String[] split(String str, char separator) {
    final int len;
    if (str == null 
        || ((len = str.length()) == 0)) {
      return null;
    }

    List<String> componentList = new ArrayList<String>();
    int startIndex = 0;

    for (int i = 0; i < len; i++) {
      if (str.charAt(i) == separator) {
        componentList.add(str.substring(startIndex, i));
        startIndex = i + 1;
      }
    }

    if (str.charAt(len - 1) != separator) {
      componentList.add(str.substring(startIndex, len));
    }

    return componentList.toArray(new String[componentList.size()]);
  }

  public static byte[][] splitAndGetPathComponents(String str) {
    try {
      final int len;
      if (str == null 
          || ((len = str.length()) == 0)
          || str.charAt(0) != Path.SEPARATOR_CHAR) {
        return null;
      }
  
      char[] charArray = UTF8.getCharArray(len);
      str.getChars(0, len, charArray, 0);
      
      // allocate list for components
      List<byte[]> componentByteList = new ArrayList<byte[]>(20);
  
      // for each component to know if it has only ascii chars
      boolean canFastConvert = true;
      int startIndex = 0;
  
      for (int i = 0; i < len; i++) {
        if (charArray[i] == Path.SEPARATOR_CHAR) {
          componentByteList.add(extractBytes(str, startIndex, i, charArray,
              canFastConvert));
          startIndex = i + 1;
          // assume only ASCII chars
          canFastConvert = true;
        } else if (charArray[i] > UTF8.MAX_ASCII_CODE) {
          // found non-ASCII char, we will do 
          // full conversion for this component
          canFastConvert = false;
        }
      }
      // the case when the last component is a filename ("/" not at the end)
      if (charArray[len - 1] != Path.SEPARATOR_CHAR) {
        componentByteList.add(extractBytes(str, startIndex, len, charArray,
            canFastConvert));
      }
      int last = componentByteList.size(); // last split
      while (--last>=1 && componentByteList.get(last).length == 0) {
        componentByteList.remove(last);
      }
      return componentByteList.toArray(new byte[last+1][]);
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  /**
   * Helper for extracting bytes either from "str"
   * or from the array of chars "charArray",
   * depending if fast conversion is possible,
   * specified by "canFastConvert"
   */  
  private static byte[] extractBytes(
      String str, 
      int startIndex, 
      int endIndex,
      char[] charArray, 
      boolean canFastConvert) throws UnsupportedEncodingException {
    if (canFastConvert) {
      // fast conversion, just copy the raw bytes
      final int len = endIndex - startIndex;
      byte[] strBytes = new byte[len];
      for (int i = 0; i < len; i++) {
        strBytes[i] = (byte) charArray[startIndex + i];
      }
      return strBytes;
    } 
    // otherwise, do expensive conversion
    return str.substring(startIndex, endIndex).getBytes(utf8charsetName);
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
  * Convert a LocatedBlocks to BlockAndLocations[]
  * @param blocks a LocatedBlocks
  * @return an array of BlockLocations
  */
 public static BlockAndLocation[] locatedBlocks2BlockLocations(
     LocatedBlocks blocks) {
   if (blocks == null) {
     return new BlockAndLocation[0];
   }
   int nrBlocks = blocks.locatedBlockCount();
   BlockAndLocation[] blkLocations = new BlockAndLocation[nrBlocks];
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
     Block block = blk.getBlock();
     blkLocations[idx] = new BlockAndLocation(block.getBlockId(),
                                           block.getGenerationStamp(),
                                           names, hosts, racks,
                                           blk.getStartOffset(),
                                           block.getNumBytes(),
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
  public static boolean isDeleted(Block block) {
    return block.getNumBytes() == BlockFlags.DELETED;
  }
  
  public static void markAsDeleted(Block block) {
    block.setNumBytes(BlockFlags.DELETED);
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
   * Returns list of InetSocketAddresses corresponding to namenodes from the
   * configuration. 
   * @param suffix 0 or 1 indicating if this is AN0 or AN1
   * @param conf configuration
   * @param keys Set of keys
   * @return list of InetSocketAddress
   * @throws IOException on error
   */
  public static List<InetSocketAddress> getRPCAddresses(String suffix,
      Configuration conf, Collection<String> serviceIds, String... keys) 
          throws IOException {
    // Use default address as fall back
    String defaultAddress = null;
    try {
      defaultAddress = conf.get(FileSystem.FS_DEFAULT_NAME_KEY + suffix);
      if (defaultAddress != null) {
        defaultAddress = NameNode.getDefaultAddress(conf);
      }
    } catch (IllegalArgumentException e) {
      defaultAddress = null;
    }
    
    for (int i = 0; i < keys.length; i++) {
      keys[i] += suffix;
    }
    
    List<InetSocketAddress> addressList = DFSUtil.getAddresses(conf,
        serviceIds, defaultAddress,
        keys);
    if (addressList == null) {
      String keyStr = "";
      for (String key: keys) {
        keyStr += key + " ";
      }
      throw new IOException("Incorrect configuration: namenode address "
          + keyStr
          + " is not configured.");
    }
    return addressList;
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
      Configuration conf, Collection<String> suffixes) throws IOException {

    List<InetSocketAddress> addressList; 
    if(suffixes != null && !suffixes.isEmpty()){
      addressList = new ArrayList<InetSocketAddress>();
      for (String s : suffixes) {
        addressList.addAll(getRPCAddresses(s, conf, getNameServiceIds(conf),
            FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY));
      }
    } else {
      // Use default address as fall back
      String defaultAddress;
      try {
        defaultAddress = NameNode.getDefaultAddress(conf);
      } catch (IllegalArgumentException e) {
        defaultAddress = null;
      }
      addressList = getAddresses(conf, defaultAddress,
        FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
    }
    if (addressList == null || addressList.isEmpty()) {
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
      defaultAddress = NameNode.getDefaultAddress(conf);
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
   * @param isAvatar - whether it's avatar config
   * @return server http
   */
  public static String getInfoServer(
      InetSocketAddress namenode, Configuration conf, boolean isAvatar) {
    String httpAddressDefault = 
        NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                                  "dfs.info.port", "dfs.http.address"); 
    String httpAddress = null;
    if(namenode != null) {
      if (!isAvatar) {
        // if non-default namenode, try reverse look up 
        // the nameServiceID if it is available
        String nameServiceId = DFSUtil.getNameServiceIdFromAddress(
            conf, namenode,
            FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);

        if (nameServiceId != null) {
          httpAddress = conf.get(DFSUtil.getNameServiceIdKey(
              FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY, nameServiceId));
        }
      } else {
        // federated, avatar addresses
        String suffix = "0";
        String nameServiceId = DFSUtil.getNameServiceIdFromAddress(
            conf, namenode,
            FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY + "0");
        if (nameServiceId == null) {
          nameServiceId = DFSUtil.getNameServiceIdFromAddress(
              conf, namenode,
              FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY + "1");
          suffix = "1";
        }
        if (nameServiceId != null) {
          httpAddress = conf.get(DFSUtil.getNameServiceIdKey(
              FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY + suffix, nameServiceId));
        }
        
        // federated, avatar addresses - ok
        if (httpAddress != null) {
          return httpAddress;
        }
        
        // non-federated, avatar adresses
        httpAddress = getNonFederatedAvatarInfoServer(namenode, "0", conf);
        if (httpAddress != null) {
          return httpAddress;
        }
        httpAddress = getNonFederatedAvatarInfoServer(namenode, "1", conf);     
      }
    }

    // else - Use non-federation, non-avatar configuration
    if (httpAddress == null) {
      httpAddress = conf.get(FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY, httpAddressDefault);
    }
    return httpAddress;
  }
  
  private static String getNonFederatedAvatarInfoServer(
      InetSocketAddress namenode, String suffix, Configuration conf) {
    String rpcAddress = conf.get("fs.default.name" + suffix);
    if (rpcAddress != null && !rpcAddress.isEmpty()
        && NetUtils.createSocketAddr(rpcAddress).equals(namenode)) {
      return conf.get(FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY + suffix);
    }
    return null;
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

  public static DistributedFileSystem convertToDFS(FileSystem fs) {
    // for RaidDFS
    if (fs instanceof FilterFileSystem) {
      fs = ((FilterFileSystem) fs).getRawFileSystem();
    }
    if (fs instanceof DistributedFileSystem)
      return (DistributedFileSystem) fs;
    else
      return null;
  }
  
  /*
   * Connect to the some url to get the html content
   */
  public static String getHTMLContent(URI uri) throws IOException,
      SocketTimeoutException {
    return getHTMLContentWithTimeout(uri.toURL(), 0, 0); 
  }
  
  public static String getHTMLContentWithTimeout(URL path, int connectTimeout, 
      int readTimeout) throws IOException, SocketTimeoutException  {
    InputStream stream = null;
    URLConnection connection = path.openConnection();
    if (connectTimeout > 0) {
      connection.setConnectTimeout(connectTimeout);
    }
    if (readTimeout > 0) {
      connection.setReadTimeout(readTimeout);
    }
    stream = connection.getInputStream();
    BufferedReader input = new BufferedReader(
        new InputStreamReader(stream));
    StringBuilder sb = new StringBuilder();
    String line = null;
    try {
      while (true) {
        line = input.readLine();
        if (line == null) {
          break;
        }
        sb.append(line + "\n");
      }
      return sb.toString();
    } finally {
      input.close();
    }
  }
  
  /**
   * Given the start time in nano seconds, return the elapsed time in
   * microseconds.
   */
  public static long getElapsedTimeMicroSeconds(long start) {
    return (System.nanoTime() - start) / 1000;
  }
  
  public static String getStackTrace() {
    StringBuilder sb = new StringBuilder();
    sb.append("------------------------------------\n");
    sb.append("Current stack trace:\n\n");
    StackTraceElement[] trace = Thread.currentThread().getStackTrace();    
    for(StackTraceElement se : trace) {
      sb.append(se + "\n");
    }
    return sb.toString();   
  }

  public static void throwAndLogIllegalState(String message, Log LOG) {
    IllegalStateException ise = new IllegalStateException(message);
    LOG.error(ise);
    throw ise;
  }
  
  public static String getAllStackTraces() {
    StringBuilder sb = new StringBuilder();
    sb.append("------------------------------------\n");
    sb.append("All stack traces:\n\n");
    Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();    
    for(Entry<Thread, StackTraceElement[]> e : traces.entrySet()) {
      sb.append("------------------------------------\n");
      sb.append(e.getKey() + "\n\n");
      for(StackTraceElement se : e.getValue()) {
        sb.append(se + "\n");
      }
    }
    return sb.toString();   
  }
}
