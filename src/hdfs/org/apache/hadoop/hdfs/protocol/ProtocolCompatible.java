package org.apache.hadoop.hdfs.protocol;

/**
 * This class provides a set of methods to check if two versions of a protocol
 * is compatible or not.
 *
 */
public class ProtocolCompatible {
  /**
   * Check if the client and NameNode have compatible ClientProtocol versions
   * 
   * @param clientVersion the version of ClientProtocol that client has
   * @param serverVersion the version of ClientProtocol that NameNode has
   * @return true if two versions are compatible
   */
  public static boolean isCompatibleClientProtocol(
      long clientVersion, long serverVersion) {
    return (clientVersion == ClientProtocol.RECOVER_LEASE_VERSION -1 ||
            clientVersion == ClientProtocol.RECOVER_LEASE_VERSION ||
            clientVersion == ClientProtocol.SAVENAMESPACE_FORCE  ||
            clientVersion == ClientProtocol.CLOSE_RECOVER_LEASE_VERSION) 
           &&
           (serverVersion == ClientProtocol.RECOVER_LEASE_VERSION - 1 ||
            serverVersion == ClientProtocol.RECOVER_LEASE_VERSION ||
            serverVersion == ClientProtocol.SAVENAMESPACE_FORCE ||
            serverVersion == ClientProtocol.CLOSE_RECOVER_LEASE_VERSION);
  }

  /**
   * Check if the client and DataNode have compatible ClientDataNodeProtocol versions
   * 
   * @param clientVersion the version of ClientDatanodeProtocol that client has
   * @param serverVersion the version of ClientDatanodeProtocol that DataNode has
   * @return true if two versions are compatible
   */
  public static boolean isCompatibleClientDatanodeProtocol(
      long clientVersion, long serverVersion) {
    return clientVersion == serverVersion ||
           (
            ( clientVersion == ClientDatanodeProtocol.GET_BLOCKINFO_VERSION-1 ||
              clientVersion == ClientDatanodeProtocol.GET_BLOCKINFO_VERSION ||
              clientVersion == ClientDatanodeProtocol.COPY_BLOCK_VERSION
            ) &&
            ( serverVersion == ClientDatanodeProtocol.GET_BLOCKINFO_VERSION-1 ||
              serverVersion == ClientDatanodeProtocol.GET_BLOCKINFO_VERSION ||
              serverVersion == ClientDatanodeProtocol.COPY_BLOCK_VERSION
           ));
  }
}
