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
    return clientVersion == serverVersion ||
           (
            ( clientVersion == ClientProtocol.OPTIMIZE_FILE_STATUS_VERSION-1 ||
              clientVersion == ClientProtocol.OPTIMIZE_FILE_STATUS_VERSION ||
              clientVersion == ClientProtocol.ITERATIVE_LISTING_VERSION ||
              clientVersion == ClientProtocol.BULK_BLOCK_LOCATIONS_VERSION ||
              clientVersion == ClientProtocol.CONCAT_VERSION ||
              clientVersion == ClientProtocol.LIST_CORRUPT_FILEBLOCKS_VERSION ||
              clientVersion == ClientProtocol.SAVENAMESPACE_FORCE ||
              clientVersion == ClientProtocol.RECOVER_LEASE_VERSION ||
              clientVersion == ClientProtocol.CLOSE_RECOVER_LEASE_VERSION
            ) &&
            ( serverVersion == ClientProtocol.OPTIMIZE_FILE_STATUS_VERSION-1 ||
              serverVersion == ClientProtocol.OPTIMIZE_FILE_STATUS_VERSION ||
              serverVersion == ClientProtocol.ITERATIVE_LISTING_VERSION ||
              serverVersion == ClientProtocol.BULK_BLOCK_LOCATIONS_VERSION  ||
              serverVersion == ClientProtocol.CONCAT_VERSION ||
              serverVersion == ClientProtocol.LIST_CORRUPT_FILEBLOCKS_VERSION ||
              serverVersion == ClientProtocol.SAVENAMESPACE_FORCE ||
              serverVersion == ClientProtocol.RECOVER_LEASE_VERSION ||
              serverVersion == ClientProtocol.CLOSE_RECOVER_LEASE_VERSION
           ));
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
