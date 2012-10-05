package org.apache.hadoop.hdfs.server.namenode;

/**
 * This exception is thrown when {@link BlockPlacementPolicyConfigurable}
 * resolves a rack to {@link org.apache.hadoop.net.NetworkTopology#DEFAULT_RACK}
 */
public class DefaultRackException extends RuntimeException {
  public DefaultRackException(String text) {
    super(text);
  }
}
