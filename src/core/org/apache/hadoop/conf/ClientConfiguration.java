package org.apache.hadoop.conf;

import java.io.IOException;
import java.util.Properties;

public interface ClientConfiguration {

  /**
   * Retrieves the client configuration of an HDFS namenode as a json string.
   *
   * @param logicalClusterName
   *          the logical name of the HDFS cluster for that namenode
   * @param timeoutMs
   *          the timeout in milliseconds
   * @param props
   *          additional options can be passed in using the properties object so
   *          that the implementation can have some custom handling of certain
   *          configurations.
   * @return the json serialized string for the configuration or null in case of
   *         an error or timeout.
   */
  public String getConfiguration(String logicalClusterName, int timeoutMs,
      Properties props) throws IOException;
}
