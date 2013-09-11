package org.apache.hadoop.conf;

import java.util.Properties;

/**
 * Just a default implementation that simply returns the same configuration.
 */
public class DefaultClientConfiguration implements ClientConfiguration {

  @Override
  public String getConfiguration(String logicalClusterName, int timeoutMs,
      Properties props) {
    return null;
  }
}
