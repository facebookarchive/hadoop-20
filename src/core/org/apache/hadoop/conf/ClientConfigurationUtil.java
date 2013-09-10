package org.apache.hadoop.conf;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;

import org.json.JSONObject;

public class ClientConfigurationUtil {
  private static Log LOG = LogFactory.getLog(ClientConfigurationUtil.class);

  private static HashMap<Class<? extends ClientConfiguration>, ClientConfiguration> classCache = new HashMap<Class<? extends ClientConfiguration>, ClientConfiguration>();

  // Map to keep track of bad URIs whose client configuration we were not
  // able to lookup. We keep this cache around so that we don't repeatedly
  // try to query a URI for which the configuration service consistently fails.
  private static final HashMap<String, Long> badURIs = new HashMap<String, Long>();
  private static final int BAD_URI_EXPIRY = 10 * 60 * 1000; // 10 minutes


  /**
   * Retrieves the implementation for {@link ClientConfiguration}. The
   * expectation is that either this will be specified in the default
   * configuration or it will be specified in the configuration of the client
   * via command line options and environment variables.
   *
   * @param conf
   *          the configuration for the client
   * @param uri
   *          the URI for the filesystem
   * @return
   */
  public static ClientConfiguration getInstance(URI uri, Configuration conf) {
    Class<? extends ClientConfiguration> clazz = conf.getClass("fs." + uri.getScheme()
        + ".client.configuration.impl",
        DefaultClientConfiguration.class, ClientConfiguration.class);
    ClientConfiguration clientConf = classCache.get(clazz);
    if (clientConf == null) {
      clientConf = (ClientConfiguration) ReflectionUtils.newInstance(clazz,
          null);
      classCache.put(clazz, clientConf);
    }
    return clientConf;
  }

  /**
   * Retrieves a modified version of the configuration from the supplied
   * configuration. This is used to override the configuration supplied with the
   * client side configuration retried from a different source.
   *
   * @param uri
   *          the URI for the FileSystem whose conf is needed
   * @param conf
   *          the default configuration
   * @return the modified client side configuration
   * @throws IOException
   */
  public static Configuration mergeConfiguration(URI uri, Configuration conf)
      throws IOException {
      try {
        Long lastBadAccess = badURIs.get(uri.getHost());
        if (lastBadAccess != null) {
            if (System.currentTimeMillis() - lastBadAccess < BAD_URI_EXPIRY) {
              return conf;
            } else {
              badURIs.remove(uri.getHost());
            }
        }
        boolean lookupLogical = conf.getBoolean(
            "dfs.client.configerator.logical.lookup.enabled", false);
        Properties props = new Properties(System.getProperties());
        props.setProperty("dfs.client.configerator.logical.lookup.enabled",
            lookupLogical + "");
        String configDir = conf.get("dfs.client.configerator.dir");
        if (configDir != null) {
          props.setProperty("dfs.client.configerator.dir", configDir);
        }
        String json = getInstance(uri, conf).getConfiguration(
            uri.getHost(),
            conf.getInt("hdfs.retrieve.client_configuration_timeout", 3000),
            props);
        if (json == null) {
          LOG.info("Client configuration lookup disabled/failed. "
              + "Using default configuration");
          return conf;
        }
        Configuration newConf = new Configuration(conf);
        JSONObject jsonObj = new JSONObject(json);
        Configuration clientConf = new Configuration(jsonObj);
        Iterator<Map.Entry<String, String>> it = clientConf.iterator();
        while (it.hasNext()) {
          Map.Entry<String, String> entry = it.next();
          String key = entry.getKey();
          String val = entry.getValue();
          newConf.set(key, val);
        }
        newConf.setBoolean("client.configuration.lookup.done", true);
        return newConf;
      } catch (Throwable t) {
        badURIs.put(uri.getHost(), System.currentTimeMillis());
        // In case of any error, fallback to the default configuration.
        LOG.info("Problem retreiving client side configuration " +
            ". Using default configuration instead", t);
        return conf;
      }
  }
}
