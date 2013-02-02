package org.apache.hadoop.conf;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.util.ReflectionUtils;

import org.json.JSONObject;

public class ClientConfigurationUtil {

  private static HashMap<Class<? extends ClientConfiguration>, ClientConfiguration> classCache = new HashMap<Class<? extends ClientConfiguration>, ClientConfiguration>();

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
    String json = getInstance(uri, conf).getConfiguration(
        uri.getHost(),
        conf.getInt("hdfs.retrieve.client_configuration_timeout", 3000),
        System.getProperties());
    if (json == null) {
      return conf;
    }
    Configuration newConf = new Configuration(conf);
    try {
      JSONObject jsonObj = new JSONObject(json);
      Configuration clientConf = new Configuration(jsonObj);
      Iterator<Map.Entry<String, String>> it = clientConf.iterator();
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        String key = entry.getKey();
        String val = entry.getValue();
        newConf.set(key, val);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return newConf;
  }
}
