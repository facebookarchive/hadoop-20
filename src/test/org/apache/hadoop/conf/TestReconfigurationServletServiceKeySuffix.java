package org.apache.hadoop.conf;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.fail;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.http.HttpServer;
import org.junit.Before;
import org.junit.Test;

public class TestReconfigurationServletServiceKeySuffix {
  private static final String CONF_FILE = "build/test/extraconf/core-site.xml";
  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());

  private static final int NAMESERVICE_COUNT = 3;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private NameNode[] namenodes;

  @Before
  public void setUp() throws IOException {
    File file = new File(CONF_FILE);
    file.deleteOnExit();
    conf = new Configuration();
    conf.setInt("fs.trash.interval", 5);

    cluster = new MiniDFSCluster(conf, 1, true, null, NAMESERVICE_COUNT);
    namenodes = new NameNode[NAMESERVICE_COUNT];
    for (int i = 0; i < NAMESERVICE_COUNT; i++) {
      namenodes[i] = cluster.getNameNode(i);
      assertNotNull("Namenode " + i + " is null.", namenodes[i]);
      assertEquals("Initial value for trash not set successfuly.", 5,
          namenodes[i].getTrashDeletionInterval() / TrashPolicyDefault.MSECS_PER_MINUTE);
    }
  }

  private void writeIntOnConfig(String key, Integer value) throws IOException {
    conf = new Configuration();
    conf.setInt(key, value);
    FileOutputStream fos = new FileOutputStream(CONF_FILE);
    conf.writeXml(fos);
  }

  private void postTrashAndTest(String key, int value, NameNode namenode, String desc) {
    HttpServer server = namenode.getHttpServer();
    assertNotNull("Server is null.", server);

    int port = server.getPort();
    String res = execPost("http://127.0.0.1:" + port + "/nnconfchange", key + "=" + value);
    LOG.info("The port number is: " + port);
    LOG.info(res);

    long trash = namenode.getTrashDeletionInterval() / TrashPolicyDefault.MSECS_PER_MINUTE;
    assertEquals(desc + " Trash Interval for " + namenode.getNameserviceID()
        + " not updated successfully.", value, trash);
  }

  @Test
  public void testServletOnApplyGenericConfiguration() throws IOException {
    writeIntOnConfig("fs.trash.interval", 3);
    for (int i = 0; i < NAMESERVICE_COUNT; i++)
      postTrashAndTest("fs.trash.interval", 3, namenodes[i], "Generic ");
  }

  @Test
  public void testServletOnApplyNameServiceSpecificConfiguration() throws IOException {
    for (int i = 0; i < NAMESERVICE_COUNT; i++)
      writeIntOnConfig("fs.trash.interval." + namenodes[i].getNameserviceID(), 10 + i);

    for (int i = 0; i < NAMESERVICE_COUNT; i++)
      postTrashAndTest("fs.trash.interval." + namenodes[i].getNameserviceID(), 10 + i,
          namenodes[i], "Specific ");
  }

  private static String execPost(String targetURL, String params) {
    URL url;
    HttpURLConnection connection = null;
    try {
      // Create connection
      url = new URL(targetURL);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");

      connection.setUseCaches(false);
      connection.setDoInput(true);
      connection.setDoOutput(true);

      // Send request
      DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
      wr.writeBytes(params);
      wr.flush();
      wr.close();

      // Get Response
      InputStream is = connection.getInputStream();
      BufferedReader rd = new BufferedReader(new InputStreamReader(is));
      String line;
      StringBuffer response = new StringBuffer();
      while ((line = rd.readLine()) != null) {
        response.append(line);
        response.append('\n');
      }
      rd.close();
      return response.toString();

    } catch (Exception e) {
      fail("Exception occured." + params + ". " + e.getMessage());
      return null;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

}
