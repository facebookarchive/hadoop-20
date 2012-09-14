package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestZookeeperTxId {

  private static Log LOG = LogFactory.getLog(TestZookeeperTxId.class);

  @Test
  public void testSerialize() throws Exception {
    ZookeeperTxId before = new ZookeeperTxId(0, 1, 2, 3);
    byte[] data = before.toBytes();
    ZookeeperTxId after = ZookeeperTxId.getFromBytes(data);
    assertEquals(before, after);
  }

  @Test
  public void testNullData() throws Exception {
    byte[] data = null;
    try {
      ZookeeperTxId.getFromBytes(data);
    } catch (IOException e) {
      LOG.info("Expected exception", e);
      return;
    }
    fail("Did not throw exception");
  }

  @Test
  public void testBadData() throws Exception {
    byte[] data = new String("BAD DATA!!!").getBytes();
    try {
      ZookeeperTxId.getFromBytes(data);
    } catch (IOException e) {
      LOG.info("Expected exception", e);
      return;
    }
    fail("Did not throw exception");
  }


  @Test
  public void testWrongObject() throws Exception {
    String str = new String("Wrong Object!!!");
    byte[] data = serialize(str);
    try {
      ZookeeperTxId.getFromBytes(data);
    } catch (IOException e) {
      LOG.info("Expected exception", e);
      return;
    }
    fail("Did not throw exception");
  }

  private byte[] serialize(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    byte[] buff = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(obj);
      buff = bos.toByteArray();
    } finally {
      if (out != null) {
        out.close();
      }
      bos.close();
    }
    return buff;
  }
}
