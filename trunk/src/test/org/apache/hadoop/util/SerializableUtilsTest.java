package org.apache.hadoop.util;

import java.util.Random;

import static org.junit.Assert.*;
import org.junit.Test;

public class SerializableUtilsTest {

  private static Random random = new Random();

  @Test
  public void testInt() throws Exception {
    Integer i = random.nextInt();
    byte[] data = SerializableUtils.toBytes(i);
    assertEquals(i, SerializableUtils.getFromBytes(data, Integer.class));
  }

  @Test
  public void testLong() throws Exception {
    Long l = random.nextLong();
    byte[] data = SerializableUtils.toBytes(l);
    assertEquals(l, SerializableUtils.getFromBytes(data, Long.class));
  }

  @Test
  public void testString() throws Exception {
    String s = new String("" + random.nextLong());
    byte[] data = SerializableUtils.toBytes(s);
    assertEquals(s, SerializableUtils.getFromBytes(data, String.class));
  }
}
