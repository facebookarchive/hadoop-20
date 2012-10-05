package org.apache.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SerializableUtils {

  public static <T extends Serializable> byte[] toBytes(T obj)
      throws IOException {
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

  public static Object getFromBytes(byte[] data,
      Class<? extends Serializable> expectedType)
      throws IOException,
      ClassNotFoundException {
    if (data == null) {
      throw new IOException("data is null");
    }
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    ObjectInputStream is = null;
    Object obj = null;
    try {
      is = new ObjectInputStream(in);
      obj = is.readObject();
      if (obj.getClass() != expectedType) {
        throw new IOException(
            "The data provided is not a serialized object of type : "
                + expectedType);
      }
    } finally {
      if (is != null) {
        is.close();
      }
      in.close();
    }
    return obj;
  }
}
