/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import java.lang.reflect.Array;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.FastWritableRegister.FastWritable;

/** A polymorphic Writable that writes an instance with it's class name.
 * Handles arrays, strings and primitive types without a Writable wrapper.
 */
public class ObjectWritable implements Writable, Configurable {

  private Class declaredClass;
  private Object instance;
  private Configuration conf;

  public ObjectWritable() {}
  
  public ObjectWritable(Object instance) {
    set(instance);
  }

  public ObjectWritable(Class declaredClass, Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** Return the instance, or null if none. */
  public Object get() { return instance; }
  
  /** Return the class this is meant to be. */
  public Class getDeclaredClass() { return declaredClass; }
  
  /** Reset the instance. */
  public void set(Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }
  
  public String toString() {
    return "OW[class=" + declaredClass + ",value=" + instance + "]";
  }

  
  public void readFields(DataInput in) throws IOException {
    readObject(in, this, this.conf);
  }
  
  public void write(DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass, conf);
  }
  
  private final static int CACHE_MAX_SIZE = 10000;
  private final static int CACHE_CONCURRENCY_LEVEL = 500;
  /**
   * A map used for caching class names, in UTF format, prefixed with the length
   * of the name. With this we avoid expensive string transformation to UTF8.
   */
  private final static ConcurrentMap<String, byte[]> cachedByteClassNames 
    = new ConcurrentHashMap<String, byte[]>(
      CACHE_MAX_SIZE, 0.75f, CACHE_CONCURRENCY_LEVEL);
    
  /** A map used for caching classes, based on their name */
  private final static ConcurrentMap<String, Class<?>> cachedClassObjects 
    = new ConcurrentHashMap<String, Class<?>>(
      CACHE_MAX_SIZE, 0.75f, CACHE_CONCURRENCY_LEVEL);
  
  static {
    cachedClassObjects.put("boolean", Boolean.TYPE);
    cachedClassObjects.put("byte", Byte.TYPE);
    cachedClassObjects.put("char", Character.TYPE);
    cachedClassObjects.put("short", Short.TYPE);
    cachedClassObjects.put("int", Integer.TYPE);
    cachedClassObjects.put("long", Long.TYPE);
    cachedClassObjects.put("float", Float.TYPE);
    cachedClassObjects.put("double", Double.TYPE);
    cachedClassObjects.put("void", Void.TYPE);
    // String is very frequent parameter, we can obtain it very efficiently from here
    cachedClassObjects.put("java.lang.String", String.class);
  }

  private static class NullInstance extends Configured implements Writable {
    private Class<?> declaredClass;
    public NullInstance() { super(null); }
    public NullInstance(Class declaredClass, Configuration conf) {
      super(conf);
      this.declaredClass = declaredClass;
    }
    public void readFields(DataInput in) throws IOException {
      String className = UTF8.readString(in);
      declaredClass = getClassWithCaching(className, getConf());
    }
    public void write(DataOutput out) throws IOException {
      writeStringCached(out, declaredClass.getName());
    }
  }

  /** Write a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static void writeObject(DataOutput out, Object instance,
                                 Class declaredClass, 
                                 Configuration conf) throws IOException {

    if (instance == null) {                       // null
      instance = new NullInstance(declaredClass, conf);
      declaredClass = Writable.class;
    }
    
    // handle FastWritable first (including ShortVoid)
    if (instance instanceof FastWritable) {
      FastWritable fwInstance = (FastWritable) instance;
      byte[] serializedName = fwInstance.getSerializedName();
      out.write(serializedName, 0, serializedName.length);
      fwInstance.write(out);
      return;
    }
    
    // write declared name
    writeStringCached(out, declaredClass.getName());

    if (declaredClass.isArray()) {                // array
      int length = Array.getLength(instance);
      out.writeInt(length);
      for (int i = 0; i < length; i++) {
        writeObject(out, Array.get(instance, i),
                    declaredClass.getComponentType(), conf);
      }
      
    } else if (declaredClass == String.class) {   // String
      UTF8.writeStringOpt(out, (String)instance);
      
    } else if (declaredClass.isPrimitive()) {     // primitive type

      if (declaredClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((Boolean)instance).booleanValue());
      } else if (declaredClass == Character.TYPE) { // char
        out.writeChar(((Character)instance).charValue());
      } else if (declaredClass == Byte.TYPE) {    // byte
        out.writeByte(((Byte)instance).byteValue());
      } else if (declaredClass == Short.TYPE) {   // short
        out.writeShort(((Short)instance).shortValue());
      } else if (declaredClass == Integer.TYPE) { // int
        out.writeInt(((Integer)instance).intValue());
      } else if (declaredClass == Long.TYPE) {    // long
        out.writeLong(((Long)instance).longValue());
      } else if (declaredClass == Float.TYPE) {   // float
        out.writeFloat(((Float)instance).floatValue());
      } else if (declaredClass == Double.TYPE) {  // double
        out.writeDouble(((Double)instance).doubleValue());
      } else if (declaredClass == Void.TYPE) {    // void
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }
    } else if (declaredClass.isEnum()) {         // enum
      writeStringCached(out, ((Enum)instance).name());
    } else if (Writable.class.isAssignableFrom(declaredClass)) { // Writable
      writeStringCached(out, instance.getClass().getName());
      ((Writable)instance).write(out);

    } else {
      throw new IOException("Can't write: "+instance+" as "+declaredClass);
    }
  }
  
  /**
   * This class should be used for writing only class/method names!!!!
   */
  public static void writeStringCached(DataOutput out, String entityName)
      throws IOException {
    byte[] name = getByteNameWithCaching(entityName);
    // name should never be null at this point
    if (name == null) {
      throw new RuntimeException("Cannot retrieve class name");
    }
    out.write(name, 0, name.length);
  }
  
  /**
   * Retrieve byte[] for given name. This should be done only for
   * class and method names. the return value represents length and
   * name as a byte array. If the name is not present, cache it, if
   * the map max capacity is not exceeded.
   */
  private static byte[] getByteNameWithCaching(String entityName) {
    byte[] name = cachedByteClassNames.get(entityName);
    if (name == null) {
      name = prepareCachedNameBytes(entityName);
      // if the cache max capacity is not exceeded, cache the name
      if (cachedByteClassNames.size() < CACHE_MAX_SIZE) {
        cachedByteClassNames.put(entityName, name);
      }
    }
    // this should never be null
    return name;
  }
  
  /**
   * Prepares a byte array for given name together with lenght
   * as the two trailing bytes.
   */
  public static byte[] prepareCachedNameBytes(String entityName) {
    UTF8 name = new UTF8();
    name.set(entityName, true);
    byte nameBytes[] = name.getBytes();
    byte cachedName[] = new byte[nameBytes.length + 2];
    System.arraycopy(nameBytes, 0, cachedName, 2, nameBytes.length);
    // we cache the length as well
    int v = nameBytes.length;
    
    cachedName[0] = (byte)((v >>> 8) & 0xFF);
    cachedName[1] = (byte)((v >>> 0) & 0xFF);
    return cachedName;
  }
  
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static Object readObject(DataInput in, Configuration conf)
    throws IOException {
    return readObject(in, null, conf);
  }
  
  /**
   * Retrieve Class for given name from cache. if not present,
   * cache it, it the map capacity is not exceeded.
   */
  private static Class<?> getClassWithCaching(String className,
      Configuration conf) {
    Class<?> classs = cachedClassObjects.get(className);
    if (classs == null) {
      try {
        classs = conf.getClassByName(className);
        if (cachedClassObjects.size() < CACHE_MAX_SIZE) {
          cachedClassObjects.put(className, classs);
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("readObject can't find class " + className,
            e);
      }
    }
    // for sanity check if the class is not null
    if (classs == null) {
      throw new RuntimeException("readObject can't find class " + className);
    }
    return classs;
  }
 
  private static boolean initializedSupportJobConf = false;
  private static boolean supportJobConf = true;
  
  private static void setObjectWritable(ObjectWritable objectWritable,
      Class<?> declaredClass, Object instance) {
    if (objectWritable != null) { // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }
  }
    
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  @SuppressWarnings("unchecked")
  public static Object readObject(DataInput in, ObjectWritable objectWritable, Configuration conf)
    throws IOException {
    String className = UTF8.readString(in);
    
    // fast processing of ShortVoid
    if (FastWritableRegister.isVoidType(className)) {
      setObjectWritable(objectWritable, ShortVoid.class, ShortVoid.instance);
      return ShortVoid.instance;
    }
    
    // handle fast writable objects first
    FastWritable fwInstance = FastWritableRegister.tryGetInstance(className,
        conf);
    if (fwInstance != null) {
      fwInstance.readFields(in);
      setObjectWritable(objectWritable, fwInstance.getClass(), fwInstance);
      return fwInstance;
    }
    
    Class<?> declaredClass = getClassWithCaching(className, conf);
    
    // read once whether we should support jobconf
    // HDFS does not need to support this, which saves on Writable.newInstance
    if (!initializedSupportJobConf) {
      supportJobConf = conf.getBoolean("rpc.support.jobconf", true);
      // since this is not synchronized, there is a race condition here
      // but we are ok with two instances initializing this
      // if the operations are re-ordered, it is still fine, another thread
      // might use "true" instead of false for one time
      initializedSupportJobConf = true;
    }

    Object instance;
    
    if (declaredClass.isPrimitive()) {            // primitive types

      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = Character.valueOf(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = Byte.valueOf(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = Short.valueOf(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = Integer.valueOf(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = Long.valueOf(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = Float.valueOf(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = Double.valueOf(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }

    } else if (declaredClass.isArray()) {              // array
      int length = in.readInt();
      instance = Array.newInstance(declaredClass.getComponentType(), length);
      for (int i = 0; i < length; i++) {
        Array.set(instance, i, readObject(in, conf));
      }
      
    } else if (declaredClass == String.class) {        // String
      instance = UTF8.readString(in);
    } else if (declaredClass.isEnum()) {         // enum
      instance = Enum.valueOf((Class<? extends Enum>) declaredClass, UTF8.readString(in));
    } else {                                      // Writable
      String str = UTF8.readString(in);
      Class instanceClass = getClassWithCaching(str, conf);
      
      Writable writable = WritableFactories.newInstance(instanceClass, conf, supportJobConf);
      writable.readFields(in);
      instance = writable;

      if (instanceClass == NullInstance.class) {  // null
        declaredClass = ((NullInstance)instance).declaredClass;
        instance = null;
      }
    }
    setObjectWritable(objectWritable, declaredClass, instance);
    return instance;
      
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
  
}
