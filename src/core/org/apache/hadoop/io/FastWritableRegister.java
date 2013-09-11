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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Register for all FastWritable classes. The register stores mapping from class
 * serial id (2 bytes long String), to an instance of the class, which is used
 * to get a new instance of this class when deserializing objects.
 * 
 * Server side hdfs code must ensure that each FastWritable class is registered
 * before any deserialization calls are made when serving client traffic.
 */
public final class FastWritableRegister {
  
  /**
   * Implementing classes can be serialized and deserialized much faster. All
   * implementing classes should be final.
   */
  public interface FastWritable extends Writable {

    /**
     * Get a new instance of the class. Each call of this method should return a
     * distinct instance.
     */
    public FastWritable getFastWritableInstance(Configuration conf);

    /**
     * Get name for this class used for fast serialization in the serialized form
     * using UTF8 encoding. The array contains two bytes which store the length of
     * the string.
     */
    public byte[] getSerializedName();
  }
  
  public static final Log LOG = LogFactory.getLog(FastWritableRegister.class
      .getName());

  // length of the serial unique code for FastWritable classes
  private final static int NAME_LEN = 2;

  // maps serialVersionUi (String of length 2) to an instance of a class
  private final static Map<String, FastWritable> register = new ConcurrentHashMap<String, FastWritable>();

  /**
   * Registers a FastWritable class for a given id (name). The names should be
   * unique, it is not possible to re-use a name for a different class. If we
   * re-register the same class, the object of the registered class must be the
   * same (the same reference).
   */
  public static void register(FastWritableId id, FastWritable fw) {
    synchronized (FastWritableRegister.class) {
      String name = id.toString();
      if (name.length() != NAME_LEN) {
        // we only allow two-byte serialization codes
        throw new RuntimeException("Code must be " + NAME_LEN + " bytes long");
      }
      if (fw == null) {
        throw new RuntimeException("Instance cannot be null");
      }
      FastWritable registeredWritable = register.get(name);
      if (registeredWritable != null && registeredWritable != fw) {
        throw new RuntimeException(
            "Trying to register different class with name: " + name);
      } else if (registeredWritable == fw) {
        return; //quietly
      } 
      LOG.info("FastWritable - Registering name: " + name);
      register.put(name, fw);
    }
  }

  /**
   * Tries to get an instance given the name of class. If the name is registered
   * as FastWritable, the instance is obtained using the registry.
   */
  public static FastWritable tryGetInstance(String name, Configuration conf) {
    if (name.length() != NAME_LEN) {
      // we use it to fast discard the request without doing map lookup
      return null;
    }
    FastWritable fw = register.get(name);
    return fw == null ? null : fw.getFastWritableInstance(conf);
  }
  
  public static boolean isVoidType(String name) {
    if (name.length() != NAME_LEN) {
      // we use it to fast discard the request
      return false;
    }
    return FastWritableId.SERIAL_VERSION_ID_VOID.toString().equals(name);
  }

  /**
   * Clear the register.
   */
  static void clear() {
    register.clear();
  }
  
  /**
   * Serial versions for all FastWritable classes.
   * Each id should be referred by a single class!!!
   */
  public enum FastWritableId {
    SERIAL_VERSION_ID_1("AA"), 
    SERIAL_VERSION_ID_2("AB"), 
    SERIAL_VERSION_ID_3("AC"), 
    SERIAL_VERSION_ID_4("AD"), 
    SERIAL_VERSION_ID_5("AE"),
    
    // special id for returnable void
    SERIAL_VERSION_ID_VOID("VV");

    private String name;

    FastWritableId(String name) {
      this.name = name;
    }

    public String toString() {
      return name;
    }
  }
}
