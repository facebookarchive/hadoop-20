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
package org.apache.hadoop.ipc;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Register for all FastProtocol methods. The register stores mapping from
 * method unique identifier to method, as well as the mapping from method to its
 * unique identifier. This allows to obtain the method of a protocol without
 * reflection, and call it on an instance of implementing class.
 * 
 * Server side code must ensure that the methods are registered at startup,
 * before any calls are made by the clients.
 * 
 * The client side should also explicitly register the methods, although if they
 * are not registered, the server side will handle them in a usual way.
 */
public final class FastProtocolRegister {

  /**
   * All protocols with registered fast methods must implement this interface.
   * The rpc layer determines based on this interface whether a method can be
   * handled in the fast way.
   */
  public interface FastProtocol {
    // empty for now
  }

  public static final Log LOG = LogFactory.getLog(FastProtocolRegister.class
      .getName());

  // length of the serial unique code for FastProtocol methods
  private final static int NAME_LEN = 2;

  // maps id -> methods, and method -> id
  private final static Map<String, Method> idToMethod = new ConcurrentHashMap<String, Method>();
  private final static Map<Method, String> methodToId = new ConcurrentHashMap<Method, String>();

  /**
   * Registers a FastProtocol's method for a given id (name). The names should
   * be unique, it is not possible to re-use a name for a different method.
   */
  public static void register(FastProtocolId id, Method m) {
    synchronized (FastProtocolRegister.class) {
      String name = id.toString();
      if (name.length() != NAME_LEN) {
        // we only allow two-byte serialization codes
        throw new RuntimeException("Code must be " + NAME_LEN + " bytes long");
      }
      if (m == null) {
        throw new RuntimeException("Method cannot be null");
      }
      Method registeredMethod = idToMethod.get(name);
      if (registeredMethod != null && registeredMethod != m) {
        throw new RuntimeException(
            "Trying to register different method with name: " + name);
      } else if (registeredMethod == m) {
        return; // quietly
      }
      LOG.info("FastProtocol - Registering method: " + name + " method: "
          + m.getName());
      idToMethod.put(name, m);
      methodToId.put(m, name);
    }
  }

  /**
   * Tries to get a method given the id. 
   * Returns null if no such method is registered.
   */
  public static Method tryGetMethod(String id) {
    if (id.length() != NAME_LEN) {
      // we use it to fast discard the request without doing map lookup
      return null;
    }
    return idToMethod.get(id);
  }

  /**
   * Tries to get the id of FastProtocol's method. 
   * Returns null if no such method is registered.
   */
  public static String tryGetId(Method m) {
    return methodToId.get(m);
  }

  /**
   * Clear the register.
   */
  static void clear() {
    synchronized (FastProtocolRegister.class) {
      idToMethod.clear();
      methodToId.clear();
    }
  }

  /**
   * Serial versions for all FastProtocol's methods. Each id should be referred by
   * a single method!!!
   */
  public enum FastProtocolId {
    SERIAL_VERSION_ID_1("AA"), 
    SERIAL_VERSION_ID_2("AB"), 
    SERIAL_VERSION_ID_3("AC"), 
    SERIAL_VERSION_ID_4("AD"), 
    SERIAL_VERSION_ID_5("AE");

    private String name;

    FastProtocolId(String name) {
      this.name = name;
    }

    public String toString() {
      return name;
    }
  }
}
