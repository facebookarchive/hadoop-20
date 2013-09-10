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

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.io.*;
import java.util.Map;
import java.util.HashMap;

import javax.net.SocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.FastProtocolRegister.FastProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/** A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class RPC {
  private static final Log LOG =
    LogFactory.getLog(RPC.class);

  private RPC() {}                                  // no public ctor


  /** A method invocation, including the method name and its parameters.*/
  protected static class Invocation implements Writable, Configurable {
    private String methodName;
    private Class[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;

    public Invocation() {}

    public Invocation(Method method, String methodName, Object[] parameters) {
      this.methodName = methodName == null ? method.getName() : methodName;
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }

    public void readFields(DataInput in) throws IOException {
      methodName = UTF8.readString(in);
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      ObjectWritable.writeStringCached(out, methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf);
      }
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      return buffer.toString();
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return this.conf;
    }

  }

  /* Cache a client using its socket factory as the hash key */
  static private class ClientCache {
    private Map<SocketFactory, Client> clients =
      new HashMap<SocketFactory, Client>();

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory 
     * if no cached client exists.
     * 
     * @param conf Configuration
     * @return an IPC client
     */
    private synchronized Client getClient(Configuration conf,
        SocketFactory factory) {
      // Construct & cache client.  The configuration is only used for timeout,
      // and Clients have connection pools.  So we can either (a) lose some
      // connection pooling and leak sockets, or (b) use the same timeout for all
      // configurations.  Since the IPC is usually intended globally, not
      // per-job, we choose (a).
      Client client = clients.get(factory);
      if (client == null) {
        client = new Client(ObjectWritable.class, conf, factory);
        clients.put(factory, client);
      } else {
        client.incCount();
      }
      return client;
    }

    /**
     * Construct & cache an IPC client with the default SocketFactory 
     * if no cached client exists.
     * 
     * @param conf Configuration
     * @return an IPC client
     */
    private synchronized Client getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /**
     * Stop a RPC client connection 
     * A RPC client is closed only when its reference count becomes zero.
     */
    private void stopClient(Client client) {
      synchronized (this) {
        client.decCount();
        if (client.isZeroReference()) {
          clients.remove(client.getSocketFactory());
        }
      }
      if (client.isZeroReference()) {
        client.stop();
      }
    }
  }

  private static ClientCache CLIENTS=new ClientCache();
  
  private static class Invoker implements InvocationHandler {
    private InetSocketAddress address;
    private UserGroupInformation ticket;
    private Client client;
    private boolean isClosed = false;
    private boolean needCheckDnsUpdate = false;
    private long timeLastDnsCheck = 0;
    final private long MIN_DNS_CHECK_INTERVAL_MSEC = 120 * 1000 ;
    final private int rpcTimeout;
    final private Class<?> protocol;
    final private boolean fastProtocol;

    public Invoker(InetSocketAddress address, UserGroupInformation ticket, 
                   Configuration conf, SocketFactory factory, int rpcTimeout,
                   Class<?> protocol) {
      this.address = address;
      this.ticket = ticket;
      this.client = CLIENTS.getClient(conf, factory);
      this.rpcTimeout = rpcTimeout;
      this.protocol = protocol;
      this.fastProtocol = FastProtocol.class.isAssignableFrom(protocol);
    }
    
    private synchronized InetSocketAddress getAddress() {
      if (needCheckDnsUpdate
          && address != null
          && NetUtils.wasInitializedWithHostname(address.getAddress())
          && System.currentTimeMillis() - this.timeLastDnsCheck > MIN_DNS_CHECK_INTERVAL_MSEC) {
        try {
          InetSocketAddress newAddr = NetUtils.resolveAddress(address);
          if (newAddr != null) {
            LOG.info("DNS change: " + newAddr);
            address = newAddr;
          }
        } finally {
          this.timeLastDnsCheck = System.currentTimeMillis();
        }
      }
      needCheckDnsUpdate = false;
      return address;
    }

    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      final boolean logDebug = !fastProtocol && LOG.isDebugEnabled();
      long startTime = 0;
      if (logDebug) {
        startTime = System.currentTimeMillis();
      }

      ObjectWritable value = null;
      try {
        String name = null;
        if (fastProtocol) {
          // try to obtain registered name for the method
          name = FastProtocolRegister.tryGetId(method);
        } 
        value = (ObjectWritable) client.call(new Invocation(method, name, args),
            getAddress(), protocol, ticket, rpcTimeout, fastProtocol);
      } catch (RemoteException re) {
        throw re;
      } catch (ConnectException ce) {
        needCheckDnsUpdate = true;
        throw ce;
      } catch (NoRouteToHostException nrhe) {
        needCheckDnsUpdate = true;
        throw nrhe;
      } catch (PortUnreachableException pue) {
        needCheckDnsUpdate = true;
        throw pue;
      } catch (UnknownHostException uhe) {
        needCheckDnsUpdate = true;
        throw uhe;
      } 
      if (logDebug) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }
    
    /* close the IPC client that's responsible for this invoker's RPCs */ 
    synchronized private void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }
  }

  /**
   * An exception indicating that the client and server have
   * incompatible versions. They are not able to communicate with each other.
   */
  public static class VersionIncompatible extends IOException {
    private String interfaceName;
    private long clientVersion;
    private long serverVersion;
    
    /**
     * Create a version incompatible exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionIncompatible(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }
    
    /**
     * Get the interface name
     * @return the java class name 
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }
    
    /**
     * Get the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }
    
    /**
     * Get the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }
  
  /**
   * A version mismatch for the RPC protocol.
   *
   * The client & server have different versions.
   * But the server is not able to determine if they are compatible
   * mostly because the client is newer than the server.
   * So the proxy is created and the application client is left to decide
   * if the client & server are compatible or not.
   */
  public static class VersionMismatch extends VersionIncompatible {
    private static final long serialVersionUID = 1L;
    private final VersionedProtocol proxy;

    /**
     * Create a version mismatch exception
     *
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super(interfaceName, clientVersion, serverVersion);
      proxy = null;
    }

    /**
     * Create a version mismatch exception
     *
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     * @param proxy the proxy
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion, VersionedProtocol proxy) {
      super(interfaceName, clientVersion, serverVersion);
      this.proxy = proxy;
    }

    /**
     * Return the proxy
     */
    public VersionedProtocol getProxy() {
      return proxy;
    }
  }

  public static <T extends VersionedProtocol> T waitForProxy(
      Class<T> protocol,
      long clientVersion,
      InetSocketAddress addr,
      Configuration conf
      ) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
  }

  public static <T extends VersionedProtocol> ProtocolProxy<T> waitForProtocolProxy(
      Class<T> protocol,
      long clientVersion,
      InetSocketAddress addr,
      Configuration conf
      ) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr, conf, Long.MAX_VALUE);
  }

  public static <T extends VersionedProtocol> T waitForProxy(
      Class<T> protocol,
      long clientVersion,
      InetSocketAddress addr,
      Configuration conf,
      long timeout
      ) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr, conf, timeout).
                 getProxy();
  }

  public static <T extends VersionedProtocol> ProtocolProxy<T> waitForProtocolProxy(
      Class<T> protocol,
      long clientVersion,
      InetSocketAddress addr,
      Configuration conf,
      long timeout
      ) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr, conf, timeout, 0);
  }

  /**
   * Get a proxy connection to a remote server
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @param rpcTimeout timeout for each RPC
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T extends VersionedProtocol> T waitForProxy(Class<T> protocol,
                                                             long clientVersion,
                                                             InetSocketAddress addr,
                                                             Configuration conf,
                                                             long connTimeout,
                                                             int rpcTimeout) throws IOException {
		return waitForProtocolProxy(protocol, clientVersion, addr, conf,
          connTimeout, rpcTimeout).getProxy();
  }

  /**
   * Get a proxy connection to a remote server
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T extends VersionedProtocol> ProtocolProxy<T> waitForProtocolProxy(
                                               Class<T> protocol,
                                               long clientVersion,
                                               InetSocketAddress addr,
                                               Configuration conf,
                                               long timeout,
                                               int rpcTimeout
                                               ) throws IOException { 
    long startTime = System.currentTimeMillis();
    UserGroupInformation ugi = null;
    try {
      ugi = UserGroupInformation.login(conf);
    } catch (LoginException le) {
      throw new RuntimeException("Couldn't login!");
    }
    IOException ioe;
    while (true) {
      try {
        return getProtocolProxy(protocol, clientVersion, addr,
			ugi, conf,
			NetUtils.getDefaultSocketFactory(conf), rpcTimeout);
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
        ioe = se;
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      }
      // check if timed out
      if (System.currentTimeMillis()-timeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static <T extends VersionedProtocol> T getProxy(
      Class<T> protocol,
      long clientVersion, InetSocketAddress addr,
      Configuration conf, SocketFactory factory) throws IOException {
    return getProtocolProxy(protocol, clientVersion,
        addr, conf, factory).getProxy();
  }

  /** Construct a client-side protocol proxy that contains a set of server
   * methods and a proxy object implementing the named protocol,
   * talking to a server at the named address. */
  public static <T extends VersionedProtocol> ProtocolProxy<T> getProtocolProxy(
      Class<T> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf,
      SocketFactory factory) throws IOException {
    UserGroupInformation ugi = null;
    try {
      ugi = UserGroupInformation.login(conf);
    } catch (LoginException le) {
      throw new RuntimeException("Couldn't login!");
    }
    return getProtocolProxy(protocol, clientVersion, addr, ugi, conf, factory);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static <T extends VersionedProtocol> T getProxy(
      Class<T> protocol,
      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory) throws IOException {
    return getProtocolProxy(protocol, clientVersion,
        addr, ticket, conf, factory).getProxy();
  }

  /** Construct a client-side protocol proxy that contains a set of server
   * methods and a proxy object implementing the named protocol,
   * talking to a server at the named address. */
  public static <T extends VersionedProtocol> ProtocolProxy<T> getProtocolProxy(
      Class<T> protocol,
      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory) throws IOException {    
    return getProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory, 0);
  }

  /**
   * Construct a client-side proxy that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
  public static <T extends VersionedProtocol> T getProxy(
      Class<T> protocol,
		                          long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout) throws IOException {
    return getProtocolProxy(protocol, clientVersion, addr, ticket,
        conf, factory, rpcTimeout).getProxy();
  }

  /**
   * Construct a client-side proxy that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
  @SuppressWarnings("unchecked")
  public static <T extends VersionedProtocol> ProtocolProxy<T> getProtocolProxy(
      Class<T> protocol,
		                          long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout) throws IOException {
    T proxy = (T) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] { protocol },
            new Invoker(addr, ticket, conf, factory, rpcTimeout, protocol));
    String protocolName = protocol.getName();
    
    try {
      ProtocolSignature serverInfo = proxy
      .getProtocolSignature(protocolName, clientVersion,
          ProtocolSignature.getFingerprint(protocol.getMethods()));
      return new ProtocolProxy<T>(protocol, proxy, serverInfo.getMethods());
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException(IOException.class);
      if (ioe.getMessage().startsWith(IOException.class.getName() + ": "
          + NoSuchMethodException.class.getName())) {
        // Method getProtocolSignature not supported      
        long serverVersion = proxy.getProtocolVersion(protocol.getName(), 
            clientVersion);
        if (serverVersion == clientVersion) {
          return  new ProtocolProxy<T>(protocol, proxy, null);
        }
        throw new VersionMismatch(protocolName, clientVersion, 
            serverVersion, proxy);
      }
      throw re;
    }
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a proxy instance
   * @throws IOException
   */
  public static <T extends VersionedProtocol> T getProxy(
      Class<T> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf)
      throws IOException {
    return getProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a proxy instance
   * @throws IOException
   */
  public static <T extends VersionedProtocol> ProtocolProxy<T> getProtocolProxy(
      Class<T> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf)
      throws IOException {

    return getProtocolProxy(protocol, clientVersion, addr, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }

  /**
   * Stop this proxy and release its invoker's resource
   * @param <T>
   * @param proxy the proxy to be stopped
   */
  public static <T extends VersionedProtocol> void stopProxy(T proxy) {
    if (proxy!=null) {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    }
  }

  /** 
   * Expert: Make multiple, parallel calls to a set of servers.
   * @deprecated Use {@link #call(Method, Object[][], InetSocketAddress[], UserGroupInformation, Configuration)} instead 
   */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, Configuration conf)
    throws IOException {
    return call(method, params, addrs, null, conf);
  }
  
  /** Expert: Make multiple, parallel calls to a set of servers. */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs,
                              UserGroupInformation ticket, Configuration conf)
    throws IOException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, null, params[i]);
    Client client = CLIENTS.getClient(conf);
    try {
    Writable[] wrappedValues =
      client.call(invocations, addrs, method.getDeclaringClass(), ticket);

    if (method.getReturnType() == Void.TYPE) {
      return null;
    }

    Object[] values =
      (Object[])Array.newInstance(method.getReturnType(), wrappedValues.length);
    for (int i = 0; i < values.length; i++)
      if (wrappedValues[i] != null)
        values[i] = ((ObjectWritable)wrappedValues[i]).get();
    
    return values;
    } finally {
      CLIENTS.stopClient(client);
    }
  }

  static Client getClient(Configuration conf, SocketFactory socketFactory) {
    return CLIENTS.getClient(conf, socketFactory);
  }
  
  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf) 
    throws IOException {
    return getServer(instance, bindAddress, port, 1, false, conf);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public static Server getServer(final Object instance, final String bindAddress, final int port,
                                 final int numHandlers,
                                 final boolean verbose, Configuration conf) 
    throws IOException {
    return getServer(instance, bindAddress, port, numHandlers, verbose, conf,
        true);
  }  

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public static Server getServer(final Object instance,
      final String bindAddress, final int port, final int numHandlers,
      final boolean verbose, Configuration conf, boolean supportOldJobConf)
      throws IOException {
    return new Server(instance, conf, bindAddress, port, numHandlers, verbose,
        supportOldJobConf);
  }

  /** An RPC Server. */
  public static class Server extends org.apache.hadoop.ipc.Server {
    private Object instance;
    private final boolean fastProtocol;
    private boolean verbose;
    private boolean authorize = false;

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port) 
      throws IOException {
      this(instance, conf,  bindAddress, port, 1, false);
    }
    
    private static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length-1];
    }
    
    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public Server(Object instance, Configuration conf, String bindAddress,  int port,
                  int numHandlers, boolean verbose) throws IOException {
      this(instance, conf, bindAddress, port, numHandlers, verbose, true);
    }
    
    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param supportOldJobConf supports server to deserialize old job conf
     */
    public Server(Object instance, Configuration conf, String bindAddress,
        int port, int numHandlers, boolean verbose, boolean supportOldJobConf)
        throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf,
          classNameBase(instance.getClass().getName()), supportOldJobConf);
      this.instance = instance;
      this.fastProtocol = instance instanceof FastProtocol;
      this.verbose = verbose;
      this.authorize = 
        conf.getBoolean(ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, 
                        false);
    }

    public Writable call(Class<?> protocol, Writable param, long receivedTime) 
    throws IOException {
      try {
        Invocation call = (Invocation)param;
        if (verbose) log("Call: " + call);

        Method method = null;
        if (fastProtocol) {
          // try to obtain method directly from the register of FastProtocol
          // methods
          method = FastProtocolRegister.tryGetMethod(call.getMethodName());
        }
        if (method == null) {
          method = protocol.getMethod(call.getMethodName(),
              call.getParameterClasses());
        }
        method.setAccessible(true);

        int qTime = (int) (System.currentTimeMillis()-receivedTime);
        long startNanoTime = System.nanoTime();
        Object value = method.invoke(instance, call.getParameters());
        long processingMicroTime = (System.nanoTime() - startNanoTime) / 1000;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Served: " + method.getName() +
                    " queueTime (millisec)= " + qTime +
                    " procesingTime (microsec)= " + processingMicroTime);
        }
        rpcMetrics.rpcQueueTime.inc(qTime);
        rpcMetrics.rpcProcessingTime.inc(processingMicroTime);

        MetricsTimeVaryingRate m =
         (MetricsTimeVaryingRate) rpcMetrics.registry.get(method.getName());
      	if (m == null) {
      	  try {
      	    m = new MetricsTimeVaryingRate(method.getName(),
      	                                        rpcMetrics.registry);
      	  } catch (IllegalArgumentException iae) {
      	    // the metrics has been registered; re-fetch the handle
	    LOG.debug("Error register " + call.getMethodName(), iae);
      	    m = (MetricsTimeVaryingRate) rpcMetrics.registry.get(
      	        call.getMethodName());
      	  }
      	}
      	// record call time in microseconds
        m.inc(processingMicroTime);

        if (verbose) log("Return: "+value);

        return new ObjectWritable(method.getReturnType(), value);

      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        } else {
          IOException ioe = new IOException(target.toString());
          ioe.setStackTrace(target.getStackTrace());
          throw ioe;
        }
      } catch (Throwable e) {
        if (!(e instanceof IOException)) {
          LOG.error("Unexpected throwable object ", e);
        }
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }

    @Override
    public void authorize(Subject user, ConnectionHeader connection) 
    throws AuthorizationException {
      if (authorize) {
        Class<?> protocol = null;
        try {
          protocol = getProtocolClass(connection.getProtocol(), getConf());
        } catch (ClassNotFoundException cfne) {
          throw new AuthorizationException("Unknown protocol: " + 
                                           connection.getProtocol());
        }
        ServiceAuthorizationManager.authorize(user, protocol);
      }
    }
  }

  private static void log(String value) {
    if (value!= null && value.length() > 55)
      value = value.substring(0, 55)+"...";
    LOG.info(value);
  }
}
