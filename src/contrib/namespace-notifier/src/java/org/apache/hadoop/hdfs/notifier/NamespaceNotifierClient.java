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
package org.apache.hadoop.hdfs.notifier;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.NotConnectedToServerException;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.ServerAlreadyKnownException;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.ServerNotKnownException;
import org.apache.hadoop.hdfs.notifier.EventType;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.collect.Maps;

/**
 * Used by the client to subscribe to the namespace notifier server. 
 */
public class NamespaceNotifierClient implements Runnable {
  public static final Log LOG = LogFactory.getLog(NamespaceNotifierClient.class);
  
  // The object that will get our callbacks
  Watcher watcher;
  
  String listeningHost;
  int listeningPort;

  // All the watches that this client has placed.
  // Mapping event to the last transaction id received
  ConcurrentMap<NamespaceEventKey, Long> watchedEvents;
  
  // When we should shutdown
  volatile boolean shouldShutdown = false;
  
  // The thrift handler implementation
  ClientHandler.Iface handler;
  
  TServer tserver;
  
  ConnectionManager connectionManager;
  
  Random generator;

  
  /**
   * Constructor used when the Namespace Notification Server is running just
   * on one machine.
   * 
   * @param watcher The notified component
   * @param host The notification server hostname or IP address
   * @param port The notification server listening port
   * @param listeningPort the port on which this client should start the
   *        thrift service.
   * @throws TException when failing to connect to the server.
   */
  public NamespaceNotifierClient(Watcher watcher,
      String host, int port, int listeningPort) throws TException {
    this(watcher, Arrays.asList(host), port, listeningPort);
  }


  /**
   * Constructor used when the Namespace Notification server is running on
   * multiple machines, but on all machines it's listening on the same port
   * number.
   * 
   * @param watcher The notified component
   * @param hosts The notification servers hostnames or IP addresses
   * @param port The notification servers listening port
   * @param listeningPort the port on which this client should start the
   *        thrift service.
   * @throws TException when failing to connect to the server.
   */
  public NamespaceNotifierClient(Watcher watcher,
      List<String> hosts, int port, int listeningPort)
          throws TException {
    this(watcher, hosts, Arrays.asList(port), listeningPort);
  }


  /**
   * Constructor used when the Namespace Notification server is running on
   * multiple machines and it's not listening on the same port number on all
   * machines.
   * 
   * @param watcher The notified component
   * @param hosts The notification servers hostnames or IP addresses
   * @param ports The notification servers listening ports
   * @param listeningPort the port on which this client should start the
   *        thrift service.
   * @throws TException when failing to connect to the server.
   */
  public NamespaceNotifierClient(Watcher watcher,
      List<String> hosts, List<Integer> ports, int listeningPort)
          throws TException {
    this.watcher = watcher;
    watchedEvents = new ConcurrentHashMap<NamespaceEventKey, Long>();
    
    try {
       listeningHost = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new TException(e);
    }
    this.listeningPort = listeningPort;
    handler = new ClientHandlerImpl(this);
    
    // Ensure pseudo-random seed between clients
    long seed = System.currentTimeMillis() + listeningPort +
        listeningHost.hashCode();
    if (LOG.isDebugEnabled()) {
      LOG.debug(listeningPort + ": using seed " + seed);
    }
    generator = new Random(seed);
    
    // Setup the Thrift server
    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TTransportFactory transportFactory = new TFramedTransport.Factory();
    TNonblockingServerTransport serverTransport;
    ClientHandler.Processor<ClientHandler.Iface> processor =
        new ClientHandler.Processor<ClientHandler.Iface>(handler);
    serverTransport = new TNonblockingServerSocket(listeningPort);
    
    TNonblockingServer.Args serverArgs =
        new TNonblockingServer.Args(serverTransport);
    serverArgs.processor(processor).transportFactory(transportFactory)
        .protocolFactory(protocolFactory);
    tserver = new TNonblockingServer(serverArgs);
    
    connectionManager = new ConnectionManager(hosts, ports, this);
    LOG.info(listeningPort + ": Successfully initialized namespace" +
        " notifier client"); 
  }
  
  
  /**
   * Called by the ConnectionManager when the connection state changed.
   * The connection lock is hold when calling this method, so no
   * other methods from the ConnectionManager should be called here.
   * 
   * @param newState the new state
   * @return when the new state is DISCONNECTED_HIDDEN or DISCONNECTED_VISIBLE,
   *         then the return value is ignored. If the new state is CONNECTED,
   *         then the return value shows if the NamespaceNotifierClient accepts
   *         or not the new server. If it isn't accepted, the ConnectionManager
   *         will try connecting to another server. 
   */
  boolean connectionStateChanged(int newState) {
    switch (newState) {
      case ConnectionManager.CONNECTED:
        LOG.info(listeningPort + ": Switched to CONNECTED state.");
        // Try to resubscribe all the watched events
        try {
          return resubscribe();
        } catch (Exception e) {
          LOG.error(listeningPort + ": Resubscribing failed", e);
          return false;
        }
      case ConnectionManager.DISCONNECTED_VISIBLE:
        LOG.info(listeningPort + ": Switched to DISCONNECTED_VISIBLE state");
        for (NamespaceEventKey eventKey : watchedEvents.keySet())
          watchedEvents.put(eventKey, -1L);
        watcher.connectionFailed();
        break;
      case ConnectionManager.DISCONNECTED_HIDDEN:
        LOG.info(listeningPort + ": Switched to DISCONNECTED_HIDDEN state.");
    }

    return true;
  }
  
  
  long getCurrentConnectionToken() {
    return connectionManager.getConnectionToken();
  }
  
  
  /**
   * Adds the specified server to the pool of known servers.
   * @param host
   * @param port
   * @throws ServerAlreadyKnownException if the server is already in the pool
   *         of known servers.
   */
  public void addServer(String host, int port)
      throws ServerAlreadyKnownException {
    connectionManager.addServer(host, port);
  }
  
  
  /**
   * Removes the specified server from the pool of known servers.
   * @param host
   * @param port
   * @throws ServerNotKnownException if the server isn't in the pool of
   *         known servers.
   */
  public void removeServer(String host, int port)
      throws ServerNotKnownException {
    connectionManager.removeServer(host, port);
  }
  
  
  /**
   * Sets the value of the timeout after which we will consider a server
   * failed.
   * @param timeout the value in milliseconds for the timeout after which
   *        we will consider the server failed. Defaults to 50000 (50 seconds).
   */
  public void setServerTimeout(long timeout) {
    connectionManager.setServerTimeout(timeout);
  }
  
  
  /**
   * Sets the value of the time between consecutive connect retries. The
   * connection is retried only after Watcher.connectionFailed was called.
   * @param retryTime the retry time in milliseconds.
   */
  public void setConnectRetryTime(long retryTime) {
    connectionManager.setConnectRetryTime(retryTime);
  }
  
  
  /**
   * The watcher (given in the constructor) will be notified when an
   * event of the given type and at the given path will happen. He will
   * keep receiving notifications until the watch is removed with
   * {@link #removeWatch(String, EventType)}.
   * 
   * The subscription is considered done if the method doesn't throw an
   * exception (even if Watcher.connectionFailed is called before this
   * method returns).
   *
   * @param path the path where the watch is placed. For the FILE_ADDED event
   *        type, this represents the path of the directory under which the
   *        file will be created.
   * @param watchType the type of the event for which we want to receive the
   *        notifications.
   * @param transactionId the transaction id of the last received notification.
   *        Notifications will from and excluding the notification with this
   *        transaction id. If this is -1, then all notifications that
   *        happened after this method returns will be received and some
   *        of the notifications between the time the method was called
   *        and the time the method returns may be received.
   * @throws WatchAlreadyPlacedException if the watch already exists for this
   *         path and type.
   * @throws NotConnectedToServerException when the Watcher.connectionSuccessful
   *         method was not called (the connection to the server isn't
   *         established yet) at start-up or after a Watcher.connectionFailed
   *         call. The Watcher.connectionFailed could of happened anytime
   *         since the last Watcher.connectionSuccessful call until this
   *         method returns.
   * @throws TransactionIdTooOldException when the requested transaction id
   *         is too old and not loosing notifications can't be guaranteed.
   *         A solution would be a manual scanning and then calling the
   *         method again with -1 as the transactionId parameter.
   */
  public void placeWatch(String path, EventType watchType,
      long transactionId) throws TransactionIdTooOldException,
      NotConnectedToServerException, InterruptedException,
      WatchAlreadyPlacedException {
    NamespaceEventKey eventKey = new NamespaceEventKey(path, watchType);
    Object connectionLock = connectionManager.getConnectionLock();
    
    LOG.info(listeningPort + ": Placing watch: " +
        NotifierUtils.asString(eventKey) + " ...");
    if (watchedEvents.containsKey(eventKey)) {
      LOG.warn(listeningPort + ": Watch already exists at " +
          NotifierUtils.asString(eventKey));
      throw new WatchAlreadyPlacedException();
    }
    
    synchronized (connectionLock) {
      connectionManager.waitForTransparentConnect();
      
      if (!subscribe(path, watchType, transactionId)) {
        connectionManager.failConnection(true);
        connectionManager.waitForTransparentConnect();
        if (!subscribe(path, watchType, transactionId)) {
          // Since we are failing visible to the client, then there isn't
          // a need to request from a given txId
          watchedEvents.put(eventKey, -1L);
          connectionManager.failConnection(false);
          return;
        }
      }
      
      watchedEvents.put(eventKey, transactionId);
    }
  }


  /**
   * Removes a previously placed watch for a particular event type from the 
   * given path. If the watch is not actually present at that path before
   * calling the method, nothing will happen.
   * 
   * To remove the watch for all event types at this path, use
   * {@link #removeAllWatches(String)}.
   * 
   * @param path the path from which the watch is removed. For the FILE_ADDED event
   *        type, this represents the path of the directory under which the
   *        file will be created.
   * @param watchType the type of the event for which don't want to receive
   *        notifications from now on.
   * @return true if successfully removed watch. false if the watch wasn't
   *         placed before calling this method.
   * @throws WatchNotPlacedException if the watch wasn't placed before calling
   *         this method.
   * @throws NotConnectedToServerException when the Watcher.connectionSuccessful
   *         method was not called (the connection to the server isn't
   *         established yet) at start-up or after a Watcher.connectionFailed
   *         call. The Watcher.connectionFailed could of happened anytime
   *         since the last Watcher.connectionSuccessfull call until this
   *         method returns.
   */
  public void removeWatch(String path, EventType watchType)
      throws NotConnectedToServerException, InterruptedException,
      WatchNotPlacedException {
    NamespaceEvent event = new NamespaceEvent(path, watchType.getByteValue());
    NamespaceEventKey eventKey = new NamespaceEventKey(path, watchType);
    Object connectionLock = connectionManager.getConnectionLock();
    ServerHandler.Client server;
    
    LOG.info(listeningPort + ": removeWatch: Removing watch from " +
        NotifierUtils.asString(eventKey) + " ...");
    if (!watchedEvents.containsKey(eventKey)) {
      LOG.warn(listeningPort + ": removeWatch: watch doesen't exist at " +
          NotifierUtils.asString(eventKey) + " ...");
      throw new WatchNotPlacedException();
    }
    
    synchronized (connectionLock) {
      connectionManager.waitForTransparentConnect();
      server = connectionManager.getServer();
      
      try {
        server.unsubscribe(connectionManager.getId(), event);
      } catch (InvalidClientIdException e1) {
        LOG.warn(listeningPort + ": removeWatch: server deleted us", e1);
        connectionManager.failConnection(true);
      } catch (ClientNotSubscribedException e2) {
        LOG.error(listeningPort + ": removeWatch: event not subscribed", e2);
      } catch (TException e3) {
        LOG.error(listeningPort + ": removeWatch: failed communicating to" +
            " server", e3);
        connectionManager.failConnection(true);
      }

      watchedEvents.remove(eventKey);
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug(listeningPort + ": Unsubscribed from " +
          NotifierUtils.asString(eventKey));
    }
  }


  /**
   * Tests if a watch is placed at the given path and of the given type.
   * 
   * @param path the path where we should test if a watch is placed. For the
   *        FILE_ADDED event type, this represents the path of the directory
   *        under which the file will be created.
   * @param watchType the type of the event for which we test if a watch is
   *        present.
   * @return <code>true</code> if a watch is placed, <code>false</code>
   *         otherwise.
   */
  public boolean haveWatch(String path, EventType watchType) {
    return watchedEvents.containsKey(new NamespaceEventKey(path, watchType));
  }
  
  
  /**
   * @return true if notifications were received for all subscribed events,
   *         false otherwise.
   */
  boolean receivedNotificationsForAllEvents() {
    return !watchedEvents.values().contains(-1L);
  }
  
  
  /**
   * Called right after a reconnect to resubscribe to all events. Must be
   * called with the connection lock acquired.
   */
  private boolean resubscribe() throws TransactionIdTooOldException,
      InterruptedException {
    for (NamespaceEventKey eventKey : watchedEvents.keySet()) {
      NamespaceEvent event = eventKey.getEvent(); 
      if (!subscribe(event.getPath(), EventType.fromByteValue(event.getType()),
          watchedEvents.get(eventKey))) {
        return false;
      }
    }
    return true;
  }
  
  
  /**
   * Should be called with the connection lock acquired and only in the
   * <code>CONNECTED</code> state.
   * @param path
   * @param watchType
   * @param transactionId
   * @return true if connected, false otherwise. 
   * @throws TransactionIdTooOldException
   */
  private boolean subscribe(String path, EventType watchType,
      long transactionId)
          throws TransactionIdTooOldException, InterruptedException {
    ServerHandler.Client server = connectionManager.getServer();
    NamespaceEvent event =  new NamespaceEvent(path, watchType.getByteValue());
    
    if (LOG.isDebugEnabled()) {
      LOG.debug(listeningPort + ": subscribe: Trying to subscribe for " +
          NotifierUtils.asString(event) + " ... from txId " + transactionId);
    }
    
    for (int retries = 0; retries < 3; retries ++) {
      try {
        server.subscribe(connectionManager.getId(), event, transactionId);
        if (LOG.isDebugEnabled()) {
          LOG.debug(listeningPort + ": subscribe: successful");
        }
        return true;
      } catch (TransactionIdTooOldException e) {
        LOG.warn(listeningPort + ": Failed to subscribe [1]", e);
        throw e;
      } catch (InvalidClientIdException e) {
        LOG.warn(listeningPort + ": Failed to subscribe [2]", e);
      } catch (TException e) {
        LOG.warn(listeningPort + ": Failed to subscribe [3]", e);
        Thread.sleep(1000);
      }
    }
    
    return false;
  }


  @Override
  public void run() {
    LOG.info(listeningPort + ": Running ...");
    new Thread(connectionManager).start();
    LOG.info(listeningPort + ": Starting thrift server on port " + listeningPort);
    tserver.serve();
  }
  
  
  public void shutdown() {
    shouldShutdown = true;
    ServerHandler.Client server = connectionManager.getServer();
    connectionManager.shutdown();
    
    try {
      server.unregisterClient(connectionManager.getId());
    } catch (InvalidClientIdException e1) {
      LOG.warn(listeningPort + ": Server deleted us before shutdown", e1);
    } catch (TException e2) {
      LOG.warn(listeningPort + ": Failed to unregister client gracefully", e2);
    }
    
    tserver.stop();
  }

  
  /**
   * Raised when the client tries server related operations, but the
   * Watcher.connectionSuccessful method was not called.
   */
  static public class NotConnectedToServerException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public NotConnectedToServerException(String arg) {
      super(arg);
    }
    
    public NotConnectedToServerException() {
      super();
    }
  }
  
  
  /**
   * Called when the placeWatch method is called, but the watch is already
   * present.
   */
  static public class WatchAlreadyPlacedException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public WatchAlreadyPlacedException(String arg) {
      super(arg);
    }
    
    public WatchAlreadyPlacedException() {
      super();
    }
  }
  
  
  /**
   * Called when the removeWatch method is called, but the watch wasn't placed.
   */
  static public class WatchNotPlacedException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public WatchNotPlacedException(String arg) {
      super(arg);
    }
    
    public WatchNotPlacedException() {
      super();
    }
  }
  
  
  /**
   * Called when the addServer method tries to add a server which is already
   * stored in the internal data structures.
   */
  static public class ServerAlreadyKnownException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public ServerAlreadyKnownException(String arg) {
      super(arg);
    }
    
    public ServerAlreadyKnownException() {
      super();
    }
  }
  
  
  /**
   * Called when the removeServer method tries to remove a server which is not
   * stored in the internal data structures.
   */
  static public class ServerNotKnownException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public ServerNotKnownException(String arg) {
      super(arg);
    }
    
    public ServerNotKnownException() {
      super();
    }
  }
}

class ConnectionManager implements Runnable {
  public static final Log LOG = LogFactory.getLog(NamespaceNotifierClient.class);

  static final int SOCKET_TIMEOUT = 35000;
  static final long DEFAULT_SERVER_TIMEOUT = 50000;
  static final int DEFAULT_CONNECT_RETRY_TIME = 1000;
  
  public static final int CONNECTED = 0;
  public static final int DISCONNECTED_HIDDEN = 1;
  public static final int DISCONNECTED_VISIBLE = 2;
  
  private int state = DISCONNECTED_VISIBLE;
  
  int listeningPort;
  
  // This lock is hold when doing operations that may modify the 
  // connection state
  private Object connectionLock = new Object();
  
  // Used to wait and notify of when we should start retrying the connection
  // with the server.
  private Object retryConnectionCondition = new Object();
  
  // Connection to notification servers information
  private List<Map.Entry<String, Integer>> servers;
  
  // The server thrift object (if we are connected to a server)
  private ServerHandler.Client server = null;
  
  private long connectionToken;
  
  // The id of the server we are currently connected to
  volatile String serverId;
  
  // The id currently assigned to us by the server we are connected to.
  volatile long id;

  private volatile long serverTimeout = DEFAULT_SERVER_TIMEOUT;
  
  private volatile int connectRetryTime = DEFAULT_CONNECT_RETRY_TIME;

  ServerTracker tracker;

  NamespaceNotifierClient notifierClient;

  
  public ConnectionManager(List<String> hosts, List<Integer> ports,
      NamespaceNotifierClient notifierClient) {
    serverId = null;
    id = -1;
    tracker = new ServerTracker();
    this.notifierClient = notifierClient;
    this.listeningPort = notifierClient.listeningPort;
    
    servers = new ArrayList<Map.Entry<String,Integer>>();
    for (int i = 0; i < hosts.size(); i ++) {
      String host = hosts.get(i);
      int port = ports.get(ports.size() == 0 ? 0 : i);
      servers.add(Maps.immutableEntry(host, port));
    }
    
    // So clients try have different priority for servers, avoiding
    // all the clients connecting to one server.
    Collections.shuffle(servers, notifierClient.generator);
  }
  

  private int getServerPosition(String host, int port) {
    for (int i = 0; i < servers.size(); i ++) {
      Map.Entry<String, Integer> serverEntry = servers.get(i);
      if (serverEntry.getKey().equals(host) && serverEntry.getValue() == port) {
        return i;
      }
    }
    return -1;
  }
  
  
  void addServer(String host, int port) 
      throws ServerAlreadyKnownException {
    if (getServerPosition(host, port) != -1) {
      throw new ServerAlreadyKnownException("Already got " + host + ":" +
          port);
    }
    
    // Put in a random position to ensure load balancing across servers
    int position = notifierClient.generator.nextInt(servers.size() + 1);
    servers.add(position, Maps.immutableEntry(host, port));
  }
  
  
  void removeServer(String host, int port)
      throws ServerNotKnownException {
    int position = getServerPosition(host, port);
    if (position == -1) {
      throw new ServerNotKnownException("Unknown host " + host + ":" + port);
    }
    servers.remove(position);
  }
  
  
  void setServerTimeout(long timeout) {
    serverTimeout = timeout;
  }
  
  
  void setConnectRetryTime(long retryTime) {
    setConnectRetryTime(retryTime);
  }
  
  
  long getId() {
    return id;
  }
  
  
  String getServerId() {
    return serverId;
  }
  
  
  ServerHandler.Client getServer() {
    return server;
  }
  
  
  /**
   * Gets the current connection state.
   * @param connectionLockHold if the connection lock returned by
   *        getConnectionLock is being hold at the moment within
   *        a synchronized block.
   * @return the current state (CONNECTED, DISCONNECTED_HIDDEN or
   *         DISCONNECTED_VISIBLE).
   */
  int getConnectionState(boolean connectionLockHold) {
    if (connectionLockHold) {
      return state;
    }
    synchronized (connectionLock) {
      return state;
    }
  }
  
  
  /**
   * @return The most recently generated connection token.
   */
  long getConnectionToken() {
    return connectionToken;
  }
  
  
  /**
   * @return An object that is being hold with synchronized() when doing
   *         operations that may change the connection state. Holding
   *         this object thus guarantees that no connection state changes
   *         will occur while doing so.
   */
  Object getConnectionLock() {
    return connectionLock;
  }
  
  
  /**
   * Must be called holding the connection lock returned by getConnectionLock.
   * It waits until the current connection state is CONNECTED. If it ever
   * gets to DISCONNECTED_VISIBLE it will raise an exception. If the current
   * state is CONNECTED, then it will return without waiting.
   * 
   * @throws InterruptedException
   * @throws NotConnectedToServerException when we got into a
   *         DISCONNECTED_VISIBLE state.
   */
  void waitForTransparentConnect() throws InterruptedException,
      NotConnectedToServerException {
    if (state == DISCONNECTED_VISIBLE) {
      LOG.warn(listeningPort + ": waitForTransparentConnect: got visible" +
          " disconnected state");
      throw new NotConnectedToServerException();
    }
    
    // Wait until we are not hidden disconnected
    while (state != CONNECTED) {
      connectionLock.wait();
      switch (state) {
        case CONNECTED:
          break;
        case DISCONNECTED_HIDDEN:
          continue;
        case DISCONNECTED_VISIBLE:
          LOG.warn(listeningPort + ": waitForTransparentConnect: got visible" +
              " disconnected state");
          throw new NotConnectedToServerException();
      }
    }
  }
  
  
  private boolean connect() {
    LOG.info(listeningPort + ": Connecting ...");
    
    synchronized (connectionLock) {
      // Ensure there are no potential lost messages.
      if (state == DISCONNECTED_HIDDEN && 
          !notifierClient.receivedNotificationsForAllEvents()) {
        LOG.info(listeningPort + ": Didn't received notifications for" +
            " all events");
        failConnection(false);
        return false;
      } else {
        LOG.info(listeningPort + ": Received notifications for all events.");
      }
      
      for (Map.Entry<String, Integer> serverAddr : servers) {
        String host = serverAddr.getKey();
        int port = serverAddr.getValue();
        LOG.info(listeningPort + ": Trying to connect to " + host + ":" +
            port);
        
        ServerHandler.Client serverObj;
        try {
          serverObj = getServerConnection(host, port);
        } catch (Exception e) {
          LOG.error(listeningPort + ": Failed to connect to server at " +
              host + ":" + port, e);
          continue;
        }
        
        // The server must answer with this token
        connectionToken = notifierClient.generator.nextLong();
        LOG.info(listeningPort + ": Generated token: " + connectionToken);
          
        try {
          // Before this function returns, the server should make the
          // registerServer call which if he answers with the correct token,
          // it will set the serverId to his.
          LOG.info(listeningPort + ": calling registerClient");
          serverObj.registerClient(notifierClient.listeningHost,
              notifierClient.listeningPort, connectionToken);
          LOG.info(listeningPort + ": registerClient call successful");
        } catch (RampUpException e1) {
          LOG.info(listeningPort + ": Server " + host + ":" + port +
              " in ramp up phase");
          continue;
        } catch (ClientConnectionException e2) {
          LOG.error(listeningPort + ": The server failed to connect to us", e2);
          continue;
        } catch (Exception e) {
          LOG.error(listeningPort + ": Server " + host + ":" + port +
              " communication failure", e);
          continue;
        }
        
        if (serverId == null || serverId.isEmpty()) {
          LOG.info(listeningPort + ": The server answered with a bad token." +
              " trying next server ...");
          continue;
        }
        LOG.info(listeningPort + ": The server answered with correct token.");
        server = serverObj;
        
        state = CONNECTED;
        if (!notifierClient.connectionStateChanged(state)) {
          try {
            server.unregisterClient(id);
          } catch (Exception e) {}
          failConnection(false);
          return false;
        }
        tracker.messageReceived();
        LOG.info(listeningPort + ": Connection status: SUCCESS");
        return true;
      }
      
    }
    
    LOG.info(listeningPort + ": Connection status: FAILED");
    return false;
  }


  void failConnection(boolean hiddenToClient) {
    LOG.info(listeningPort + ": Failing connection. Hidden to client=" +
        hiddenToClient);
    serverId = null;
    id = -1;
    server = null;
    if (hiddenToClient) {
      state = DISCONNECTED_HIDDEN;
    } else {
      state = DISCONNECTED_VISIBLE;
    }
    notifierClient.connectionStateChanged(state);

    synchronized (retryConnectionCondition) {
      retryConnectionCondition.notify();
    }
  }  


  @Override
  public void run() {
    // Initial connect
    forceConnect();
    new Thread(tracker).start();
    
    // Retry on failure
    while (!notifierClient.shouldShutdown) {
      synchronized (retryConnectionCondition) {
        try {
          retryConnectionCondition.wait();
        } catch (InterruptedException e) {
          if (notifierClient.shouldShutdown) {
            break;
          }
          continue;
        }
      }
      
      if (getConnectionState(false) == DISCONNECTED_VISIBLE) {
        notifierClient.watcher.connectionFailed();
      }
      
      if (notifierClient.shouldShutdown) {
        break;
      }
      
      forceConnect();
    }
  }
  
  
  void shutdown() {
    synchronized (retryConnectionCondition) {
      retryConnectionCondition.notify();
    }
  }
  
  
  private void forceConnect() {
    LOG.info(listeningPort + ": ConnectionChecker forcing connect ...");
    while (true) {
      LOG.info(listeningPort + ": forceConnect loop start ...");
      try {
        Thread.sleep(connectRetryTime);
      } catch (InterruptedException e) {}
      
      LOG.info(listeningPort + ": forceConnect trying connect ...");
      int prevState = getConnectionState(false);
      if (connect()) {
        if (prevState == DISCONNECTED_VISIBLE) {
          notifierClient.watcher.connectionSuccesful();
        }
        break;
      }
      LOG.info(listeningPort + ": forceConnect done trying connect");
    }
    LOG.info(listeningPort + ": forceConnect done");
  }
  
  
  private ServerHandler.Client getServerConnection(String host, int port) 
      throws TTransportException, IOException {
    TTransport transport;
    TProtocol protocol;
    ServerHandler.Client serverObj;
    
    transport = new TFramedTransport(new TSocket(host, port, SOCKET_TIMEOUT));
    protocol = new TBinaryProtocol(transport);
    serverObj = new ServerHandler.Client(protocol);
    transport.open();
    
    return serverObj;
  }
  
  
  class ServerTracker implements Runnable {
    volatile long lastReceivedTimestamp = -1;
    
    /**
     * Should be called when a message was received from the server.
     */
    public void messageReceived() {
      lastReceivedTimestamp = System.currentTimeMillis();
    }
    
    @Override
    public void run() {
      lastReceivedTimestamp = System.currentTimeMillis();
      while (!notifierClient.shouldShutdown) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {}
        
        synchronized (connectionLock) {
          if (state != CONNECTED) {
            continue;
          }
          
          if (System.currentTimeMillis() > lastReceivedTimestamp + serverTimeout) {
            LOG.info(listeningPort + ": ServerTracker: Server timeout." +
                " Failing connection ...");
            failConnection(true);
          }
        }
      }
    }
  }
}