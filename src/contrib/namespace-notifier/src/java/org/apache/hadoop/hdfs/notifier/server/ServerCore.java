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
package org.apache.hadoop.hdfs.notifier.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.notifier.ClientHandler;
import org.apache.hadoop.hdfs.notifier.ClientNotSubscribedException;
import org.apache.hadoop.hdfs.notifier.EventType;
import org.apache.hadoop.hdfs.notifier.InvalidClientIdException;
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.NamespaceEventKey;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.NotifierUtils;
import org.apache.hadoop.hdfs.notifier.ServerHandler;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;
import org.apache.hadoop.hdfs.notifier.server.metrics.NamespaceNotifierMetrics;
import org.apache.hadoop.util.Daemon;
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

public class ServerCore implements IServerCore {
  public static final Log LOG = LogFactory.getLog(ServerCore.class);
   
  public static final String DISPATCHER_COUNT = "notifier.dispatcher.count";
  public static final String LISTENING_PORT = "notifier.thrift.port";
  public static final String SERVER_ID = "notifier.server.id";
  
  // The timeout after which the core will stop trying to do a graceful
  // shutdown and will interrupt the threads
  private static final int SHUTDOWN_TIMEOUT = 5000;
  
  private static final int SOCKET_READ_TIMEOUT = 25000;
  
  // The number of dispatcher threads
  private int dispatcherCount;

  // The port on which the Thrift ServerHandler service is listening
  private int listeningPort;

  // For each event, we retain the list of clients which are subscribed
  // for that event. If changes in subscriptions are done, a
  // synchronized block should be used.
  private Map<NamespaceEventKey, Set<Long>> subscriptions;
  
  // Data structures for each registered client
  private ConcurrentMap<Long, ClientData> clientsData;
  
  // Used to generate the client id's
  private Random clientIdsGenerator;
  
  // Stores the notifications over an configurable amount of time
  private IServerHistory serverHistory;

  // The dispatchers which actually send the messages. The dispatchers also
  // save the notifications in the history
  private List<IServerDispatcher> dispatchers;
  
  // The handler for our Thrift service
  private ServerHandler.Iface handler;
  
  // The reader of the edit log
  private IServerLogReader logReader;
  
  // The server for our Thrift service
  private TServer tserver;
  
  // Hadoop configuration
  private Configuration conf;
  
  private long serverId;
  
  private volatile boolean shouldShutdown = false;
  
  AtomicLong numTotalSubscriptions = new AtomicLong(0);
  
  public NamespaceNotifierMetrics metrics;
  
  // A list with all the threads the server is running
  List<Thread> threads = new ArrayList<Thread>();
  

  public ServerCore(Configuration configuration) throws ConfigurationException {
    conf = configuration;
    initDataStructures();
  }
  
  public ServerCore() throws ConfigurationException {
    conf = initConfiguration();
    initDataStructures();
  }
  
  
  private void initDataStructures() {
    clientsData = new ConcurrentHashMap<Long, ServerCore.ClientData>();
    subscriptions = new HashMap<NamespaceEventKey, Set<Long>>();
    clientIdsGenerator = new Random();
    
    metrics = new NamespaceNotifierMetrics(conf, serverId);
  }
  
  
  @Override
  public void init(IServerLogReader logReader, IServerHistory serverHistory,
      List<IServerDispatcher> dispatchers, ServerHandler.Iface handler) {
    this.serverHistory = serverHistory;
    this.logReader = logReader;
    this.dispatchers = dispatchers;
    this.handler = handler;
  }
  
  
  @Override
  public void run() {
    LOG.info("Starting server ...");
    LOG.info("Max heap size: " + Runtime.getRuntime().maxMemory());
    
    // Setup the Thrift server
    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TTransportFactory transportFactory = new TFramedTransport.Factory();
    TNonblockingServerTransport serverTransport;
    ServerHandler.Processor<ServerHandler.Iface> processor =
        new ServerHandler.Processor<ServerHandler.Iface>(handler);
    try {
      serverTransport = new TNonblockingServerSocket(listeningPort);
    } catch (TTransportException e) {
      LOG.error("Failed to setup the Thrift server.", e);
      return;
    }
    TNonblockingServer.Args serverArgs =
        new TNonblockingServer.Args(serverTransport);
    serverArgs.processor(processor).transportFactory(transportFactory)
        .protocolFactory(protocolFactory);
    tserver = new TNonblockingServer(serverArgs);
    
    // Start the worker threads
    threads.add(new Thread(serverHistory));
    for (IServerDispatcher dispatcher : dispatchers) {
      threads.add(new Thread(dispatcher));
    }
    threads.add(new Thread(logReader));
    threads.add(new Thread(new ThriftServerRunnable()));
    LOG.info("Starting thrift server on port " + listeningPort);
    for (Thread t : threads) {
      t.start();
    }

    try {
      while (!shutdownPending()) {
        // Read a notification
        NamespaceNotification notification =
            logReader.getNamespaceNotification();
        if (notification != null) {
          handleNotification(notification);
          continue;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed fetching transaction log data", e);
    } finally {
      shutdown();
    }
    
    long shuttingDownStart = System.currentTimeMillis();
    for (Thread t : threads) {
      long remaining = SHUTDOWN_TIMEOUT + System.currentTimeMillis() -
          shuttingDownStart;
      try {
        if (remaining > 0) {
          t.join(remaining);
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted when closing threads at the end");
      }
      if (t.isAlive()) {
        t.interrupt();
      }
    }
    LOG.info("Shutdown");
  }


  /**
   * Called when the Namespace Notifier server should shutdown.
   */
  @Override
  public void shutdown() {
    LOG.info("Shutting down ...");
    shouldShutdown = true;
    tserver.stop();
  }
  
  
  @Override
  public boolean shutdownPending() {
    return shouldShutdown;
  }

  
  private Configuration initConfiguration() throws ConfigurationException {
    Configuration.addDefaultResource("namespace-notifier-server-default.xml");
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration conf = new Configuration();
    conf.addResource("namespace-notifier-server-site.xml");
    conf.addResource("hdfs-site.xml");
    
    dispatcherCount = conf.getInt(DISPATCHER_COUNT, -1);
    listeningPort = conf.getInt(LISTENING_PORT, -1);
    serverId = conf.getLong(SERVER_ID, -1);
    LOG.info(dispatcherCount + " " + listeningPort + " " + serverId);

    if (dispatcherCount == -1) {
      throw new ConfigurationException("Invalid or missing dispatcherCount: " +
          dispatcherCount);
    }
    if (listeningPort == -1) {
      throw new ConfigurationException("Invalid or missing listeningPort: " +
          listeningPort);
    }
    if (serverId == -1) {
      throw new ConfigurationException("Invalid or missing serverId: " +
          serverId);
    }
    return conf;
  }


  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  
  /**
   * Adds the client to the internal data structures and connects to it.
   * If the method throws an exception, then it is guaranteed it will also
   * be removed from the internal structures before throwing the exception.
   * 
   * @param host the host on which the client is running
   * @param port the port on which the client is running the Thrift service
   * @return the client's id.
   * @throws TTransportException when something went wrong when connecting to
   *                             the client. 
   */
  @Override
  public long addClientAndConnect(String host, int port)
      throws TTransportException, IOException {
    long clientId = getNewClientId();
    LOG.info("Adding client with id=" + clientId + " host=" + host +
        " port=" + port + " and connecting ...");
    
    ClientHandler.Client clientObj;
    try {
      clientObj = getClientConnection(host, port);
      LOG.info("Succesfully connected to client " + clientId);
    } catch (IOException e1) {
      LOG.error("Failed to connect to client " + clientId, e1);
      throw e1;
    } catch (TTransportException e2) {
      LOG.error("Failed to connect to client " + clientId, e2);
      throw e2;
    }
    
    // Save the client to the internal structures
    addClient(clientId, clientObj);
    LOG.info("Successfully added client " + clientId + " and connected."); 
    return clientId;
  }
  
  
  /**
   * Used to handle a generated notification:
   * - sending the notifications to the clients which subscribed to the
   *   associated event.
   * - saving the notification in the history.
   * @param n
   */
  @Override
  public void handleNotification(NamespaceNotification n) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling " + NotifierUtils.asString(n) + " ...");
    }

    // Add the notification to the queues
    Set<Long> clientsForNotification = getClientsForNotification(n);
    if (clientsForNotification != null) {
      synchronized (clientsForNotification) {
        for (Long clientId : clientsForNotification) {
          ConcurrentLinkedQueue<NamespaceNotification> clientQueue =
              clientsData.get(clientId).queue;
          // Just test that the client wasn't removed meanwhile
          if (clientQueue == null) {
            continue;
          }
          clientQueue.add(n);
        }
      }
      
      ServerDispatcher.queuedNotificationsCount.addAndGet(
          clientsForNotification.size());
    }
    
    // Save it in history
    serverHistory.storeNotification(n);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Done handling " + NotifierUtils.asString(n));
    }
  }
  
  
  /**
   * Adds the client to the internal structures
   * @param clientId the client id for this client.
   * @param clientObj the Thrift client object for this client
   */
  @Override
  public void addClient(long clientId, ClientHandler.Iface clientObj) {
    LOG.info("Adding client " + clientId + " ...");
    ClientData clientData;
    
    clientData = new ClientData(clientId, clientObj);
    clientsData.put(clientId, clientData);
    
    // Assign it to a dispatcher
    IServerDispatcher dispatcher =
        dispatchers.get(getDispatcherIdForClient(clientId));
    dispatcher.assignClient(clientId);
    LOG.info("Succesfully added client.");
    metrics.numRegisteredClients.set(clientsData.size());
  }
  
  
  /**
   * Removes a client from the internal data structures. This also removes
   * the client from all the events to which he subscribed.
   * 
   * @param clientId the client's id for the client we want to remove
   * @return true if the client was present in the internal data structures,
   *         false otherwise.
   */
  @Override
  public boolean removeClient(long clientId) {
    ClientData clientData = clientsData.get(clientId);
    if (clientData == null) {
      return false;
    }
    
    LOG.info("Removing client " + clientId + " ...");
    
    IServerDispatcher dispatcher =
        dispatchers.get(getDispatcherIdForClient(clientId));
    dispatcher.removeClient(clientId);
    
    // Iterate over all the sets in which this client figures as subscribed
    // and remove it
    synchronized (subscriptions) {
      for (Set<Long> subscribedSet : clientData.subscriptions) {
        synchronized (subscribedSet) {
          subscribedSet.remove(clientId);
        }
      }
    }

    metrics.numTotalSubscriptions.set(numTotalSubscriptions.
        getAndAdd(-clientData.subscriptions.size()));
    
    clientsData.remove(clientId);
    LOG.info("Client " + clientId + " removed");
    metrics.numRegisteredClients.set(clientsData.size());
    return true;
  }
  
  
  /**
   * Checks if the client with the given id is registered.
   * @param clientId the id of the client
   * @return true if registered, false otherwise.
   */
  @Override
  public boolean isRegistered(long clientId) {
    return clientsData.containsKey(clientId);
  }
  
  
  /**
   * Gets the ClientHandler.Client object for the given client id.
   * 
   * @param clientId
   * @return the ClientHandler.Client object or null if the clientId
   *         is invalid.
   */
  @Override
  public ClientHandler.Iface getClient(long clientId) {
    ClientData clientData = clientsData.get(clientId);
    return (clientData == null) ? null : clientData.handler;
  }
  
  
  /**
   * Used to get the set of clients for which a notification should be sent.
   * While iterating over this set, you should use synchronized() on it to
   * avoid data inconsistency (or ordering problems).
   * 
   * @param n the notification for which we want to get the set of clients
   * @return the set of clients or null if there are no clients subscribed
   *         for this notification
   */
  @Override
  public Set<Long> getClientsForNotification(NamespaceNotification n) {
    String eventPath = NotifierUtils.getBasePath(n);
    if (LOG.isDebugEnabled()) {
      LOG.debug("getClientsForNotification called for " +
          NotifierUtils.asString(n) + ". Searching at path " + eventPath);
    }
    synchronized (subscriptions) {
      return subscriptions.get(new NamespaceEventKey(eventPath, n.type));
    }
  }
  
  
  /**
   * @return the set of clients id's for all the clients that are currently
   * subscribed to us. Warning: this set should not be modified directly.
   */
  @Override
  public Set<Long> getClients() {
    return clientsData.keySet();
  }
  
  
  @Override
  public void subscribeClient(long clientId, NamespaceEvent event, long txId) 
      throws TransactionIdTooOldException, InvalidClientIdException {
    NamespaceEventKey eventKey = new NamespaceEventKey(event);
    Set<Long> clientsForEvent;
    ClientData clientData = clientsData.get(clientId);
    
    if (clientData == null) {
      LOG.warn("subscribe client called with invalid id " + clientId);
      throw new InvalidClientIdException();
    }
    
    LOG.info("Subscribing client " + clientId + " to " +
        NotifierUtils.asString(event) + " from txId " + txId);

    synchronized (subscriptions) {
      clientsForEvent = subscriptions.get(eventKey);
      if (clientsForEvent == null) {
        clientsForEvent = new HashSet<Long>();
        subscriptions.put(eventKey, clientsForEvent);
      }
      synchronized (clientsForEvent) {
        clientData.subscriptions.add(clientsForEvent);
      }
    }
    
    // It is needed to lock this set while queue'ing the notifications, or we
    // may get ordering problems. This is out of the
    // synchronized(subscriptions) block because it will block all the
    // subscriptions and may induce serious latency (the queueNotifications
    // operations can take a lot of time).
    synchronized (clientsForEvent) {
      queueNotifications(clientId, event, txId);
      clientsForEvent.add(clientId);
    }
    LOG.info(clientId + " subscribed to " + NotifierUtils.asString(event) +
        " from txId " + txId);
    
    metrics.numTotalSubscriptions.set(numTotalSubscriptions.incrementAndGet());
  }

  
  @Override
  public void unsubscribeClient(long clientId, NamespaceEvent event) 
      throws ClientNotSubscribedException, InvalidClientIdException {
    NamespaceEventKey eventKey = new NamespaceEventKey(event);
    Set<Long> clientsForEvent;
    ClientData clientData = clientsData.get(clientId);
    
    if (clientData == null) {
      LOG.warn("subscribe client called with invalid id " + clientId);
      throw new InvalidClientIdException();
    }
    
    LOG.info("Unsubscribing client " + clientId + " from " +
        NotifierUtils.asString(event));
    
    synchronized (subscriptions) {
      clientsForEvent = subscriptions.get(eventKey);
      if (clientsForEvent == null) {
        throw new ClientNotSubscribedException();
      }

      synchronized (clientsForEvent) {
        if (!clientsForEvent.contains(clientId)) {
          throw new ClientNotSubscribedException();
        }
        clientsForEvent.remove(clientId);
        clientData.subscriptions.remove(clientsForEvent);
        
        if (clientsForEvent.size() == 0) {
          subscriptions.remove(eventKey);
        }
      }
    }
    LOG.info("Client " + clientId + " unsubsribed from " +
        NotifierUtils.asString(event));
    metrics.numTotalSubscriptions.set(numTotalSubscriptions.decrementAndGet());
  }
  
  
  /**
   * @return the id of this server
   */
  @Override
  public long getId() {
    return serverId;
  }


  /**
   * Gets the queue of notifications that should be sent to a client. It is
   * important to send the notifications in this queue first before sending
   * any other notification, or the order will be affected.
   * 
   * @param clientId the id of the client for which we want to get the queued
   *                 notifications.
   * @return the queue with the notifications for this client. The returned
   *         queue is synchronized and no other synchronization mechanisms are
   *         required. null if the client is not registered.
   */
  @Override
  public Queue<NamespaceNotification> getClientNotificationQueue(long clientId) {
    ClientData clientData = clientsData.get(clientId);
    return (clientData == null) ? null : clientData.queue;
  }
  
  
  @Override
  public IServerHistory getHistory() {
    return serverHistory;
  }
  
  
  /**
   * Returns the id of the dispatcher for a given client.
   * 
   * @param clientId the id of the client
   * @return the id of the dispatcher assigned for this client.
   */
  @Override
  public int getDispatcherIdForClient(long clientId) {
    return (int)(Math.abs(clientId) % dispatchers.size());
  }
  
  
  @Override
  public int getDispatcherCount() {
    return dispatcherCount;
  }

  
  /**
   * Queues the notification for a client. The queued notifications will be sent
   * asynchronously after this method returns to the specified client.
   * 
   * The queued notifications will be notifications for the given event and
   * their associated transaction id is greater then the given transaction
   * id (exclusive).
   * 
   * @param clientId the client to which the notifications should be sent
   * @param event the subscribed event
   * @param txId the transaction id from which we should send the
   *             notifications (exclusive). If this is -1, then
   *             nothing will be queued for this client.
   * @throws TransactionIdTooOldException when the history has no records for
   *         the given transaction id.
   * @throws InvalidClientIdException when the client isn't registered
   */
  private void queueNotifications(long clientId, NamespaceEvent event, long txId)
      throws TransactionIdTooOldException, InvalidClientIdException {
    if (txId == -1) {
      return;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Queueing notifications for client " + clientId + " from txId " +
          txId + " at [" + event.path + ", " +
          EventType.fromByteValue(event.type) + "] ...");
    }
    
    ClientData clientData = clientsData.get(clientId);
    if (clientData == null) {
      LOG.error("Missing the client data for client id: " + clientId);
      throw new InvalidClientIdException("Missing the client data");
    }
    
    // Store the notifications in the queue for this client
    serverHistory.addNotificationsToQueue(event, txId, clientData.queue);
  }


  /**
   * Generates a new client id which is not present in the current set of ids
   * for the clients which are subscribed to this server.
   * 
   * @return A newly generated client id
   */
  private long getNewClientId() {
    while (true) {
      long clientId = Math.abs(clientIdsGenerator.nextLong());
      if (!clientsData.containsKey(clientId)) {
        return clientId;
      }
    }
  }
  
  
  private ClientHandler.Client getClientConnection(String host, int port)
      throws TTransportException, IOException {
    TTransport transport;
    TProtocol protocol;
    ClientHandler.Client clientObj;
    
    // TODO - make it user configurable
    transport = new TFramedTransport(new TSocket(host, port, SOCKET_READ_TIMEOUT));    
    protocol = new TBinaryProtocol(transport);
    clientObj = new ClientHandler.Client(protocol);
    transport.open();
    
    return clientObj;
  }
  
  
  @Override
  public NamespaceNotifierMetrics getMetrics() {
    return metrics;
  }


  /**
   * Returns the lock that must be hold when calling the client's
   * Thrift methods.
   * @param clientId the id of the client for which we want to get the
   *        communication lock.
   * @return the Lock object, or null if no such client.
   */
  @Override
  public Lock getClientCommunicationLock(long clientId) {
    ClientData clientData = clientsData.get(clientId);
    if (clientData == null)
      return null;
    return clientData.communicationLock;
  }
  
  
  class ClientData {
    // The asssigned id for this client
    final long id;
    
    // The thrift handler for this client
    final ClientHandler.Iface handler;
    
    // To ensure that only one thread calls a client's Thrift method at a 
    // given moment.
    Lock communicationLock = new ReentrantLock();
    
    // The list with all the subscription sets in which this client appears
    // for easier removal
    List<Set<Long>> subscriptions = new LinkedList<Set<Long>>();
    
    // The queue with notifications for this client
    ConcurrentLinkedQueue<NamespaceNotification> queue =
        new ConcurrentLinkedQueue<NamespaceNotification>();
    
    ClientData(long id, ClientHandler.Iface handler) {
      this.id = id;
      this.handler = handler;
    }
  }
  
  
  class ThriftServerRunnable implements Runnable {

    @Override
    public void run() {
      try {
        tserver.serve();
      } catch (Exception e) {
        LOG.error("Thrift server failed", e);
      } finally {
        shutdown();
      }
    }
    
  }
  

  public static void main(String[] args) {
    List<IServerDispatcher> dispatchers;
    IServerLogReader logReader;
    IServerHistory serverHistory;
    IServerCore core;
    ServerHandler.Iface handler;
    Daemon coreDaemon = null;
    
    try {
      core = new ServerCore();
      
      serverHistory = new ServerHistory(core, false); // TODO - enable ramp-up
      logReader = new ServerLogReader(core);
      dispatchers = new ArrayList<IServerDispatcher>();
      for (int i = 0; i < core.getDispatcherCount(); i ++) {
        dispatchers.add(new ServerDispatcher(core, i));
      }
      handler = new ServerHandlerImpl(core);
      
      core.init(logReader, serverHistory, dispatchers, handler);
      
      coreDaemon = new Daemon(core);
      coreDaemon.start();
      coreDaemon.join();
      
    } catch (ConfigurationException e) {
      e.printStackTrace();
      System.err.println("Invalid configurations.");
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Failed reading the transaction log");
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.err.println("Core interrupted");
    }
  }
}