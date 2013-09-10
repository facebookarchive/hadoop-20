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

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.ShortVoid;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.FlushableLogger;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.ipc.RPC.Invocation;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.security.authorize.AuthorizationException;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 * @see Client
 */
public abstract class Server {

  /**
   * The first four bytes of Hadoop RPC connections
   */
  public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());
  
  // 1 : Introduce ping and server does not throw away RPCs
  // 3 : Introduce the protocol into the RPC connection header
  public static final byte CURRENT_VERSION = 3;
  
  /**
   * How many calls per handler are allowed in the queue.
   */
  private static final String IPC_SERVER_HANDLER_QUEUE_SIZE_KEY = 
                                       "ipc.server.handler.queue.size";
  /**
   * How many calls/handler are allowed in the queue.
   */
  private static final int MAX_QUEUE_SIZE_PER_HANDLER = 100;

  /**
   * Initial and max size of response buffer
   */
  static int INITIAL_RESP_BUF_SIZE = 10240;
  static int MAX_RESP_BUF_SIZE = 1024*1024;
  int PURGE_INTERVAL = 900000; // 15mins

  long cleanupInterval = 10000; //the minimum interval between
                                        //two cleanup runs

  public static final String  IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY =
                                       "ipc.server.max.response.size";
  public static final int IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT =
                                       1024 * 1024;
  public static final String IPC_SERVER_RPC_READ_THREADS_KEY =
                                        "ipc.server.read.threadpool.size";
  public static final String IPC_SERVER_CLIENT_IDLETHRESHOLD =
                                        "ipc.client.idlethreshold";
  public static final String IPC_SERVER_CLIENT_CONN_MAXIDLETIME =
                                        "ipc.client.connection.maxidletime";
  public static final int IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1;
  
  public static final Log LOG = LogFactory.getLog(Server.class);
  // immediate flush logger
  private static final Log FLOG = FlushableLogger.getLogger(LOG);

  private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

  private static final Map<String, Class<?>> PROTOCOL_CACHE = 
    new ConcurrentHashMap<String, Class<?>>();
  
  private static final Map<Long, Call> delayedCalls = 
    new ConcurrentHashMap<Long, Server.Call>();
  private static final AtomicLong delayedRpcId = new AtomicLong();
  
  static Class<?> getProtocolClass(String protocolName, Configuration conf) 
  throws ClassNotFoundException {
    Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }

  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static Server get() {
    return SERVER.get();
  }

  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

  /**
   * This is the UGI of original caller, it is set when call is made through proxy layer and
   * overrides UGI of proxy process. This must be set by RPC handler after original UGI gets
   * deserialized.
   */
  private static final ThreadLocal<UserGroupInformation> OrigUGI = new
      ThreadLocal<UserGroupInformation>();

  /** Returns the remote side ip address when invoked inside an RPC
   *  Returns null incase of an error.
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null) {
      return call.connection.socket.getInetAddress();
    }
    return null;
  }
  
  /**
   * Gives access to the current call object in the code handling the response
   * @return The object of the call for the current thread
   */
  public static Object getCall() {
    return CurCall.get();
  }
  
  /**
   * Gives access to the subject of the current call.
   */
  public static UserGroupInformation getCurrentUGI() {
    try {
      // Check original caller's UGI in case call went through proxy
      UserGroupInformation origUGI = OrigUGI.get();
      if (origUGI != null) {
        return origUGI;
      }
      // If original caller's UGI is not set then we get the UGI of connecting client
      Call call = CurCall.get();
      if (call != null) {
        return call.connection.header.getUgi();
      }
    } catch (Exception e) { }
    return null;
  }

  /**
   * Sets original caller's UGI and associated Subject, used by NameNode proxy layer
   */
  public static void setOrignalCaller(UserGroupInformation ugi) {
    OrigUGI.set(ugi);
  }

  /**
   * If invoked from the RPC handling code will mark this call as
   * delayed response. It returns the id of the delayed call.
   * The response will only be sent once sendDelayedResponse method
   * is called with the id returned from this one.
   * @return id of the delayed response.
   */
  public static long delayResponse() {
    Call call = CurCall.get();
    long res = 0;
    if (call != null) {
      call.delayResponse();
      res = delayedRpcId.getAndIncrement();
      delayedCalls.put(res, call);
    }
    return res;
  }
  
  public static void sendDelayedResponse(long id) throws IOException {
    Call call = delayedCalls.remove(id);
    if (call != null) {
      call.sendDelayedResponse();
    }
  }
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  private String bindAddress;
  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private int readThreads;                        // number of read threads
  private boolean supportOldJobConf;                 // whether supports job conf as parameter
  private Class<? extends Writable> paramClass;   // class of call parameters
  private final boolean isInvocationClass;
  private int maxIdleTime;                        // the maximum idle time after
                                                  // which a client may be disconnected
  private int thresholdIdleConnections;           // the number of idle connections
                                                  // after which we will start
                                                  // cleaning up idle
                                                  // connections

  protected final RpcMetrics  rpcMetrics;

  private Configuration conf;

  private int maxQueueSize;
  private final int maxRespSize;
  private int socketSendBufferSize;
  private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm

  volatile private boolean running = true;         // true while server runs
  private BlockingQueue<Call> callQueue; // queued calls

  //maintain a list
  final ConnectionSet connectionSet;
  //of client connections
  private Listener listener = null;
  private Responder responder = null;
  private Handler[] handlers = null;
  private long pollingInterval = 1000;
  
  private boolean directHandling = false;

  /**
   * A convenience method to bind to a given address and report
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address,
                          int backlog) throws IOException {
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      BindException bindException = new BindException("Problem binding to " + address
                                                      + " : " + e.getMessage());
      bindException.initCause(e);
      throw bindException;
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: " +
                                       address.getHostName());
      } else {
        throw e;
      }
    }
  }

  /** A call queued for handling. */
  private static class Call {
    private int id;                               // the client's call id
    private Writable param;                       // the parameter passed
    private Connection connection;                // connection to client
    private long timestamp;     // the time received when response is null
                                   // the time served when response is not null
    private ByteBuffer response;                      // the response for this call
    private boolean delayResponse = false;
    private Responder responder;
    

    public Call(int id, Writable param, Connection connection, Responder responder) { 
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
      this.responder = responder;
    }
    
    @Override
    public String toString() {
      return param.toString() + " from " + connection.toString();
    }

    public synchronized void setResponse(ByteBuffer response) {
      this.response = response;
    }
    
    public synchronized void delayResponse() {
      this.delayResponse = true;
    }
    
    public synchronized void sendDelayedResponse() throws IOException {
      this.delayResponse = false;
      if (response != null) {
        responder.doRespond(this);
      }
    }
    
    public synchronized boolean delayed() {
      return this.delayResponse;
    }
  }
  
  private String getIpcServerName() {
    return "IPC Server listener on " + port;
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {

    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
//    private Selector[] readSelectors = null;
    private Reader[] readers = null;
    private int currentReader = 0;
    private InetSocketAddress address; //the address we bind at
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private int backlogLength = conf.getInt("ipc.server.listen.queue.size", 128);
    private ExecutorService readPool;
    volatile boolean cleanConnectionsAfterShutdown = true;

    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();

      readers = new Reader[readThreads];
      readPool = Executors.newFixedThreadPool(readThreads);
      for (int i = 0; i < readThreads; i++) {
        Selector readSelector = Selector.open();
        Reader reader = new Reader(readSelector, i);
        readers[i] = reader;
        readPool.execute(reader);
      }

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName(getIpcServerName());
      this.setDaemon(true);
    }

    private class Reader implements Runnable {
      private ByteArrayOutputStream buf;
      private final String name;
      private Selector readSelector = null;
      private AbstractQueue<SocketChannel> newChannels = new ArrayBlockingQueue<SocketChannel>(
          1024 * 16);

      Reader(Selector readSelector, int instanceNumber) {
        this.readSelector = readSelector;
        if (directHandling) {
          buf = new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
        }
        this.name = "IPC SocketReader "+ instanceNumber + " on " + port;
      }
      public void run() {
        FLOG.info(name + ": starting");
        synchronized (this) {
          while (running) {
            SelectionKey key = null;
            try {
              readSelector.select();
              processNewChannels();

              Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
              while (iter.hasNext()) {
                key = iter.next();
                iter.remove();
                if (key.isValid()) {
                  if (key.isReadable()) {
                    doRead(key, buf);
                    if (directHandling && buf.size() > maxRespSize) {
                      buf = new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
                    }
                  }
                }
                key = null;
              }
            } catch (InterruptedException e) {
              if (running) {                      // unexpected -- log it
                LOG.info(getName() + " caught: " +
                         StringUtils.stringifyException(e));
              }
            } catch (IOException ex) {
              LOG.error("Error in Reader", ex);
            }
          }
          try {
            readSelector.close();
          } catch (IOException ex) {}
        }
      }
      
      void addChannelToQueue(SocketChannel channel) {
        newChannels.add(channel);
        readSelector.wakeup();
      }
      
      void processNewChannel(SocketChannel channel) throws IOException {
        try {
          SelectionKey readKey = this.registerChannel(channel);
          Connection c = new Connection(readKey, channel,
              System.currentTimeMillis());
          readKey.attach(c);
          connectionSet.addConnection(c);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Server connection from " + c.toString()
                + "; # active connections: " + getNumOpenConnections()
                + "; # queued calls: " + callQueue.size());

          }
        } catch (IOException e) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Faill to set up server connection from "
                + NetUtils.getSrcNameFromSocketChannel(channel)
                + "; # active connections: " + connectionSet.numConnections
                + "; # queued calls: " + callQueue.size());

          }
        }
      }
      
      void processNewChannels() throws IOException {
        SocketChannel channel = null;
        while ((channel = newChannels.poll()) != null) {
          processNewChannel(channel);          
        }
      }

      public synchronized SelectionKey registerChannel(SocketChannel channel)
                                                          throws IOException {
          return channel.register(readSelector, SelectionKey.OP_READ);
      }

    }
    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all
     * connections will be looked at for the cleanup.
     */
    private void cleanupConnections(boolean force) {
      if (force || getNumOpenConnections() > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        connectionSet.cleanIdleConnections(!force, getName());        
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    @Override
    public void run() {
      FLOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      while (running) {
        SelectionKey key = null;
        try {
          selector.select(cleanupInterval);
          Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
                else if (key.isReadable())
                  doRead(key, null);
              }
            } catch (IOException e) {
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          LOG.warn("Out of Memory in server select", e);
          closeCurrentConnection(key, e);
          cleanupConnections(true);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(getName() + " caught: " +
                     StringUtils.stringifyException(e));
          }
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException e) { }

        selector= null;
        acceptChannel= null;

        if (cleanConnectionsAfterShutdown) {
          cleanConnections();
        }
        readPool.shutdownNow();
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
          closeConnection(c);
          c = null;
        }
      }
    }

    // The method that will return the next read selector to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }

    InetSocketAddress getAddress() {
      return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }

    void doAccept(SelectionKey key) throws IOException,  OutOfMemoryError {
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      // accept up to backlogLength/10 connections
      for (int i=0; i < backlogLength/10; i++) {
        SocketChannel channel = server.accept();
        if (channel==null) return;

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        Reader reader = getReader();
        reader.addChannelToQueue(channel);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Add connection to queue from "
              + NetUtils.getSrcNameFromSocketChannel(channel)
              + "; # active connections: " + getNumOpenConnections()
              + "; # queued calls: " + callQueue.size());
        }
      }
    }

    void doRead(SelectionKey key, ByteArrayOutputStream buf)
        throws InterruptedException {
      int count = 0;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;
      }
      c.setLastContact(System.currentTimeMillis());

      try {
        count = c.readAndProcess(buf);
      } catch (InterruptedException ieo) {
        LOG.info(getName() + ": readAndProcess caught InterruptedException", ieo);
        throw ieo;
      } catch (Exception e) {
        LOG.info(getName() + ": readAndProcess threw exception " + e +
            " from client " + c.getHostAddress() +
            ". Count of bytes read: " + count, e);
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + ": disconnecting client " +
                    c.getHostAddress() + ". Number of active connections: "+
                    getNumOpenConnections());
        closeConnection(c);
        c = null;
      }
      else {
        c.setLastContact(System.currentTimeMillis());
      }
    }

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.info(getName() + ":Exception in closing listener socket. " + e);
        }
      }
      readPool.shutdownNow();
    }
  }

  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
    private Selector writeSelector;
    private int pending;         // connections waiting to register
    volatile boolean responderExtension = false;

    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run() {
      FLOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      long lastPurgeTime = 0;   // last check for old calls.

      while (running || responderExtension) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long now = System.currentTimeMillis();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          LOG.debug("Checking for old call responses.");
          ArrayList<Call> calls;

          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              Call call = (Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) {
                calls.add(call);
              }
            }
          }

          for(Call call : calls) {
            try {
              doPurge(call, now);
            } catch (IOException e) {
              LOG.warn("Error in purging old calls " + e);
            }
          }
        } catch (OutOfMemoryError e) {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          LOG.warn("Out of Memory in server select", e);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          LOG.warn("Exception in Responder " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info("Stopping " + this.getName());
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue
    // for a long time.
    //
    private void doPurge(Call call, long now) throws IOException {
      LinkedList<Call> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        Iterator<Call> iter = responseQueue.listIterator(0);
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestamp + PURGE_INTERVAL) {
            closeConnection(call.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private boolean processResponse(LinkedList<Call> responseQueue,
                                    boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements = 0;
      Call call = null;
      try {
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          SocketChannel channel = call.connection.channel;
          if (!directHandling && LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to #" + call.id + " from " +
                      call.connection);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          int numBytes = channelWrite(channel, call.response);
          if (numBytes < 0) {
            return true;
          }
          if (!call.response.hasRemaining()) {
            call.connection.decRpcCount();
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (!directHandling && LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then
            // insert in Selector queue.
            //
            call.connection.responseQueue.addFirst(call);

            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = System.currentTimeMillis();

              incPending();
              try {
                // Wakeup the thread blocked on select, only then can the call
                // to channel.register() complete.
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (ClosedChannelException e) {
                //Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (!directHandling && LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote partial " + numBytes +
                        " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName()+", call " + call + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
    
    synchronized void doStop() {
      try {
        if (!this.isAlive())
          return;
        if (writeSelector != null) {
          writeSelector.wakeup();
          Thread.yield();
        }
        waitForConnections();
        responder.responderExtension = false;
        // best effort, if we didn't send everything within timeout
        // just kill the reponder
        responder.interrupt();
        cleanConnections();
      } catch (Throwable t) {
        if (responder != null)
          responder.interrupt();
      }
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  class Connection {
    private boolean versionRead = false; //if initial signature and
                                         //version are read
    private boolean headerRead = false;  //if the connection header that
                                         //follows version is read.

    private SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; // number of outstanding rpcs
    private long lastContact;
    private int dataLength;
    private Socket socket;
    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;

    ConnectionHeader header = new ConnectionHeader();
    Class<?> protocol;

    Subject user = null;

    // Fake 'call' for failed authorization response
    private final int AUTHROIZATION_FAILED_CALLID = -1;
    private final Call authFailedCall = 
      new Call(AUTHROIZATION_FAILED_CALLID, null, null, null);
    private ByteArrayOutputStream authFailedResponse = new ByteArrayOutputStream();
    
    public Connection(SelectionKey key, SocketChannel channel, 
                      long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      InetAddress addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount == 0;
    }

    /* Decrement the outstanding RPC count */
    private void decRpcCount() {
      rpcCount--;
    }

    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount++;
    }

    boolean timedOut(long currentTime) {
      if (isIdle() && currentTime -  lastContact > maxIdleTime)
        return true;
      return false;
    }

    public int readAndProcess(ByteArrayOutputStream buf) throws IOException,
        InterruptedException {
      while (true) {
        /* Read at most one RPC. If the header is not read completely yet
         * then iterate until we read first RPC or until there is no data left.
         */
        int count = -1;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);
          if (count < 0 || dataLengthBuffer.remaining() > 0)
            return count;
        }

        if (!versionRead) {
          //Every connection is expected to send the header.
          ByteBuffer versionBuffer = ByteBuffer.allocate(1);
          count = channelRead(channel, versionBuffer);
          if (count <= 0) {
            return count;
          }
          int version = versionBuffer.get(0);

          dataLengthBuffer.flip();
          if (!HEADER.equals(dataLengthBuffer) || version != CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " +
                     hostAddress + ":" + remotePort +
                     " got version " + version +
                     " expected version " + CURRENT_VERSION);
            return -1;
          }
          dataLengthBuffer.clear();
          versionRead = true;
          continue;
        }

        boolean rpcCountIncreased = false;
        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();

          if (dataLength == Client.PING_CALL_ID) {
            dataLengthBuffer.clear();
            return 0;  //ping message
          }
          data = ByteBuffer.allocate(dataLength);
          incRpcCount();  // Increment the rpc count
          rpcCountIncreased = true;
        }

        count = channelRead(channel, data);

        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          if (headerRead) {
            processData(buf);
            data = null;
            return count;
          } else {
            processHeader();
            headerRead = true;
            data = null;
            
            if (rpcCountIncreased) {
              // RPC counter has been incremented, but since we only read
              // header, we need to offset it.
              decRpcCount();
            }
            
            // Authorize the connection
            try {
              authorize(user, header);

              if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully authorized " + header);
              }
            } catch (AuthorizationException ae) {
              authFailedCall.connection = this;
              setupResponse(authFailedResponse, authFailedCall,
                            Status.FATAL, null,
                            ae.getClass().getName(), ae.getMessage());
              responder.doRespond(authFailedCall);

              // Close this connection
              return -1;
            }

            continue;
          }
        }
        return count;
      }
    }

    /// Reads the connection header following version
    private void processHeader() throws IOException {
      DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(data.array()));
      header.readFields(in);
      try {
        String protocolClassName = header.getProtocol();
        if (protocolClassName != null) {
          protocol = getProtocolClass(header.getProtocol(), conf);
        }
      } catch (ClassNotFoundException cnfe) {
        throw new IOException("Unknown protocol: " + header.getProtocol());
      }

      // TODO: Get the user name from the GSS API for Kerberbos-based security
      // Create the user subject
      user = SecurityUtil.getSubject(header.getUgi());
    }

    private void processData(ByteArrayOutputStream buf) throws  IOException, InterruptedException {
      DataInputStream dis =
        new DataInputStream(new ByteArrayInputStream(data.array()));
      int id = dis.readInt();                    // try to read an id

      boolean isDebugEnabled = LOG.isDebugEnabled();
      if (isDebugEnabled) {
        LOG.debug(" got #" + id);
      }

      Writable param;
      if (isInvocationClass) {
        Invocation inv = new Invocation();
        inv.setConf(conf);
        param = inv;
      } else {
        param = ReflectionUtils.newInstance(paramClass, conf,
            supportOldJobConf); // read param
      }
      param.readFields(dis);        
        
      Call call = new Call(id, param, this, responder);
      if (directHandling) {
        if (isDebugEnabled) {
          LOG.debug("Staring processing call #" + id);
        }
        processCall(call, buf);
        if (isDebugEnabled) {
          LOG.debug("Finish processing call #" + id);
        }
      } else {
        callQueue.put(call);              // queue the call; maybe blocked here
        // added one call to the queue
        rpcMetrics.callQueueLen.inc(1);
      }
    }

    synchronized void close() throws IOException {
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception e) {}
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception e) {}
      }
      try {socket.close();} catch(Exception e) {}
    }
  }
  
  private void processCall(final Call call, final ByteArrayOutputStream buf) throws IOException {
    boolean isDebugEnabled = LOG.isDebugEnabled();
    if (isDebugEnabled) {
      LOG.debug(Thread.currentThread().getName() + ": has #" + call.id + " from " +
                call.connection);
    }

    String errorClass = null;
    String error = null;
    Writable value = null;

    // Original caller's UGI can be set from handler only, for now have to assume that there was
    // no proxy and orignal caller is an actual caller.
    setOrignalCaller(null);
    CurCall.set(call);
    try {
      if (directHandling) {
        value = call(call.connection.protocol, call.param, call.timestamp);
      } else {
        // Make the call as the user via Subject.doAs, thus associating
        // the call with the Subject
        value = Subject.doAs(call.connection.user,
            new PrivilegedExceptionAction<Writable>() {
              @Override
              public Writable run() throws Exception {
                // make the call
                return call(call.connection.protocol, call.param, call.timestamp);
  
              }
            });
      }
    } catch (PrivilegedActionException pae) {
      Exception e = pae.getException();
      LOG.info(Thread.currentThread().getName()+", call "+call+": error: " + e, e);
      errorClass = e.getClass().getName();
      error = StringUtils.stringifyException(e);
    } catch (Throwable e) {
      LOG.info(Thread.currentThread().getName()+", call "+call+": error: " + e, e);
      errorClass = e.getClass().getName();
      error = StringUtils.stringifyException(e);
    }
    setOrignalCaller(null);
    CurCall.set(null);
    
    if (directHandling 
        && value != null 
        && error == null
        && value instanceof ObjectWritable
        && ((ObjectWritable) value).getDeclaredClass().equals(ShortVoid.class)) {
      // we want to write the response directly for short voids
      // 12 bytes = (int) call.id + (int) status + ShortVoid serialized len
      if (isDebugEnabled) {
        LOG.debug("Start to construct result of call #" + call.id);
      }
      ByteBuffer response = ByteBuffer
          .allocate(8 + ShortVoid.serializedName.length);
      response.putInt(call.id);
      response.put(Status.SUCCESSBYTES);
      response.put(ShortVoid.serializedName);
      response.position(0);
      call.setResponse(response);
    } else {
      if (isDebugEnabled) {
        LOG.debug("Start to construct result using setupResponse of call #" + call.id);
      }
      setupResponse(buf, call,
                    (error == null) ? Status.SUCCESS : Status.ERROR,
                    value, errorClass, error);
    }
    if (!call.delayed()) {
      responder.doRespond(call);
    }
  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    public Handler(int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber + " on " + port);
    }

    @Override
    public void run() {
      FLOG.info(getName() + ": starting");
      SERVER.set(Server.this);
       ByteArrayOutputStream buf =
         new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
      while (running) {
        try {
          final Call call = callQueue.poll(pollingInterval, TimeUnit.MILLISECONDS); 
          // pop the queue; maybe blocked here for up to a second
          // poll() is used instead of take() to enable clean shutdown
          if (call == null)
            continue;
          // we picked up one call from the queue
          rpcMetrics.callQueueLen.inc(-1);
          
          processCall(call, buf);
          
          // monitor the size of the buffer
          rpcMetrics.rpcResponseSize.inc(buf.size());
          
          // Discard the large buf and reset it back to
          // smaller size to freeup heap
          if (buf.size() > maxRespSize) {
            LOG.warn("Large response size " + buf.size() + " for call " +
                call.toString());
            buf = new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
          }
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(getName() + " caught: " +
                     StringUtils.stringifyException(e));
          }
        } catch (Exception e) {
          LOG.info(getName() + " caught: " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info(getName() + ": exiting");
    }

  }

  protected Server(String bindAddress, int port,
      Class<? extends Writable> paramClass, int handlerCount, Configuration conf)
      throws IOException {
    this(bindAddress, port, paramClass, handlerCount, conf, true);
  }
  
  protected Server(String bindAddress, int port,
                  Class<? extends Writable> paramClass, int handlerCount,
                  Configuration conf, boolean supportOldJobConf)
    throws IOException
  {
    this(bindAddress, port, paramClass, handlerCount, conf, Integer
        .toString(port), supportOldJobConf);
  }
  /** Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   *
   */
  protected Server(String bindAddress, int port,
                  Class<? extends Writable> paramClass, int handlerCount,
                  Configuration conf, String serverName, boolean supportOldJobConf)
    throws IOException {
    
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.port = port;
    this.paramClass = paramClass;
    this.isInvocationClass = paramClass.equals(Invocation.class);
    this.directHandling = conf.getBoolean("ipc.direct.handling", false);
    
    if (directHandling) {
      LOG.info("Starting direct handler server");
      // reader threads are directly calling protocol methods
      this.handlerCount = 0;
      // make the direct buffer size bigger for faster reads
      NIO_BUFFER_LIMIT = 8 * NIO_BUFFER_LIMIT_BASE;
      // initial response buffer size must be at least the size
      // of max NIO_BUFFER_LIMIT (see channelIO())
      INITIAL_RESP_BUF_SIZE = NIO_BUFFER_LIMIT;
    } else {
      // reader threads populate callQueue, calls are executed by handlers
      this.handlerCount = handlerCount;
    }
    
    this.socketSendBufferSize = 0;
    this.maxQueueSize = handlerCount * conf.getInt(
                                   IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
                                   MAX_QUEUE_SIZE_PER_HANDLER);
    this.maxRespSize = conf.getInt(IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
                                   IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
    this.readThreads = conf.getInt(IPC_SERVER_RPC_READ_THREADS_KEY,
                                   IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    this.supportOldJobConf = supportOldJobConf;
    conf.setBoolean("rpc.support.jobconf", supportOldJobConf);
    this.callQueue  = new ArrayBlockingQueue<Call>(maxQueueSize);
    this.maxIdleTime = 2*conf.getInt(IPC_SERVER_CLIENT_CONN_MAXIDLETIME, 1000);
    this.thresholdIdleConnections = conf.getInt(IPC_SERVER_CLIENT_IDLETHRESHOLD, 4000);

    // Start the listener here and let it bind to the port
    listener = new Listener();
    this.port = listener.getAddress().getPort();
    this.rpcMetrics = new RpcMetrics(serverName,
                          Integer.toString(this.port), this);
    // For now, we use readThreads*4 as connection bucket numbers for simplicity.
    this.connectionSet = new ConnectionSet(getIpcServerName(), readThreads * 4,
        rpcMetrics);
    this.tcpNoDelay = conf.getBoolean("ipc.server.tcpnodelay", false);
    this.pollingInterval = conf.getLong("rpc.polling.interval", 1000);

    // Create the responder here
    responder = new Responder();
  }
  
  void cleanConnections() {
    connectionSet.cleanConnections();
  }
  
  void waitForConnections() {
    int retries = 10;
    while (retries-- > 0) {
      if (connectionSet.ifConnectionsClean()) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  private void closeConnection(Connection connection) {
    connectionSet.removeConnection(connection);
    try {
      connection.close();
    } catch (IOException e) {
    }
  }

  /**
   * Setup response for the IPC Call.
   *
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param status {@link Status} of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(ByteArrayOutputStream response,
                             Call call, Status status,
                             Writable rv, String errorClass, String error)
  throws IOException {
    response.reset();
    DataOutputStream out = new DataOutputStream(response);
    out.writeInt(call.id);                // write call id
    
    if (status == Status.SUCCESS) {
      out.write(Status.SUCCESSBYTES);
      rv.write(out);
    } else {
      out.writeInt(status.state);           // write status
      WritableUtils.writeString(out, errorClass);
      WritableUtils.writeString(out, error);
    }
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
  }

  Configuration getConf() {
    return conf;
  }

  /** Sets the socket buffer size used for responding to RPCs */
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  /**
   * Denotes whether the server is alive
   * @return true if the server is alive, false otherwise.
   */
  public boolean isAlive() {
    return listener.isAlive();
  }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() throws IOException {
    if (responder.isAlive()) {
      // The server is already running
      return;
    }
    responder.start();
    listener.start();
    handlers = new Handler[handlerCount];

    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /**
   * Waits for all RPC handlers to exit. This ensures that no further RPC
   * calls would be processed by this server.
   */
  public synchronized void waitForHandlers() throws InterruptedException {
    if (handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].join();
        }
      }
    }
  }
  
  public synchronized void stop() {
    // by default interrupt handlers and stop the responder
    stop(true);
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop(boolean interruptHandlers) {
    LOG.info("Stopping server on " + port);
    
    if (!interruptHandlers) {
      // keep responder working
      responder.responderExtension = true;
      // do not purge connections in listener
      listener.cleanConnectionsAfterShutdown = false;
    } else {
      responder.interrupt();
    }
    
    running = false;
    if (interruptHandlers && handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }

    listener.interrupt();
    listener.doStop();
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
  }
  
  public synchronized void stopResponder() {
    responder.doStop();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }

  /**
   * Called for each call.
   * @deprecated Use {@link #call(Class, Writable, long)} instead
   */
  @Deprecated
  public Writable call(Writable param, long receiveTime) throws IOException {
    return call(null, param, receiveTime);
  }

  /** Called for each call. */
  public abstract Writable call(Class<?> protocol,
                               Writable param, long receiveTime)
  throws IOException;

  /**
   * Authorize the incoming client connection.
   *
   * @param user client user
   * @param connection incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  public void authorize(Subject user, ConnectionHeader connection)
  throws AuthorizationException {}

  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections() {
    return connectionSet.numConnections.get();
  }

  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen() {
    return callQueue.size();
  }


  /**
   * When the read or write buffer size is larger than this limit, i/o will be
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static final int NIO_BUFFER_LIMIT_BASE = 8 * 1024;
  private static int NIO_BUFFER_LIMIT = NIO_BUFFER_LIMIT_BASE; //should not be more than 64KB.

  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large
   * buffer.
   *
   * @see WritableByteChannel#write(ByteBuffer)
   */
  private static int channelWrite(WritableByteChannel channel,
                                  ByteBuffer buffer) throws IOException {

    return (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.write(buffer) : channelIO(null, channel, buffer);
  }


  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * ByteBuffer increases. There should not be any performance degredation.
   *
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  private static int channelRead(ReadableByteChannel channel,
                                 ByteBuffer buffer) throws IOException {

    return (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.read(buffer) : channelIO(channel, null, buffer);
  }

  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @see #channelRead(ReadableByteChannel, ByteBuffer)
   * @see #channelWrite(WritableByteChannel, ByteBuffer)
   */
  private static int channelIO(ReadableByteChannel readCh,
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {

    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;

    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);

        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);
      }
    }

    int nBytes = initialRemaining - buf.remaining();
    return (nBytes > 0) ? nBytes : ret;
  }
}
