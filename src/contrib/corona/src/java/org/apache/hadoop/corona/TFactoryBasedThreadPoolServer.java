package org.apache.hadoop.corona;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadFactory;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Variation of Thrift's TThreadPoolServer to allow using
 * threads created by a custom thread factory. This is based
 * on version Apache Thrift version 0.7.0.
 * TThreadPoolServer does not perform a forced shutdown of its threadpool
 * threads. This will prevent a clean exit of a process after shutdown()
 * is called. By using DaemonThreadFactory as the thread factory, this
 * class allows a clean exit of the process.
 */
public class TFactoryBasedThreadPoolServer extends TServer {

  /**
   * This is a helper method which creates a TFactoryBased..Server object using
   * the processor, ServerSocket object and socket timeout limit. This is useful
   * when we change the mechanism of server object creation. As a result,
   * we don't have to change code in multiple places.
   * @param processor
   * @param serverSocket
   * @param socketTimeOut
   * @return A TFactoryBasedThreadPoolServer object
   * @throws IOException
   */
  public static TFactoryBasedThreadPoolServer createNewServer(
    TProcessor processor, ServerSocket serverSocket, int socketTimeOut)
    throws IOException {
    TServerSocket socket = new TServerSocket(serverSocket, socketTimeOut);
    TFactoryBasedThreadPoolServer.Args args =
      new TFactoryBasedThreadPoolServer.Args(socket);
    args.stopTimeoutVal = 0;
    args.processor(processor);
    args.transportFactory(new TFramedTransport.Factory());
    args.protocolFactory(new TBinaryProtocol.Factory(true, true));
    return new TFactoryBasedThreadPoolServer(
      args, new TFactoryBasedThreadPoolServer.DaemonThreadFactory());

  }

  public static class DaemonThreadFactory implements ThreadFactory {
    ThreadFactory defaultFactory = Executors.defaultThreadFactory();
    public Thread newThread(Runnable r) {
      Thread t = defaultFactory.newThread(r);
      t.setDaemon(true);
      return t;
    }
  }

  /** This constructor is basically the same as TThreadPoolServer,
   * but can use a thread factory.
   */
  public TFactoryBasedThreadPoolServer(Args args, ThreadFactory factory) {
    super(args);

    SynchronousQueue<Runnable> executorQueue =
      new SynchronousQueue<Runnable>();

    stopTimeoutUnit = args.stopTimeoutUnit;
    stopTimeoutVal = args.stopTimeoutVal;

    executorService_ = new ThreadPoolExecutor(args.minWorkerThreads,
                                              args.maxWorkerThreads,
                                              60,
                                              TimeUnit.SECONDS,
                                              executorQueue,
                                              factory);
  }

  /**************************** Code from TThreadPoolServer ******************/
  private static final Log LOG = LogFactory.getLog(TFactoryBasedThreadPoolServer.class);

  public static class Args extends AbstractServerArgs<Args> {
    public int minWorkerThreads = 5;
    public int maxWorkerThreads = Integer.MAX_VALUE;
    public int stopTimeoutVal = 60;
    public TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;

    public Args(TServerTransport transport) {
      super(transport);
    }

    public Args minWorkerThreads(int n) {
      minWorkerThreads = n;
      return this;
    }

    public Args maxWorkerThreads(int n) {
      maxWorkerThreads = n;
      return this;
    }
  }

  // Executor service for handling client connections
  private ExecutorService executorService_;

  // Flag for stopping the server
  private volatile boolean stopped_;

  private final TimeUnit stopTimeoutUnit;

  private final long stopTimeoutVal;

  public void serve() {
    try {
      serverTransport_.listen();
    } catch (TTransportException ttx) {
      LOG.error("Error occurred during listening.", ttx);
      return;
    }

    stopped_ = false;
    setServing(true);
    while (!stopped_) {
      int failureCount = 0;
      try {
        TTransport client = serverTransport_.accept();
        WorkerProcess wp = new WorkerProcess(client);
        executorService_.execute(wp);
      } catch (TTransportException ttx) {
        if (!stopped_) {
          ++failureCount;
          LOG.warn("Transport error occurred during acceptance of message.", ttx);
        }
      }
    }

    executorService_.shutdown();

    // Loop until awaitTermination finally does return without a interrupted
    // exception. If we don't do this, then we'll shut down prematurely. We want
    // to let the executorService clear it's task queue, closing client sockets
    // appropriately.
    long timeoutMS = stopTimeoutUnit.toMillis(stopTimeoutVal);
    long now = System.currentTimeMillis();
    while (timeoutMS >= 0) {
      try {
        executorService_.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
        break;
      } catch (InterruptedException ix) {
        long newnow = System.currentTimeMillis();
        timeoutMS -= (newnow - now);
        now = newnow;
      }
    }
    setServing(false);
  }

  public void stop() {
    stopped_ = true;
    serverTransport_.interrupt();
  }

  private class WorkerProcess implements Runnable {

    /**
     * Client that this services.
     */
    private TTransport client_;

    /**
     * Default constructor.
     *
     * @param client Transport to process
     */
    private WorkerProcess(TTransport client) {
      client_ = client;
    }

    /**
     * Loops on processing a client forever
     */
    public void run() {
      TProcessor processor = null;
      TTransport inputTransport = null;
      TTransport outputTransport = null;
      TProtocol inputProtocol = null;
      TProtocol outputProtocol = null;
      try {
        processor = processorFactory_.getProcessor(client_);
        inputTransport = inputTransportFactory_.getTransport(client_);
        outputTransport = outputTransportFactory_.getTransport(client_);
        inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
        outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
        // we check stopped_ first to make sure we're not supposed to be shutting
        // down. this is necessary for graceful shutdown.
        while (!stopped_ && processor.process(inputProtocol, outputProtocol)) {}
      } catch (TTransportException ttx) {
        // Assume the client died and continue silently
      } catch (TException tx) {
        LOG.error("Thrift error occurred during processing of message.", tx);
      } catch (Exception x) {
        LOG.error("Error occurred during processing of message.", x);
      }

      if (inputTransport != null) {
        inputTransport.close();
      }

      if (outputTransport != null) {
        outputTransport.close();
      }
    }
  }
}
