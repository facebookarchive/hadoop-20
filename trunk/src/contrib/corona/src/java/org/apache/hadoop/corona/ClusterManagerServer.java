package org.apache.hadoop.corona;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.Utilities;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

public class ClusterManagerServer extends Thread {
  public static final Log LOG = LogFactory.getLog(ClusterManagerServer.class);

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    Utilities.makeProcessExitOnUncaughtException(LOG);
  }

  Configuration conf;
  int port;
  TServer server;
  volatile boolean running = true;

  public ClusterManagerServer(Configuration conf, ClusterManager cm)
    throws IOException {
    this(new CoronaConf(conf), cm);
  }

  public ClusterManagerServer(CoronaConf conf, ClusterManager cm)
      throws IOException {
    this.conf = conf;
    String target = conf.getClusterManagerAddress();
    InetSocketAddress addr = NetUtils.createSocketAddr(target);
    ServerSocket serverSocket = new ServerSocket(addr.getPort());
    this.port = serverSocket.getLocalPort();
    TServerSocket socket = new TServerSocket(serverSocket, conf.getCMSoTimeout());

    TFactoryBasedThreadPoolServer.Args args =
      new TFactoryBasedThreadPoolServer.Args(socket);
    args.stopTimeoutVal = 0;
    args.processor(new ClusterManagerService.Processor(cm));
    args.transportFactory(new TFramedTransport.Factory());
    args.protocolFactory(new TBinaryProtocol.Factory(true, true));
    server = new TFactoryBasedThreadPoolServer(
      args, new TFactoryBasedThreadPoolServer.DaemonThreadFactory());
  }

  public void stopRunning() {
    this.running = false;
    this.server.stop();
    // Do an dummy connect to the server port. This will cause an thrift
    // exception and move the server beyond the blocking accept.
    // Thread.interrupt() does not help.
    try {
      new Socket(java.net.InetAddress.getByName("127.0.0.1"), port).close();
    } catch (IOException e) {}
    this.interrupt();
  }

  public void run() {
    server.serve();
  }

  public static void main(String[] args)
      throws IOException, TTransportException {
    StringUtils.startupShutdownMessage(ClusterManager.class, args, LOG);
    Configuration conf = new Configuration();
    ClusterManager cm = new ClusterManager(conf);
    try {
      ClusterManagerServer server = new ClusterManagerServer(conf, cm);
      server.start();
      server.join();
    } catch (InterruptedException e) {
      System.exit(0);
    }
  }
}
