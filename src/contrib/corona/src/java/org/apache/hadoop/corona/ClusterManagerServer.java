package org.apache.hadoop.corona;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;

public class ClusterManagerServer extends Thread {
  public static final Log LOG = LogFactory.getLog(ClusterManagerServer.class);

  static {
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
    this.port = addr.getPort();
    ServerSocket serverSocket = new ServerSocket(addr.getPort());
    this.port = serverSocket.getLocalPort();
    server = TFactoryBasedThreadPoolServer.createNewServer(
      new ClusterManagerService.Processor(cm), serverSocket,
      conf.getCMSoTimeout());
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
      throws IOException, TTransportException, ParseException {
    StringUtils.startupShutdownMessage(ClusterManager.class, args, LOG);
    Configuration conf = new Configuration();
    boolean recoverFromDisk = false;
    // Check if we want to start the ClusterManager to restore the persisted
    // state
    Option recoverFromDiskOption =
      new Option("recoverFromDisk",
                  "Used to restart the CM from the state persisted on disk");
    Options options = new Options();
    options.addOption(recoverFromDiskOption);
    CommandLineParser parser = new GnuParser();
    CommandLine line = parser.parse(options, args);

    if (line.hasOption("recoverFromDisk")) {
      recoverFromDisk = true;
    }
    ClusterManager cm = new ClusterManager(conf, recoverFromDisk);
    try {
      ClusterManagerServer server = new ClusterManagerServer(conf, cm);
      server.start();
      server.join();
    } catch (InterruptedException e) {
      System.exit(0);
    }
  }
}
