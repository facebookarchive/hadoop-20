package org.apache.hadoop.thriftdatanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.ipc.ProtocolProxy;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.hadoop.thriftdatanode.api.*;


/**
 * ThriftHadoopDatanode
 * A thrift wrapper around the Hadoop Datanode
 */
public class HadoopThriftDatanodeServer extends ThriftHadoopDatanode {

  static int serverPort = 0;                    // default port
  TServer    server = null;

  public static class HadoopThriftHandler implements ThriftHadoopDatanode.Iface
  {

    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.thriftdatanode");
    public static final int timeout = 10000;

    // HDFS glue
    Configuration conf;
    FileSystem fs;
        
    // stucture that maps each host:port object into an RPC object
    private HashMap<String, ClientDatanodeProtocol> hadoopHash = 
      new HashMap<String, ClientDatanodeProtocol>();

    // Detect inactive session
    private static volatile boolean fsRunning = true;
    private static long now;

    // allow outsider to change the hadoopthrift path
    public void setOption(String key, String val) {
    }

    /**
     * Current system time.
     * @return current time in msec.
     */
    static long now() {
      return System.currentTimeMillis();
    }

    /**
    * getVersion
    *
    * @return current version of the interface.
    */
    public String getVersion() {
      return "0.1";
    }

    /**
     * shutdown
     *
     * cleanly closes everything and exit.
     */
    public void shutdown(int status) {
      LOG.info("HadoopThriftDatanodeServer shutting down.");
      try {
        fs.close();
      } catch (IOException e) {
        LOG.warn("Unable to close file system");
      }
      Runtime.getRuntime().exit(status);
    }

    /**
     * HadoopThriftDatanodeServer
     *
     * Constructor for the HadoopThriftDatanodeServer glue with Thrift Class.
     *
     * @param name - the name of this handler
     */
    public HadoopThriftHandler(String name) {
      conf = new Configuration();
      now = now();
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        LOG.warn("Unable to open hadoop file system...");
        Runtime.getRuntime().exit(-1);
      }
    }

    /**
      * printStackTrace
      *
      * Helper function to print an exception stack trace to the log and not stderr
      *
      * @param e the exception
      *
      */
    static private void printStackTrace(Exception e) {
      for(StackTraceElement s: e.getStackTrace()) {
        LOG.error(s);
      }
    }

    /**
     * Parse a host:port pair and return the port name
     */
    static private int getPort(String name) {
      int colon = name.indexOf(":");
      if (colon < 0) {
        return 50010; // default port.
      }
      return Integer.parseInt(name.substring(colon+1));
    }

    /**
     * Creates one rpc object if necessary
     */
    private synchronized ClientDatanodeProtocol getOrCreate(String name)
      throws IOException {
      ClientDatanodeProtocol obj =  hadoopHash.get(name);
      if (obj != null) {
        return obj;
      }
      // connection does not exist, create a new one.
      DatanodeID dn = new DatanodeID(name, "", -1, getPort(name));
      ClientDatanodeProtocol instance =
        DFSClient.createClientDatanodeProtocolProxy(dn, conf, timeout); 

      // cache connection
      hadoopHash.put(name, instance);
      return instance;
    }

    /**
     * Implement the API exported by this thrift server
     */

    public ThdfsBlock recoverBlock(TDatanodeID datanode,
                          ThdfsNamespaceId namespaceId,
                          ThdfsBlock block,
                          boolean keepLength,
                          List<TDatanodeID> targets,
                          long deadline)
                          throws ThriftIOException, TException {
      Block blk = new Block(block.blockId, block.numBytes, 
                            block.generationStamp);
      DatanodeInfo[] targs = new DatanodeInfo[targets.size()];
      for (int i = 0; i < targs.length; i++) {
        targs[i] = new DatanodeInfo(
           new DatanodeID(targets.get(i).name, "", -1, 
                          getPort(targets.get(i).name)));
      }
      // make RPC to datanode
      try {
        ClientDatanodeProtocol remote = getOrCreate(datanode.name);
        Block nblk = remote.recoverBlock(namespaceId.id, blk,
                              keepLength, targs, deadline).getBlock();
        return new ThdfsBlock(nblk.getBlockId(), nblk.getNumBytes(),
                              nblk.getGenerationStamp());
      } catch (IOException e) {
        String msg = "Error recoverBlock datanode " + datanode.name +
                     " namespaceid " + namespaceId.id +
                     " block " + blk;
        LOG.warn(msg);
        throw new ThriftIOException(msg);
      }
    }

    // get block info from datanode
    public ThdfsBlock getBlockInfo(TDatanodeID datanode,
                          ThdfsNamespaceId namespaceid,
                          ThdfsBlock block)
                          throws ThriftIOException, TException { 
      Block blk = new Block(block.blockId, block.numBytes, 
                             block.generationStamp);
      // make RPC to datanode
      try {
        ClientDatanodeProtocol remote = getOrCreate(datanode.name);
        Block nblk = remote.getBlockInfo(namespaceid.id, blk);
        return new ThdfsBlock(nblk.getBlockId(), nblk.getNumBytes(),
                              nblk.getGenerationStamp());
      } catch (IOException e) {
        String msg = "Error getBlockInfo datanode " + datanode.name +
                     " namespaceid " + namespaceid.id +
                     " block " + blk;
        LOG.warn(msg);
        throw new ThriftIOException(msg);
      }
    }

    // Instruct the datanode to copy a block to specified target.
    public void copyBlock(TDatanodeID datanode,
                          ThdfsNamespaceId srcNamespaceId, ThdfsBlock srcblock,
                          ThdfsNamespaceId dstNamespaceId, ThdfsBlock destblock,
                          TDatanodeID target, boolean asynchronous)
                          throws ThriftIOException, TException {
      Block sblk = new Block(srcblock.blockId, srcblock.numBytes, 
                             srcblock.generationStamp);
      Block dblk = new Block(destblock.blockId, destblock.numBytes, 
                             destblock.generationStamp);
      DatanodeInfo targs = new DatanodeInfo(
           new DatanodeID(target.name, "", -1, getPort(target.name)));

      // make RPC to datanode
      try {
        ClientDatanodeProtocol remote = getOrCreate(datanode.name);
        remote.copyBlock(srcNamespaceId.id, sblk,
                         dstNamespaceId.id, dblk,
                         targs, asynchronous);
      } catch (IOException e) {
        String msg = "Error copyBlock datanode " + datanode.name +
                     " srcnamespaceid " + srcNamespaceId.id +
                     " destnamespaceid " + dstNamespaceId.id +
                     " srcblock " + sblk +
                     " destblock " + dblk;
        LOG.warn(msg);
        throw new ThriftIOException(msg);
      }
    }

    // Retrives filename of blockfile and metafile from datanode
    public ThdfsBlockPath getBlockPathInfo(TDatanodeID datanode,
                                  ThdfsNamespaceId namespaceId,
                                  ThdfsBlock block)
                                  throws ThriftIOException, TException {
      Block blk = new Block(block.blockId, block.numBytes, 
                            block.generationStamp);

      // make RPC to datanode to find local pathnames of blocks
      try {
        ClientDatanodeProtocol remote = getOrCreate(datanode.name);
        BlockPathInfo pathinfo = remote.getBlockPathInfo(namespaceId.id, blk);
        return new ThdfsBlockPath(pathinfo.getBlockPath(), 
                                  pathinfo.getMetaPath());
      } catch (IOException e) {
        String msg = "Error getBlockPathInfo datanode " + datanode.name +
                     " namespaceid " + namespaceId.id +
                     " block " + blk;
        LOG.warn(msg);
        throw new ThriftIOException(msg);
      }
    }

  } // end of ThriftHadoopDatanode.Iface

  // Bind to port. If the specified port is 0, then bind to random port.
  private ServerSocket createServerSocket(int port) throws IOException {
    try {
      ServerSocket sock = new ServerSocket();
      // Prevent 2MSL delay problem on server restarts
      sock.setReuseAddress(true);
      // Bind to listening port
      if (port == 0) {
        sock.bind(null);
        serverPort = sock.getLocalPort();
      } else {
        sock.bind(new InetSocketAddress(port));
      }
      return sock;
    } catch (IOException ioe) {
      throw new IOException("Could not create ServerSocket on port " + port + "." +
                            ioe);
    }
  }

  /**
   * Constructs a server object
   */
  public HadoopThriftDatanodeServer(String [] args) throws Exception {

    if (args.length > 0) {
      serverPort = new Integer(args[0]);
    }
    try {
      ServerSocket ssock = createServerSocket(serverPort);
      TServerTransport serverTransport = new TServerSocket(ssock);
      Iface handler = new HadoopThriftHandler("hdfs-thriftdatanode-dhruba");
      ThriftHadoopDatanode.Processor processor = new ThriftHadoopDatanode.Processor(handler);
      TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
      serverArgs.minWorkerThreads = 10;
      serverArgs.processor(processor);
      serverArgs.transportFactory(new TTransportFactory());
      serverArgs.protocolFactory(new TBinaryProtocol.Factory());
      server = new TThreadPoolServer(serverArgs);
      System.out.println("Starting the hadoop datanode thrift server on port [" + serverPort + "]...");
      HadoopThriftHandler.LOG.info("Starting the hadoop datanode thrift server on port [" +serverPort + "]...");
      System.out.flush();

    } catch (Exception x) {
      HadoopThriftHandler.LOG.warn("XXX Exception in HadoopThriftDatanodeServer");
      x.printStackTrace();
      throw x;
    }
  }

  public static void main(String [] args) throws Exception {
    HadoopThriftDatanodeServer me = new HadoopThriftDatanodeServer(args);
    me.server.serve();
  }
};

