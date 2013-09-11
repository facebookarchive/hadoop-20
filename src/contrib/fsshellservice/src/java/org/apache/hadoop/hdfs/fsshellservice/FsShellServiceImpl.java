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
package org.apache.hadoop.hdfs.fsshellservice;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * A running service that execute simple FSShell commands as Thrift calls
 */
public class FsShellServiceImpl implements FsShellService.Iface, Runnable {
  
  static {
    Configuration.addDefaultResource("fsshellservice-default.xml");
    Configuration.addDefaultResource("hdfs-default.xml");

    Configuration.addDefaultResource("fsshellservice-site.xml");
    Configuration.addDefaultResource("hdfs-site.xml"); 
  }
  public static final Log LOG = LogFactory.getLog(FsShellServiceImpl.class);

  Configuration conf = new Configuration();
  private int clientTimeout;
  private TServer tserver;

  private FileSystem getFileSystem(String name, boolean unique) throws IOException,
      URISyntaxException {
    URI uri = new URI(name);
    if (!unique) {
      return FileSystem.get(uri, conf);
    } else {
      return FileSystem.newInstance(uri, conf);
    }
  }
  
  @Override
  public void copyFromLocal(String src, String dest, boolean validate)
      throws FsShellException, TException {
    LOG.info("copy from local: src: " + src + " dest: " + dest);
    try {
      FileSystem fs = getFileSystem(dest, true);
      try {
        fs.copyFromLocalFile(false, false, validate, new Path(src), new Path(
            dest));
      } finally {
        fs.close();
      }
    } catch (IOException e) {
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public void copyToLocal(String src, String dest, boolean validate)
      throws FsShellException, TException {
    LOG.info("copy to local: src: " + src + " dest: " + dest);
    try {
      getFileSystem(src, false).copyToLocalFile(false, validate, new Path(src),
          new Path(dest));
    } catch (IOException e) {
      LOG.info("copyToLocal IOException", e);
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public boolean remove(String path, boolean recursive, boolean skipTrash)
      throws FsShellException, TException {
    LOG.info("remove: src: " + path + " recusive: " + recursive
        + " skipTrash: " + skipTrash);
    try {
      return getFileSystem(path, false).delete(new Path(path), recursive, skipTrash);
    } catch (IOException e) {
      LOG.info("remove IOException", e);
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }
  
  @Override
  public boolean mkdirs(String f) throws FsShellException, TException {
    LOG.info("mkdir f: " + f);
    try {
      return getFileSystem(f, false).mkdirs(new Path(f));
    } catch (IOException e) {
      LOG.info("mkdirs IOException", e);
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public boolean rename(String src, String dest) throws FsShellException,
      TException {
    LOG.info("rename: src: " + src + " dest: " + dest);
    try {
      return getFileSystem(src, false).rename(new Path(src), new Path(dest));
    } catch (IOException e) {
      LOG.info("src IOException", e);
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public List<DfsFileStatus> listStatus(String path) throws FsShellException,
      FsShellFileNotFoundException, TException {
    LOG.info("listStatus: path: " + path);
    try {
      FileStatus[] fsArray =  getFileSystem(path, false).listStatus(new Path(path));
      if (fsArray == null) {
        // directory doesn't exist
        throw new FsShellFileNotFoundException("The directory doesn't exist.");
      }
      List<DfsFileStatus> retList = new ArrayList<DfsFileStatus>(fsArray.length);
      for (FileStatus fs : fsArray) {
        retList.add(new DfsFileStatus(fs.getPath().toString(), fs.getLen(), fs
            .isDir(), fs.getModificationTime(), fs.getAccessTime()));
      }
      return retList;
    } catch (IOException e) {
      LOG.info("listStatus IOException", e);
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }
  
  @Override
  public DfsFileStatus getFileStatus(String path) throws FsShellException,
      FsShellFileNotFoundException, TException {
    LOG.info("getFileStatus: path: " + path);
    try {
      FileSystem fs = getFileSystem(path, false);
      FileStatus fi = fs.getFileStatus(new Path(path));

      if (fi != null) {
        fi.makeQualified(fs);
        return new DfsFileStatus(fi.getPath().toString(), fi.getLen(), fi.isDir(),
            fi.getModificationTime(), fi.getAccessTime());
      } else {
        throw new FsShellFileNotFoundException("File does not exist: " + path);
      }
    } catch (FileNotFoundException fnfe) {
      LOG.info("getFileStatus FileNotFoundException", fnfe);
      throw new FsShellFileNotFoundException(fnfe.getMessage());
    } catch (IOException e) {
      LOG.info("getFileStatus IOException", e);
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public boolean exists(String path) throws FsShellException, TException {
    LOG.info("exists: path: " + path);
    try {
      return getFileSystem(path, false).exists(new Path(path));
    } catch (IOException e) {
      LOG.info("exists IOException", e);
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public int getFileCrc(String path) throws FsShellException, TException {
    LOG.info("getFileCrc: path: " + path);
    try {
      return getFileSystem(path, false).getFileCrc(new Path(path));
    } catch (IOException e) {
      LOG.info("getFileCrc IOException", e);
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  private void initThriftServer(int port, int maxQueue) {
    // Setup the Thrift server
    LOG.info("Setting up Thrift server listening port " + port);
    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TTransportFactory transportFactory = new TFramedTransport.Factory();
    TServerTransport  serverTransport;
    FsShellService.Processor<FsShellService.Iface> processor =
        new FsShellService.Processor<FsShellService.Iface>(this);

    ServerSocket serverSocket_;
    try {
      // Make server socket. Use loop-back address to only serve requests
      // from local clients, in order to prevent ambiguity for commands
      // of copyFromLocal() and copyToLocal()
      serverSocket_ = new ServerSocket(port, maxQueue,
          InetAddress.getAllByName(null)[0]);
      // Prevent 2MSL delay problem on server restarts
      serverSocket_.setReuseAddress(true);
    } catch (IOException ioe) {
      LOG.error("Could not create ServerSocket on local address ", ioe);
      return;
    }
    serverTransport = new TServerSocket(serverSocket_, clientTimeout);
    TThreadPoolServer.Args serverArgs =
        new TThreadPoolServer.Args(serverTransport);
    serverArgs.processor(processor).transportFactory(transportFactory)
        .protocolFactory(protocolFactory);
    tserver = new TThreadPoolServer(serverArgs);    
  }
  
  @Override
  public void run() {
    try {
      LOG.info("Starting Thrift server");
      if (tserver == null) {
        LOG.error("Error when starting the server");
        return;
      }
      tserver.serve();
    } catch (Exception e) {
      LOG.error("Thrift server failed", e);
    }
  }

  void init() {
    int port = conf.getInt("fssshellservice.port", 62001);
    int maxQueue = conf.getInt("fssshellservice.max.queue", 1024);
    clientTimeout = conf.getInt("fsshellservice.server.clienttimeout",
        600 * 1000);
    initThriftServer(port, maxQueue);
  }

  FsShellServiceImpl (Configuration conf) {
    init();
  }
  
  
  public static void main(String[] args) {
    FsShellServiceImpl imp = new FsShellServiceImpl(new Configuration());
    Thread t = new Thread(imp);
    t.start();
    while (true) {
      try {
        t.join();
        break;
      } catch (InterruptedException e) {
        t.interrupt();
      }
    }
  }
}
