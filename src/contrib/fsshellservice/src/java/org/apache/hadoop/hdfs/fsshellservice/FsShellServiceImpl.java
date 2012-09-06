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

  private FileSystem getFileSystem(String name) throws IOException,
      URISyntaxException {
    return FileSystem.get(new URI(name), conf);
  }
  
  @Override
  public void copyFromLocal(String src, String dest)
      throws FsShellException, TException {
    LOG.info("copy from local: src: " + src + " dest: " + dest);
    try {
      getFileSystem(dest).copyFromLocalFile(new Path(src), new Path(dest));
    } catch (IOException e) {
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public void copyToLocal(String src, String dest) throws FsShellException,
      TException {
    LOG.info("copy to local: src: " + src + " dest: " + dest);
    try {
      getFileSystem(src).copyToLocalFile(new Path(src), new Path(dest));
    } catch (IOException e) {
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public boolean remove(String path, boolean recursive)
      throws FsShellException, TException {
    LOG.info("remove: src: " + path + " recusive: " + recursive);
    try {
      return getFileSystem(path).delete(new Path(path), recursive);
    } catch (IOException e) {
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }
  
  @Override
  public boolean mkdirs(String f) throws FsShellException, TException {
    LOG.info("mkdir f: " + f);
    try {
      return getFileSystem(f).mkdirs(new Path(f));
    } catch (IOException e) {
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
      return getFileSystem(src).rename(new Path(src), new Path(dest));
    } catch (IOException e) {
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public List<DfsFileStatus> listStatus(String path) throws FsShellException,
      TException {
    LOG.info("listStatus: path: " + path);
    try {
      FileStatus[] fsArray =  getFileSystem(path).listStatus(new Path(path));
      if (fsArray == null) {
        // directory doesn't exist
        throw new FsShellException("The directory doesn't exist.");
      }
      List<DfsFileStatus> retList = new ArrayList<DfsFileStatus>(fsArray.length);
      for (FileStatus fs : fsArray) {
        retList.add(new DfsFileStatus(fs.getPath().toString(), fs.getLen(), fs
            .isDir(), fs.getModificationTime(), fs.getAccessTime()));
      }
      return retList;
    } catch (IOException e) {
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }
  
  @Override
  public DfsFileStatus getFileStatus(String path) throws FsShellException,
      TException {
    LOG.info("getFileStatus: path: " + path);
    try {
      FileSystem fs = getFileSystem(path);
      FileStatus fi = fs.getFileStatus(new Path(path));
      if (fi != null) {
        fi.makeQualified(fs);
        return new DfsFileStatus(fi.getPath().toString(), fi.getLen(), fi.isDir(),
            fi.getModificationTime(), fi.getAccessTime());
      } else {
        throw new FsShellException("File does not exist: " + path);
      }
    } catch (IOException e) {
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  @Override
  public boolean exists(String path) throws FsShellException, TException {
    LOG.info("exists: path: " + path);
    try {
      return getFileSystem(path).exists(new Path(path));
    } catch (IOException e) {
      throw new FsShellException(e.toString());
    } catch (URISyntaxException e) {
      throw new FsShellException(e.toString());
    }
  }

  
  private void initThriftServer(int port) {
    // Setup the Thrift server
    LOG.info("Setting up Thrift server listening port " + port);
    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TTransportFactory transportFactory = new TFramedTransport.Factory();
    TServerTransport  serverTransport;
    FsShellService.Processor<FsShellService.Iface> processor =
        new FsShellService.Processor<FsShellService.Iface>(this);
    try {
      serverTransport = new TServerSocket(port, clientTimeout);
    } catch (TTransportException e) {
      LOG.error("Failed to setup the Thrift server.", e);
      return;
    }
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
      tserver.serve();
    } catch (Exception e) {
      LOG.error("Thrift server failed", e);
    }
  }

  void init() {
    int port = conf.getInt("fssshellservice.port", 62001);
    clientTimeout = conf.getInt("fsshellservice.server.clienttimeout",
        600 * 1000);
    initThriftServer(port);
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
