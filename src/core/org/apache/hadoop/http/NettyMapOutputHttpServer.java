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
package org.apache.hadoop.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Create a Netty server to only answer map output http requests. HttpServer is
 * used to handle all other requests.
 */
public class NettyMapOutputHttpServer {
  /** Netty's backlog parameter. */
  public static final String BOOTSTRAP_BACKLOG_PARAM = "backlog";
  /** The backlog to be used for netty. */
  public static final String BACKLOG_CONF = "mapred.task.tracker.netty.backlog";
  /** The default backlog. */
  public static final int DEFAULT_BACKLOG = 1000;
  /** Maximum thread pool size key */
  public static final String MAXIMUM_THREAD_POOL_SIZE =
    "mapred.task.tracker.netty.maxThreadPoolSize";
  /** Default maximum thread pool size (same as jetty) */
  public static final int DEFAULT_MAXIMUM_THREAD_POOL_SIZE = 254;
  /** Class logger */
  private static final Log LOG =
    LogFactory.getLog(NettyMapOutputHttpServer.class);

  private ChannelFactory channelFactory;
  /** Port that this server runs on */
  private int port;
  /** Accepted channels */
  private final ChannelGroup accepted = new DefaultChannelGroup();
  /** Maximum bind attempts */
  private final int DEFAULT_BIND_ATTEMPT_MAX = 50;
  /** Worker thread pool (if implemented as a ThreadPoolExecutor) */
  private ThreadPoolExecutor workerThreadPool = null;

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   *
   * @param startingPort The port to use on the server (or start
   *        at if binding fails).  If 0, use an let the server pick the port.
   */
  public NettyMapOutputHttpServer(int startingPort) throws IOException {
    this.port = startingPort;
  }

  public synchronized void init(Configuration conf) {
      ThreadFactory bossFactory = new ThreadFactoryBuilder()
        .setNameFormat("ShuffleHandler Netty Boss #%d")
        .build();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setNameFormat("ShuffleHandler Netty Worker #%d")
      .build();

    int maximumPoolSize = conf.getInt(MAXIMUM_THREAD_POOL_SIZE,
                                      DEFAULT_MAXIMUM_THREAD_POOL_SIZE);
    try {
      workerThreadPool =
        (ThreadPoolExecutor) Executors.newCachedThreadPool(workerFactory);
      workerThreadPool.setMaximumPoolSize(maximumPoolSize);
    } catch (ClassCastException e) {
      LOG.warn("Netty worker thread pool is not of type ThreadPoolExecutor", e);
    }
    LOG.info("Netty starting up with a maximum of " + maximumPoolSize +
        " worker threads");
    channelFactory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(bossFactory),
        workerThreadPool, maximumPoolSize);
  }

  public synchronized int start(
    Configuration conf, ChannelPipelineFactory pipelineFactory) {
    ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
    bootstrap.setPipelineFactory(pipelineFactory);
    bootstrap.setOption(
      BOOTSTRAP_BACKLOG_PARAM,
      conf.getInt(BACKLOG_CONF, DEFAULT_BACKLOG));
    // Try to bind to a port.  If the port is 0, netty will select a port.
    int bindAttempt = 0;
    while (bindAttempt < DEFAULT_BIND_ATTEMPT_MAX) {
      try {
        InetSocketAddress address = new InetSocketAddress(port);
        Channel ch = bootstrap.bind(address);
        accepted.add(ch);
        port = ((InetSocketAddress) ch.getLocalAddress()).getPort();
        break;
      } catch (ChannelException e) {
        LOG.warn("start: Likely failed to bind on attempt " +
                 bindAttempt + " to port " + port, e);
        // Only increment the port number when set by the user
        if (port != 0) {
          ++port;
        }
        ++bindAttempt;
      }
    }

    LOG.info(this.getClass() + " is listening on port " + port);
    return port;
  }

  public synchronized void stop() {
    accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
    bootstrap.releaseExternalResources();
  }

  /**
   * Get the worker thread pool for metrics.
   *
   * @return Worker thread pool or null if the thread pool is not a
   *         ThreadPoolExcecutor.
   */
  public ThreadPoolExecutor getWorkerThreadPool() {
    return workerThreadPool;
  }
}
