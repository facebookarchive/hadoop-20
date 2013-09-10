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
package org.apache.hadoop.hdfs.storageservice;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.net.HostAndPort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProxyProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.CreateRequest;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.GetPartialListingRequest;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.PingRequest;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.RequestMetaInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.TClientProxyProtocol;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** A class for tesing NameNode and ClientProxyService call latency */
public class NNLatencyBenchmark implements Tool {
  /** Populate default configs */
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }

  private static final Log LOG = LogFactory.getLog(NNLatencyBenchmark.class);
  /** HDFS root folder to use for testing, will be created and destroyed by testing framework */
  private static final String ROOT = "/NNLatencyBenchmark/";
  /** Number of calls to be executed as a warm up, measured latencies are discarded */
  static int WARMUP_SAMPLES = 50;
  /** Number of calls whose latencies are averaged to form the final result */
  static int MEASURED_SAMPLES = 500;
  /** File handler for results file */
  private final File resultsFile;
  /** ID of a cluster that we use */
  private int clusterId;
  /** Nameservice ID for benchmarks */
  private String nameserviceId;
  private Configuration conf;
  private String proxyHostname;
  private int proxyPortThrift;
  private int proxyPortRPC;

  private DistributedFileSystem fileSystem;
  /** Client -> (Hadoop's RPC old protocol) -> NameNode */
  private ClientProtocol directClientProtocol;
  /** Client -> (Hadoop's RPC new protocol) -> NameNode */
  private ClientProxyProtocol directClientProxyProtocol;

  private ThriftClientManager clientManager;
  /** Client -> (Thrift) -> Proxy -> (Hadoop's RPC new protocol) -> NameNode */
  private TClientProxyProtocol proxyTClientProxyProtocol;
  /** Client -> (Hadoop's RPC new protocol) -> Proxy -> (Hadoop's RPC new protocol) -> NameNode */
  private ClientProxyProtocol proxyClientProxyProtocol;

  private RequestMetaInfo metaInfo;

  public NNLatencyBenchmark() throws IOException {
    resultsFile = File.createTempFile("NNLatencyBenchmark-", ".txt", new File("/tmp/"));
    LOG.info("Results file: " + resultsFile.getAbsolutePath());
  }

  /** Arguments: 1. proxy host name, 2. proxy Thrift port, 3. proxy Hadoop's RPC port */
  private void parseArgs(String[] args) throws IOException {
    if (args == null) {
      return;
    }
    proxyHostname = (args.length > 0) ? args[0] : conf.get(
        StorageServiceConfigKeys.PROXY_THRIFT_HOST_KEY,
        StorageServiceConfigKeys.PROXY_THRIFT_HOST_DEFAULT);
    proxyPortThrift = (args.length > 1) ? Integer.parseInt(args[1]) : conf.getInt(
        StorageServiceConfigKeys.PROXY_THRIFT_PORT_KEY,
        StorageServiceConfigKeys.PROXY_THRIFT_PORT_DEFAULT);
    proxyPortRPC = (args.length > 2) ? Integer.parseInt(args[2]) : conf.getInt(
        StorageServiceConfigKeys.PROXY_RPC_PORT_KEY,
        StorageServiceConfigKeys.PROXY_RPC_PORT_DEFAULT);
  }

  /** Sets up clients before each benchmark */
  private void setUp() throws Exception {
    try {
      fileSystem = (DistributedFileSystem) FileSystem.get(
          StorageServiceConfigKeys.translateToOldSchema(conf, nameserviceId), conf);
      InetSocketAddress nameNodeAddr = fileSystem.getClient().getNameNodeAddr();
      metaInfo = new RequestMetaInfo(clusterId, nameserviceId, RequestMetaInfo.NO_NAMESPACE_ID,
          RequestMetaInfo.NO_APPLICATION_ID, (UnixUserGroupInformation) UserGroupInformation.getUGI(
          this.conf));

      directClientProtocol = RPC.getProxy(ClientProtocol.class, ClientProtocol.versionID,
          nameNodeAddr, conf);

      directClientProxyProtocol = RPC.getProxy(ClientProxyProtocol.class,
          ClientProxyProtocol.versionID, nameNodeAddr, conf);

      clientManager = new ThriftClientManager();
      FramedClientConnector connector = new FramedClientConnector(HostAndPort.fromParts(
          proxyHostname, proxyPortThrift));
      proxyTClientProxyProtocol = clientManager.createClient(connector, TClientProxyProtocol.class)
          .get();

      proxyClientProxyProtocol = RPC.getProxy(ClientProxyProtocol.class,
          ClientProxyProtocol.versionID, new InetSocketAddress(proxyHostname, proxyPortRPC), conf);

      fileSystem.mkdirs(new Path(ROOT));
    } catch (Exception e) {
      tearDown();
      throw e;
    }
  }

  /** Tears down clients after each benchmark */
  private void tearDown() throws Exception {
    try {
      if (fileSystem != null) {
        fileSystem.delete(new Path(ROOT), true, true);
      }
    } finally {
      RPC.stopProxy(proxyClientProxyProtocol);
      IOUtils.cleanup(LOG, proxyTClientProxyProtocol, clientManager, fileSystem);
    }
  }

  ////////////////////////////////////////
  // Benchmarks
  ////////////////////////////////////////

  @Benchmark
  public void createCallLatency(OutputStreamWriter results) throws Exception {
    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      private int i = 0;

      @Override
      public void callTimed() throws Exception {
        proxyTClientProxyProtocol.create(new CreateRequest(metaInfo, ROOT + "newfilea-" + i++,
            metaInfo.getOrigCaller().getUserName(), FsPermission.getDefault(), true, true,
            (short) 1, 1024));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
    results.write("  |  ");

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      private int i = 0;

      @Override
      public void callTimed() throws Exception {
        proxyClientProxyProtocol.create(new CreateRequest(metaInfo, ROOT + "newfileb-" + i++,
            metaInfo.getOrigCaller().getUserName(), FsPermission.getDefault(), true, true,
            (short) 1, 1024));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
    results.write("  |  ");

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      private int i = 0;

      @Override
      public void callTimed() throws Exception {
        directClientProxyProtocol.create(new CreateRequest(metaInfo, ROOT + "newfilec-" + i++,
            metaInfo.getOrigCaller().getUserName(), FsPermission.getDefault(), true, true,
            (short) 1, 1024));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
    results.write("  |  ");

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      private int i = 0;

      @Override
      public void callTimed() throws Exception {
        directClientProtocol.create(ROOT + "newfiled-" + i++, FsPermission.getDefault(),
            metaInfo.getOrigCaller().getUserName(), true, true, (short) 1, 1024);
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
  }

  @Benchmark
  public void getPartialListingLatency(OutputStreamWriter results) throws Exception {
    fileSystem.create(new Path(ROOT + "filea")).close();
    fileSystem.create(new Path(ROOT + "fileb")).close();

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      @Override
      public void callTimed() throws Exception {
        proxyTClientProxyProtocol.getPartialListing(new GetPartialListingRequest(metaInfo, ROOT,
            new byte[0]));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
    results.write("  |  ");

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      @Override
      public void callTimed() throws Exception {
        proxyClientProxyProtocol.getPartialListing(new GetPartialListingRequest(metaInfo, ROOT,
            new byte[0]));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
    results.write("  |  ");

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      @Override
      public void callTimed() throws Exception {
        directClientProxyProtocol.getPartialListing(new GetPartialListingRequest(metaInfo, ROOT,
            new byte[0]));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
    results.write("  |  ");

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      @Override
      public void callTimed() throws Exception {
        directClientProtocol.getPartialListing(ROOT, new byte[0]);
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
  }

  @Benchmark
  public void pingLatency(OutputStreamWriter results) throws Exception {
    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      @Override
      public void callTimed() throws Exception {
        proxyTClientProxyProtocol.ping(new PingRequest(metaInfo));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
    results.write("  |  ");

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      @Override
      public void callTimed() throws Exception {
        proxyClientProxyProtocol.ping(new PingRequest(metaInfo));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
    results.write("  |  ");

    results.write(new MeanDevAccumulator().addResults(new TimedCallable() {
      @Override
      public void callTimed() throws Exception {
        directClientProxyProtocol.ping(new PingRequest(metaInfo));
      }
    }, WARMUP_SAMPLES, MEASURED_SAMPLES).report());
  }

  ////////////////////////////////////////
  // Tool (and main method)
  ////////////////////////////////////////

  @Override
  public int run(String[] args) throws Exception {
    parseArgs(args);
    // Create results file
    FileWriter results = new FileWriter(resultsFile, true);
    // Run all benchmarks
    int failed = 0;
    for (Method method : NNLatencyBenchmark.class.getMethods()) {
      if (method.isAnnotationPresent(Benchmark.class)) {
        results.write(method.getName() + " : ");
        try {
          setUp();
          try {
            method.invoke(this, results);
          } finally {
            tearDown();
          }
        } catch (Throwable e) {
          failed++;
          LOG.error(method.getName() + " failed with: ", e);
        }
        results.write("\n");
      }
    }
    results.close();
    return -failed;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    clusterId = conf.getInt(FSConstants.DFS_CLUSTER_ID, RequestMetaInfo.NO_CLUSTER_ID);
    if (clusterId == RequestMetaInfo.NO_CLUSTER_ID) {
      String msg = "No cluster specified in configuration";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    String[] nameserviceIds = conf.getStrings(FSConstants.DFS_FEDERATION_NAMESERVICES);
    if (nameserviceIds == null || nameserviceIds.length < 1) {
      String msg = "No nameservice ID found";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    this.nameserviceId = nameserviceIds[0];
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static void main(String[] args) {
    try {
      System.exit(ToolRunner.run(new NNLatencyBenchmark(), args));
    } catch (Exception e) {
      LOG.error("Benchmark exited with error: ", e);
      System.exit(-1);
    }
  }

  /**
   * Each method with @Benchmark annotation is assumed to be a separate benchmark.
   * Benchmark driver will create all clients (setUp() method), run annotated call and clean up
   * (tearDown() method) right after.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  public static @interface Benchmark {
  }

  public static class MeanDevAccumulator {
    private int n = 0;
    private double mean = 0D;
    private double acc = 0;

    public void add(double x) {
      n++;
      double delta = x - mean;
      mean += delta / n;
      acc += delta * (x - mean);
    }

    public MeanDevAccumulator addResults(Callable<Double> experiment, int skip, int times) throws
        Exception {
      while (--skip >= 0) {
        experiment.call();
      }
      while (--times >= 0) {
        double duration = experiment.call();
        add(duration);
        TimeUnit.MILLISECONDS.sleep(Math.max(5, Math.round(20D - duration)));
      }
      return this;
    }

    public double getVariance() {
      return acc / (n - 1);
    }

    public double getStdDev() {
      return Math.sqrt(getVariance());
    }

    public double getMean() {
      return mean;
    }

    public String report() {
      return getMean() + " (\u00B1 " + getStdDev() + ")";
    }
  }

  public static abstract class TimedCallable implements Callable<Double> {
    protected abstract void callTimed() throws Exception;

    @Override
    public Double call() throws Exception {
      long start = System.nanoTime();
      callTimed();
      return ((double) (System.nanoTime() - start)) / 1e6;
    }
  }
}
