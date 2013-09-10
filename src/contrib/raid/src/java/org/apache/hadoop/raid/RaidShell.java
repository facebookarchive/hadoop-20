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

package org.apache.hadoop.raid;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import javax.security.auth.login.LoginException;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RemoteIterator;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.hadoop.raid.DistRaid.EncodingCandidate;
import org.apache.hadoop.raid.RaidUtils.RaidInfo;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.raid.protocol.RaidProtocol;
import org.apache.hadoop.raid.tools.FastFileCheck;
import org.apache.hadoop.raid.tools.ParityVerifier;
import org.apache.hadoop.raid.tools.RSBenchmark;
import org.json.JSONException;
import org.xml.sax.SAXException;
import java.util.concurrent.atomic.*;

/**
 * A {@link RaidShell} that allows browsing configured raid policies.
 */
public class RaidShell extends Configured implements Tool {
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("raid-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("raid-site.xml");
  }
  public static final Log LOG = LogFactory.getLog( "org.apache.hadoop.RaidShell");
  public RaidProtocol raidnode;
  RaidProtocol rpcRaidnode;
  private UnixUserGroupInformation ugi;
  volatile boolean clientRunning = true;
  private Configuration conf;
  AtomicInteger corruptCounter = new AtomicInteger();
  AtomicLong numNonRaidedMissingBlks = new AtomicLong();
  Map<String, AtomicLongArray> numStrpMissingBlksMap = 
            new HashMap<String, AtomicLongArray>(Codec.getCodecs().size());
  private final PrintStream out;

  final static private String DistRaidCommand = "-distRaid";
  final static private String FILE_CHECK_CMD = "-fileCheck";
  final SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
  
  /**
   * Start RaidShell.
   * <p>
   * The RaidShell connects to the specified RaidNode and performs basic
   * configuration options.
   * @throws IOException
   */
  public RaidShell(Configuration conf) throws IOException {
    this(conf, System.out);
  }
  
  public RaidShell(Configuration conf, PrintStream out) throws IOException {
    super(conf);
    this.conf = conf;
    this.out = out;
    for (Codec codec : Codec.getCodecs()) {
      numStrpMissingBlksMap.put(codec.id, 
                new AtomicLongArray(codec.parityLength + codec.stripeLength));
    }
  }

  void initializeRpc(Configuration conf, InetSocketAddress address) throws IOException {
    try {
      this.ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }

    this.rpcRaidnode = createRPCRaidnode(address, conf, ugi);
    this.raidnode = createRaidnode(rpcRaidnode);
  }

  void initializeLocal(Configuration conf) throws IOException {
    try {
      this.ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }
  }

  public static RaidProtocol createRaidnode(Configuration conf) throws IOException {
    return createRaidnode(RaidNode.getAddress(conf), conf);
  }

  public static RaidProtocol createRaidnode(InetSocketAddress raidNodeAddr,
      Configuration conf) throws IOException {
    try {
      return createRaidnode(createRPCRaidnode(raidNodeAddr, conf,
        UnixUserGroupInformation.login(conf, true)));
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }
  }

  public static RaidProtocol createRPCRaidnode(InetSocketAddress raidNodeAddr,
      Configuration conf, UnixUserGroupInformation ugi)
    throws IOException {
    LOG.info("RaidShell connecting to " + raidNodeAddr);
    return (RaidProtocol)RPC.getProxy(RaidProtocol.class,
        RaidProtocol.versionID, raidNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, RaidProtocol.class));
  }

  public static RaidProtocol createRaidnode(RaidProtocol rpcRaidnode)
    throws IOException {
    Map<Class<? extends Exception>,RetryPolicy> remoteExceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class,
        RetryPolicies.retryByRemoteException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();

    methodNameToPolicyMap.put("create", methodPolicy);

    return (RaidProtocol) RetryProxy.create(RaidProtocol.class,
        rpcRaidnode, methodNameToPolicyMap);
  }

  /**
   * Close the connection to the raidNode.
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      clientRunning = false;
      RPC.stopProxy(rpcRaidnode);
    }
  }

  /**
   * Displays format of commands.
   */
  private static void printUsage(String cmd) {
    if ("-showConfig".equals(cmd)) {
      System.err.println("Usage: java RaidShell" + 
                         " [-showConfig]"); 
    } else if ("-recover".equals(cmd)) {
      System.err.println("Usage: java RaidShell" +
                         " [-recover srcPath1 corruptOffset]");
    } else if ("-recoverBlocks".equals(cmd)) {
      System.err.println("Usage: java RaidShell" +
                         " [-recoverBlocks path1 path2...]");
    } else if ("-raidFile".equals(cmd)) {
      System.err.println(
        "Usage: java RaidShell -raidFile <path-to-file> <path-to-raidDir> <XOR|RS>");
    } else if (DistRaidCommand.equals(cmd)) {
      System.err.println("Usage: java RaidShell " + DistRaidCommand 
                       + " <raid_policy_name> <path1> ... <pathn>");
    } else if (FILE_CHECK_CMD.equals(cmd)) {
      System.err.println(
          "Usage: java RaidShell -fileCheck [-filesPerJob N] [-sourceOnly] <path-to-file>");
    } else if ("-fsck".equals(cmd)) {
      System.err.println("Usage: java RaidShell [-fsck [path [-threads numthreads] [-count]] [-retNumStrpsMissingBlks] [-listrecoverablefiles]]");
    } else if ("-usefulHar".equals(cmd)) {
      System.err.println("Usage: java RaidShell [-usefulHar <XOR|RS> [path-to-raid-har]]");
    } else if ("-checkFile".equals(cmd)) {
      System.err.println("Usage: java RaidShell [-checkFile path]");
    } else if ("-purgeParity".equals(cmd)) {
      System.err.println("Usage: java RaidShell -purgeParity path <XOR|RS>");
    } else if ("-checkParity".equals(cmd)) {
      System.err.println("Usage: java RaidShell [-checkParity path]");
    } else if ("-findMissingParityFiles".equals(cmd)) { 
      System.err.println("Usage: java RaidShell -findMissingParityFiles [-r] rootPath");
    } else if ("-verifyFile".equals(cmd)) {
      System.err.println("Usage: java RaidShell -verifyFile rootPath");
    } else if ("-verifyParity".equals(cmd)) {
      System.err.println("Usage: java RaidShell -verifyParity -repl expectRepl" +
                         " [-restore] parityRootPath");
    } else if ("-rs_benchmark".equals(cmd)) {
      System.err.println("Usage: java RaidShell -rs_benchmark -verify -native" + 
                         " [-encode E] [-seed S] [-dpos P] [-dlen L] [-elen L]");
    } else if ("-estimateSaving".equals(cmd)) {
      System.err.println("Usage: java RaidShell -estimateSaving xor:/x/y/xor,rs:/x/y/rs,dir-xor:/x/y/dir_xor " +
      		"[-threads numthreads] [-debug]");
    } else if ("-smoketest".equals(cmd)) {
      System.err.println("Usage: java RaidShell -smoketest");
    } else {
      System.err.println("Usage: java RaidShell");
      System.err.println("           [-showConfig ]");
      System.err.println("           [-help [cmd]]");
      System.err.println("           [-recover srcPath1 corruptOffset]");
      System.err.println("           [-recoverBlocks path1 path2...]");
      System.err.println("           [-raidFile <path-to-file> <path-to-raidDir> <XOR|RS>");
      System.err.println("           [" + DistRaidCommand 
                                        + " <raid_policy_name> <path1> ... <pathn>]");
      System.err.println("           [-fsck [path [-threads numthreads] [-count]] [-retNumStrpsMissingBlks] [-listrecoverablefiles]]");
      System.err.println("           [" + FILE_CHECK_CMD
                                        + " [-filesPerJob N] [-sourceOnly] <path-to-file>");
      System.err.println("           [-usefulHar <XOR|RS> [path-to-raid-har]]");
      System.err.println("           [-checkFile path]");
      System.err.println("           [-purgeParity path <XOR|RS>]");
      System.err.println("           [-findMissingParityFiles [-r] rootPath");
      System.err.println("           [-verifyParity -repl expectRepl " +
                         " [-restore] parityRootPath");
      System.err.println("           [-checkParity path]");
      System.err.println("           [-verifyFile rootPath]");
      System.err.println("           [-rs_benchmark -verify -native" + 
                         " [-encode E] [-seed S] [-dpos P] [-dlen L] [-elen L]");
      System.err.println("           [-estimateSaving xor:/x/y/xor,rs:/x/y/rs,dir-xor:/x/y/dir_xor " +
      		"[-threads numthreads] [-debug]");
      System.err.println("           [-smoketest]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];
    //
    // verify that we have enough command line parameters
    //
    if ("-showConfig".equals(cmd)) {
      if (argv.length < 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-recover".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-fsck".equals(cmd)) {
      if ((argv.length < 1) || (argv.length > 5)) {
        printUsage(cmd);
        return exitCode;
      }
    } else if (DistRaidCommand.equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    }

    try {
      if ("-showConfig".equals(cmd)) {
        initializeRpc(conf, RaidNode.getAddress(conf));
        exitCode = showConfig(cmd, argv, i);
      } else if ("-recover".equals(cmd)) {
        initializeRpc(conf, RaidNode.getAddress(conf));
        exitCode = recoverAndPrint(cmd, argv, i);
      } else if ("-recoverBlocks".equals(cmd)) {
        initializeLocal(conf);
        recoverBlocks(argv, i);
        exitCode = 0;
      } else if ("-raidFile".equals(cmd)) {
        initializeLocal(conf);
        raidFile(argv, i);
        exitCode = 0;
      } else if (DistRaidCommand.equals(cmd)) {
        initializeLocal(conf);
        distRaid(argv, i);
        exitCode = 0;
      } else if (FILE_CHECK_CMD.equals(cmd)) {
        initializeLocal(conf);
        fileCheck(argv, i);
        exitCode = 0;
      } else if ("-fsck".equals(cmd)) {
        fsck(cmd, argv, i);
        exitCode = 0;
      } else if ("-usefulHar".equals(cmd)) {
        usefulHar(argv, i);
        exitCode = 0;
      } else if ("-checkFile".equals(cmd)) {
        checkFile(cmd, argv, i);
        exitCode = 0;
      } else if ("-purgeParity".equals(cmd)) {
        purgeParity(cmd, argv, i);
        exitCode = 0;
      } else if ("-checkParity".equals(cmd)) {
        checkParity(cmd, argv, i);
        exitCode = 0;
      } else if ("-findMissingParityFiles".equals(cmd)) {
        findMissingParityFiles(argv, i);
        exitCode = 0;
      } else if ("-verifyParity".equals(cmd)) {
        verifyParity(argv, i);
        exitCode = 0;
      } else if ("-verifyFile".equals(cmd)) {
        verifyFile(argv, i);
      } else if ("-rs_benchmark".equals(cmd)) {
        rsBenchmark(argv, i);
      } else if ("-estimateSaving".equals(cmd)) {
        estimateSaving(argv, i);
      } else if ("-smoketest".equals(cmd)) {
        initializeRpc(conf, RaidNode.getAddress(conf));
        boolean succeed = startSmokeTest();
        if (succeed) {
          System.err.println("Raid Smoke Test Succeeded!");
          exitCode = 0;
        } else {
          System.err.println("Raid Smoke Test Failed!");
          exitCode = -1;
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge);
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by raidnode server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " +
                           content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " +
                           ex.getLocalizedMessage());
      }
    } catch (Exception e) {
      exitCode = -1;
      LOG.error(cmd.substring(1) + ": ", e);
    }
    return exitCode;
  }

  /**
   * Find the files that do not have a corresponding parity file and have replication
   * factor less that 3
   * args[] contains the root where we need to check
   */
  private void findMissingParityFiles(String[] args, int startIndex) {
    boolean restoreReplication = false;
    Path root = null;
    for (int i = startIndex; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("-r")) {
        restoreReplication = true;
      } else {
        root = new Path(arg);
      }
    }
    if (root == null) {
      throw new IllegalArgumentException("Too few arguments");
    }
    try {
      FileSystem fs = root.getFileSystem(conf);
      // Make sure default uri is the same as root  
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, fs.getUri().toString());
      MissingParityFiles mParFiles = new MissingParityFiles(conf, restoreReplication);
      mParFiles.findMissingParityFiles(root, System.out);
    } catch (IOException ex) {
      System.err.println("findMissingParityFiles: " + ex);
    }
  }
  
  private long estimateSaving(final Codec codec,
      final List<Path> files, final int targetReplication, final int numThreads,
      final boolean isDebug) throws IOException { 
    final AtomicLong totalSavingSize = new AtomicLong(0);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    LOG.info("Processing " + files.size() + " files/dirs for " + codec.id +
        " in " + numThreads + " threads");
    if (isDebug) {
      System.out.println("oldDiskSize | oldParitySize | newDiskSize | newParitySize" +
                         "| savingSize | totalSavingSize | path ");
    }
    final AtomicInteger finishNum = new AtomicInteger(0);
    for (int i = 0; i < numThreads; i++) {
      final int startIdx = i; 
      Runnable work = new Runnable() {
        public void run() {
          try {
            for (int idx = startIdx; idx < files.size(); idx += numThreads) {
              try {
                Path p = files.get(idx);
                FileSystem fs = FileSystem.get(conf);
                p = fs.makeQualified(p);
                FileStatus stat = null;
                try {
                  stat = fs.getFileStatus(p);
                } catch (FileNotFoundException e) {
                  LOG.warn("Path " + p  + " does not exist", e);
                }
                if (stat == null) {
                  continue;
                }
                short repl = 0;
                List<FileStatus> lfs = null;
                if (codec.isDirRaid) {
                  if (!stat.isDir()) {
                    continue;
                  }
                  lfs = RaidNode.listDirectoryRaidFileStatus(conf, fs, p);
                  if (lfs == null) {
                    continue;
                  }
                  repl = DirectoryStripeReader.getReplication(lfs);
                } else {
                  repl = stat.getReplication();
                }
                 
                // if should not raid, will not put the file into the write list.
                if (!RaidNode.shouldRaid(conf, fs, stat, codec, lfs)) {
                  LOG.info("Should not raid file: " + p);
                  continue;
                }
                // check the replication.
                boolean add = false;
                if (repl > targetReplication) {
                  add = true;
                } else if (repl == targetReplication &&
                           !ParityFilePair.parityExists(stat, codec, conf)) {
                  add = true;
                }
                if (add) {
                  long oldDiskSize = 0L;
                  long newDiskSize = 0L;
                  long numBlocks = 0L;
                  long parityBlockSize = 0L;
                  if (codec.isDirRaid) {
                    for (FileStatus fsStat: lfs) {
                      oldDiskSize += fsStat.getLen() * (fsStat.getReplication());
                      newDiskSize += fsStat.getLen() * targetReplication;
                    }
                    numBlocks = DirectoryStripeReader.getBlockNum(lfs);
                    parityBlockSize =
                        DirectoryStripeReader.getParityBlockSize(conf, lfs); 
                  } else {
                    oldDiskSize = stat.getLen() * stat.getReplication();
                    newDiskSize = stat.getLen() * targetReplication; 
                    numBlocks = RaidNode.getNumBlocks(stat);
                    parityBlockSize = stat.getBlockSize();
                  }
                  
                  long numStripes = RaidNode.numStripes(numBlocks,
                      codec.stripeLength); 
                  long newParitySize = numStripes * codec.parityLength * 
                      parityBlockSize * targetReplication;
                  long oldParitySize = 0L;
                  for (Codec other: Codec.getCodecs()) {
                    if (other.priority < codec.priority) {
                      Path parityPath = new Path(other.parityDirectory,
                          RaidNode.makeRelative(stat.getPath()));
                      long logicalSize = 0;
                      try {
                        logicalSize = 
                            fs.getContentSummary(parityPath).getSpaceConsumed();
                      } catch (IOException ioe) {
                        // doesn't exist
                        continue;
                      }
                      oldParitySize += logicalSize;
                    }
                  }
                  long savingSize = oldDiskSize + oldParitySize - newDiskSize -
                      newParitySize;
                  totalSavingSize.addAndGet(savingSize);
                  if (isDebug) {
                    System.out.println(oldDiskSize + " " + oldParitySize + " " +
                        newDiskSize + " " + newParitySize + " " + savingSize + 
                        " " + totalSavingSize.get() + " " + stat.getPath());
                  }
                }
              } catch (IOException ioe) {
                LOG.warn("Get IOException" , ioe);
              }
            }
          } finally {
            finishNum.incrementAndGet();
          }
        }
      };
      if (executor != null) {
        executor.execute(work);
      }
    }
    if (executor != null) {
      try {
        while (finishNum.get() < numThreads) {
          try {
            Thread.sleep(2000);
          } catch (InterruptedException ie) {
            LOG.warn("EstimateSaving get exception ", ie);
            throw new IOException(ie);
          }
        }
      } finally {
        executor.shutdown(); // Waits for submitted tasks to finish.
      }
    }
    return totalSavingSize.get();
  }
  
  private ArrayList<Path> readFileList(String fileListPath)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    ArrayList<Path> paths = new ArrayList<Path>();
    InputStream in = fs.open(new Path(fileListPath));
    BufferedReader input = new BufferedReader(
        new InputStreamReader(in));
    String l;
    try {
      while ((l = input.readLine()) != null){
        paths.add(new Path(l));
      }
      return paths;
    } finally {
      input.close();
    }
  }
  
  private void estimateSaving(String[] args, int startIndex)
      throws Exception {
    String mappings = args[startIndex++];
    HashMap<String, String> codecFileListMap = new HashMap<String, String>();
    for (String mapping: mappings.split(",")) {
      String[] parts = mapping.split(":");
      codecFileListMap.put(parts[0], parts[1]);
    }
    int numThreads = 10;
    boolean isDebug = false;
    while (startIndex < args.length) {
      if (args[startIndex].equals("-threads")) {
        numThreads = Integer.parseInt(args[++startIndex]);
      } else if (args[startIndex].equals("-debug")) {
        isDebug = true;
      } else {
        throw new IOException("Can't recognize " + args[startIndex]);
      }
      startIndex++;
    }
    long totalSavingSize = 0;
    ArrayList<PolicyInfo> allPolicies = new ArrayList<PolicyInfo>();
    ArrayList<PolicyInfo> allPoliciesWithSrcPath = new ArrayList<PolicyInfo>();
    ConfigManager configMgr = new ConfigManager(conf);
    for (PolicyInfo info : configMgr.getAllPolicies()) {
      allPolicies.add(info);
      if (info.getSrcPath() != null) {
        allPoliciesWithSrcPath.add(info);
      }
    }
    for (PolicyInfo info: allPolicies) {
      if (info.getFileListPath() == null || !info.getShouldRaid()) {
        continue;
      }
      Codec c = Codec.getCodec(info.getCodecId());
      // Assume each codec has only one fileList path
      String filePath = codecFileListMap.get(c.id);
      if (filePath == null) {
        continue;
      }
      List<Path> files = readFileList(filePath); 
      totalSavingSize += estimateSaving(c, files,
          Integer.parseInt(info.getProperty("targetReplication")),
          numThreads, isDebug);
    }
    LOG.info("Total Saving Bytes is:" + totalSavingSize);
  }
  
  /**
   * search each parity and verify the source files have
   * the expected replication
   */
  private void verifyParity(String[] args, int startIndex) {
    boolean restoreReplication = false;
    int repl = -1;
    Path root = null;
    for (int i = startIndex; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("-restore")) {
        restoreReplication = true;
      } else if (arg.equals("-repl")){
        i++;
        if (i >= args.length) {
          throw new IllegalArgumentException("Missing repl after -r option");
        }
        repl = Integer.parseInt(args[i]);
      } else {
        root = new Path(arg);
      }
    }
    if (root == null) {
      throw new IllegalArgumentException("Too few arguments");
    }
    if (repl == -1) {
      throw new IllegalArgumentException("Need to specify -r option");
    }
    if (repl < 1 || repl > 3) {
      throw new IllegalArgumentException("repl could only in the range [1, 3]");
    }
    Codec matched = null;
    String rootPath = root.toUri().getPath();
    if (!rootPath.endsWith(Path.SEPARATOR)) {
      rootPath += Path.SEPARATOR;
    }
    for (Codec code : Codec.getCodecs()) {
      if (rootPath.startsWith(code.getParityPrefix())) {
        matched = code;
        break;
      }
    }
    if (matched == null) {
      throw new IllegalArgumentException(
          "root needs to starts with parity dirs");
    }
    try {
      FileSystem fs = root.getFileSystem(conf);
      // Make sure default uri is the same as root  
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, fs.getUri().toString());
      ParityVerifier pv = new ParityVerifier(conf, restoreReplication, 
          repl, matched);
      pv.verifyParities(root, System.out);
    } catch (IOException ex) {
      System.err.println("findMissingParityFiles: " + ex);
    }
  }
  
  /**
   * Scan file and verify the checksums match the checksum store if have
   * args[] contains the root where we need to check
   */
  private void verifyFile(String[] args, int startIndex) {
    Path root = null;
    if (args.length <= startIndex) {
      throw new IllegalArgumentException("too few arguments");
    }
    String arg = args[startIndex];
    root = new Path(arg);
    try {
      FileSystem fs = root.getFileSystem(conf);
      // Make sure default uri is the same as root  
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, fs.getUri().toString());
      FileVerifier fv = new FileVerifier(conf);
      fv.verifyFile(root, System.out);
    } catch (IOException ex) {
      System.err.println("verifyFile: " + ex);
    }
  }
  
  private void rsBenchmark(String[] args, int startIndex) {
    boolean verify = false;
    boolean hasSeed = false;
    boolean useNative = false;
    StringBuilder encodeMethod = new StringBuilder("rs");
    long seed = 0;
    int dpos = 0;
    int dlen = 0;
    int elen = RSBenchmark.DEFAULT_DATALEN;
    for (int idx = startIndex; idx < args.length; idx++) {
      String option = args[idx];
      if (option.equals("-verify")) {
        verify = true;
      } else if (option.equals("-seed")) {
        hasSeed = true;
        seed = Long.parseLong(args[++idx]);
      } else if (option.equals("-encode")) {
        encodeMethod.setLength(0);
        encodeMethod.append((args[++idx]));
      } else if (option.equals("-dpos")) {
        dpos = Integer.parseInt(args[++idx]);
      } else if (option.equals("-dlen")) {
        dlen = Integer.parseInt(args[++idx]);
      } else if (option.equals("-elen")) {
        elen = Integer.parseInt(args[++idx]);
      } else if (option.equals("-native")) {
        useNative = true;
      }
    }
    if (dlen == 0) {
      dlen = elen;
    }
    RSBenchmark rsBen = hasSeed?
        new RSBenchmark(
          verify,
          encodeMethod.toString(),
          seed,
          dpos,
          dlen,
          elen,
          useNative):
        new RSBenchmark(
            verify,
            encodeMethod.toString(),
            dpos,
            dlen,
            elen,
            useNative);
    rsBen.run();
  }

  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int showConfig(String cmd, String argv[], int startindex)
      throws IOException {
    int exitCode = 0;
    PolicyInfo[] all = raidnode.getAllPolicies();
    for (PolicyInfo p: all) {
      out.println(p);
    }
    return exitCode;
  }
  
  private boolean startSmokeTest() throws Exception {
    return raidnode.startSmokeTest();
  }

  /**
   * Recovers the specified path from the parity file
   */
  public Path[] recover(String cmd, String argv[], int startindex)
    throws IOException {
    Path[] paths = new Path[(argv.length - startindex) / 2];
    int j = 0;
    for (int i = startindex; i < argv.length; i = i + 2) {
      String path = argv[i];
      long corruptOffset = Long.parseLong(argv[i+1]);
      LOG.info("RaidShell recoverFile for " + path + " corruptOffset " + corruptOffset);
      Path recovered = new Path("/tmp/recovered." + System.currentTimeMillis());
      FileSystem fs = recovered.getFileSystem(conf);
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      Configuration raidConf = new Configuration(conf);
      raidConf.set("fs.hdfs.impl",
                     "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
      raidConf.set("fs.raid.underlyingfs.impl",
                     "org.apache.hadoop.hdfs.DistributedFileSystem");
      raidConf.setBoolean("fs.hdfs.impl.disable.cache", true);
      java.net.URI dfsUri = dfs.getUri();
      FileSystem raidFs = FileSystem.get(dfsUri, raidConf);
      FileUtil.copy(raidFs, new Path(path), fs, recovered, false, conf);

      paths[j] = recovered;
      LOG.info("Raidshell created recovery file " + paths[j]);
      j++;
    }
    return paths;
  }

  public int recoverAndPrint(String cmd, String argv[], int startindex)
    throws IOException {
    int exitCode = 0;
    for (Path p : recover(cmd,argv,startindex)) {
      out.println(p);
    }
    return exitCode;
  }

  public void recoverBlocks(String[] args, int startIndex)
    throws IOException, InterruptedException {
    LOG.info("Recovering blocks for " + (args.length - startIndex) + " files");
    BlockReconstructor.CorruptBlockReconstructor fixer = new BlockReconstructor.CorruptBlockReconstructor(conf);
    for (int i = startIndex; i < args.length; i++) {
      String path = args[i];
      fixer.reconstructFile(new Path(path), null);
    }
  }
  
  private void fileCheck(String[] args, int startIndex) 
      throws IOException, InterruptedException {
    FastFileCheck checker = new FastFileCheck(conf);
    checker.startFileCheck(args, startIndex, conf);
  }

  /**
   * Submit a map/reduce job to raid the input paths
   * @param args all input parameters
   * @param startIndex staring index of arguments: policy_name path1, ..., pathn
   * @return 0 if successful
   * @throws IOException if any error occurs
   * @throws ParserConfigurationException 
   * @throws ClassNotFoundException 
   * @throws RaidConfigurationException 
   * @throws SAXException 
   */
  private int distRaid(String[] args, int startIndex) throws IOException,
    SAXException, RaidConfigurationException,
    ClassNotFoundException, ParserConfigurationException, JSONException {
    // find the matched raid policy
    String policyName = args[startIndex++];
    ConfigManager configManager = new ConfigManager(conf);
    PolicyInfo policy = configManager.getPolicy(policyName);
    if (policy == null) {
      System.err.println ("Invalid policy: " + policyName);
      return -1;
    }
    Codec codec = Codec.getCodec(policy.getCodecId());
    if (codec == null) {
      System.err.println("Policy " + policyName
          + " with invalid codec " + policy.getCodecId());
    }

    // find the matched paths to raid
    FileSystem fs = FileSystem.get(conf);
    List<FileStatus> pathsToRaid = new ArrayList<FileStatus>();
    List<Path> policySrcPaths = policy.getSrcPathExpanded();
    for (int i = startIndex; i< args.length; i++) {
      boolean invalidPathToRaid = true;
      Path pathToRaid = new Path(args[i]).makeQualified(fs);
      String pathToRaidStr = pathToRaid.toString();
      if (!pathToRaidStr.endsWith(Path.SEPARATOR)) {
        pathToRaidStr = pathToRaidStr.concat(Path.SEPARATOR);
      }
      for (Path srcPath : policySrcPaths) {
        String srcStr = srcPath.toString();
        if (!srcStr.endsWith(Path.SEPARATOR)) {
          srcStr = srcStr.concat(Path.SEPARATOR);
        }
        if (pathToRaidStr.startsWith(srcStr)) {
          if (codec.isDirRaid) {
            FileUtil.listStatusForLeafDir(
                fs, fs.getFileStatus(pathToRaid), pathsToRaid);
          } else {
            FileUtil.listStatusHelper(fs, pathToRaid, 
                Integer.MAX_VALUE, pathsToRaid);
          }
          invalidPathToRaid = false;
          break;
        }
      }
      if (invalidPathToRaid) {
        System.err.println("Path " + pathToRaidStr + 
          " does not support by the given policy " + policyName);
      }
    }
    
    // Check if files are valid
    List<FileStatus> validPaths = new ArrayList<FileStatus>();
    List<PolicyInfo> policyInfos = new ArrayList<PolicyInfo>(1);
    policyInfos.add(policy);
    RaidState.Checker checker = new RaidState.Checker(
        policyInfos, conf);
    long now = System.currentTimeMillis();
    for (FileStatus fileStatus : pathsToRaid) {
      FileStatus[] dirStats = null;
      if (codec.isDirRaid) {
        dirStats = fs.listStatus(fileStatus.getPath());
      }
      RaidState stat = checker.check(
          policy, fileStatus, now, false, 
          dirStats == null ? null : Arrays.asList(dirStats));
      if (stat == RaidState.NOT_RAIDED_BUT_SHOULD) {
        validPaths.add(fileStatus);
      } else {
        System.err.println("Path " + fileStatus.getPath() + 
            " is not qualified for raiding: " + stat);
      }
    }
    if (validPaths.isEmpty()) {
      System.err.println("No file can be raided");
      return 0;
    }
    DistRaid dr = new DistRaid(conf);
    //add paths for distributed raiding
    List<EncodingCandidate> validEC =
        RaidNode.splitPaths(conf, Codec.getCodec(policy.getCodecId()), validPaths);
    dr.addRaidPaths(policy, validEC);
    
    if (dr.startDistRaid()) {
      System.out.println("Job started: " + dr.getJobTrackingURL());
      System.out.print("Job in progress ");
      while (!dr.checkComplete()) {
        try {
          System.out.print(".");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Got interrupted.");
        }
      }
      if (dr.successful()) {
        System.out.println("/nFiles are successfully raided.");
        return 0;
      } else {
        System.err.println("/nRaid job failed.");
        return -1;
      }
    }
    return -1;
  }
  
  public void raidFile(String[] args, int startIndex) throws IOException {
    Path file = new Path(args[startIndex]);
    Path destPath = new Path(args[startIndex + 1]);
    Codec codec = Codec.getCodec(args[startIndex + 2]);
    LOG.info("Raiding file " + file + " to " + destPath + " using " + codec);
    FileSystem fs = destPath.getFileSystem(conf);
    FileStatus stat = fs.getFileStatus(file);
    boolean doSimulate = false;
    int targetRepl = conf.getInt("raidshell.raidfile.targetrepl",
      stat.getReplication());
    int metaRepl = conf.getInt("raidshell.raidfile.metarepl", 2);
    List<EncodingCandidate> lec = RaidNode.splitPaths(conf, codec, stat);
    for (EncodingCandidate ec : lec) {
      RaidNode.doRaid(conf, ec, destPath, codec, new RaidNode.Statistics(),
          RaidUtils.NULL_PROGRESSABLE, doSimulate, targetRepl, metaRepl);
    }
  }
  
  public static int collectNumCorruptBlocksInFile(final DistributedFileSystem dfs,
                                    final Path filePath) throws IOException {
    FileStatus stat = dfs.getFileStatus(filePath);
    BlockLocation[] blocks = 
        dfs.getFileBlockLocations(stat, 0, stat.getLen());
    
    int count = 0;
    for (BlockLocation block : blocks) {
      if (RaidShell.isBlockCorrupt(block)) {
        count ++;
        if (LOG.isDebugEnabled()) {
          LOG.debug("file " + filePath.toString() + " corrupt in block " + 
                   block);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("file " + filePath.toString() + " OK in block " + 
                   block);
        }
      }
    }
    return count;
  }
  
  

  /**
   * checks whether a file has more than the allowable number of
   * corrupt blocks and must therefore be considered corrupt
   */
  
  protected boolean isFileCorrupt(final DistributedFileSystem dfs, 
      final FileStatus fileStat) 
    throws IOException {
    return isFileCorrupt(dfs, fileStat, false, conf,
        this.numNonRaidedMissingBlks, this.numStrpMissingBlksMap); 
  }
  
  protected boolean isFileCorrupt(final DistributedFileSystem dfs, 
      final FileStatus fileStat, boolean cntMissingBlksPerStrp) 
    throws IOException {
    return isFileCorrupt(dfs, fileStat, cntMissingBlksPerStrp, conf,
        this.numNonRaidedMissingBlks, this.numStrpMissingBlksMap); 
  }
  
  /**
   * 
   * @param dfs
   * @param filePath
   * @param cntMissingBlksPerStrp
   * @param numNonRaidedMissingBlks
   * @param numStrpMissingBlksMap
   * @return
   * @throws IOException
   */
  public static boolean isFileCorrupt(final DistributedFileSystem dfs, 
                                final FileStatus fileStat, 
                                final boolean cntMissingBlksPerStrp,
                                final Configuration conf, 
                                AtomicLong numNonRaidedMissingBlks,
                                Map<String, AtomicLongArray> numStrpMissingBlksMap) 
    throws IOException {
    if (fileStat == null) {
      return false;
    }
    Path filePath = fileStat.getPath();
    try {
      // corruptBlocksPerStripe: 
      // map stripe # -> # of corrupt blocks in that stripe (data + parity)
      HashMap<Integer, Integer> corruptBlocksPerStripe =
        new LinkedHashMap<Integer, Integer>();
      boolean fileCorrupt = false;
      // Har checking requires one more RPC to namenode per file
      // skip it for performance. 
      RaidInfo raidInfo = RaidUtils.getFileRaidInfo(fileStat, conf, true);
      if (raidInfo.codec == null) {
        raidInfo = RaidUtils.getFileRaidInfo(fileStat, conf, false);
      }
      if (raidInfo.codec == null) {
        // Couldn't find out the parity file, so the file is corrupt
        int count = collectNumCorruptBlocksInFile(dfs, filePath);
        if (cntMissingBlksPerStrp && numNonRaidedMissingBlks != null) {
          numNonRaidedMissingBlks.addAndGet(count);
        }
        return true;
      }

      if (raidInfo.codec.isDirRaid) {
        RaidUtils.collectDirectoryCorruptBlocksInStripe(conf, dfs, raidInfo, 
            fileStat, corruptBlocksPerStripe);
      } else {
        RaidUtils.collectFileCorruptBlocksInStripe(dfs, raidInfo, fileStat,
            corruptBlocksPerStripe);
      }

      final int maxCorruptBlocksPerStripe = raidInfo.parityBlocksPerStripe;

      for (Integer corruptBlocksInStripe: corruptBlocksPerStripe.values()) {
        if (corruptBlocksInStripe == null) {
          continue;
        }
        //detect if the file has any stripes which cannot be fixed by Raid
        if (LOG.isDebugEnabled()) {
          LOG.debug("file " + filePath.toString() +
                    " has corrupt blocks per Stripe value " +
                    corruptBlocksInStripe);
        }
        if (!fileCorrupt) {
          if (corruptBlocksInStripe > maxCorruptBlocksPerStripe) {
            fileCorrupt = true;
          }
        }
        if(cntMissingBlksPerStrp && numStrpMissingBlksMap != null) {
          numStrpMissingBlksMap.get(raidInfo.codec.id).incrementAndGet(
              corruptBlocksInStripe-1);
        }
      }
      return fileCorrupt;
    } catch (SocketException e) {
      // Re-throw network-related exceptions.
      throw e;
    } catch (SocketTimeoutException e) {
      throw e;
    } catch (IOException e) {
      // re-throw local exceptions.
      if (e.getCause() != null &&
          !(e.getCause() instanceof RemoteException)) {
        throw e;
      }
      
      LOG.error("While trying to check isFileCorrupt " + filePath +
        " got exception ", e);
      return true;
    }
  }

  /**
   * checks the raided file system, prints a list of corrupt files to
   * this.out and returns the number of corrupt files.
   * Also prints out the total number of files with at least one missing block.
   * When called with '-retNumStrpsMissingBlks', also prints out number of stripes
   * with certain number of blocks missing for files using the 'RS' codec. 
   */
  public void fsck(String cmd, String[] args, int startIndex) throws IOException {
    final int numFsckArgs = args.length - startIndex;
    int numThreads = 16;
    String path = "/";
    boolean argsOk = false;
    boolean countOnly = false;
    boolean cntMissingBlksPerStrp = false;
    boolean listRecoverableFile = false;
    if (numFsckArgs >= 1) {
      argsOk = true;
      path = args[startIndex];
    }
    for (int i = startIndex + 1; i < args.length; i++) {
      if (args[i].equals("-threads")) {
        numThreads = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-count")) {
        countOnly = true;
      } else if (args[i].equals("-retNumStrpsMissingBlks")) {
        cntMissingBlksPerStrp = true;
      } else if (args[i].equals("-listrecoverablefiles")) {
        listRecoverableFile = true;
      }
    }
    if (!argsOk) {
      printUsage(cmd);
      return;
    }
    final String dateString = dateFormat.format(new Date());;
    System.err.println("Running RAID FSCK with " + numThreads + 
      " threads on " + path + " at time " + dateString);

    FileSystem fs = (new Path(path)).getFileSystem(conf);

    // if we got a raid fs, get the underlying fs 
    if (fs instanceof DistributedRaidFileSystem) {
      fs = ((DistributedRaidFileSystem) fs).getFileSystem();
    }

    // check that we have a distributed fs
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("expected DistributedFileSystem but got " +
                fs.getClass().getName());
    }
    final DistributedFileSystem dfs = (DistributedFileSystem) fs;

    // get a list of corrupted files (not considering parity blocks just yet)
    // from the name node
    // these are the only files we need to consider:
    // if a file has no corrupted data blocks, it is OK even if some
    // of its parity blocks are corrupted, so no further checking is
    // necessary
    System.err.println("Querying NameNode for list of corrupt files under " + path);
    final String[] files = DFSUtil.getCorruptFiles(dfs, path);
    final List<String> corruptFileCandidates = new LinkedList<String>();
    for (final String f: files) {
      // if this file is a parity file
      // or if it does not start with the specified path,
      // ignore it
      boolean matched = false;
      for (Codec c : Codec.getCodecs()) {
        if (f.startsWith(c.getParityPrefix())) {
          matched = true;
        }
      }
      if (!matched) {
        corruptFileCandidates.add(f);
      }
    }
    // filter files marked for deletion
    RaidUtils.filterTrash(conf, corruptFileCandidates);
    
    //clear numStrpMissingBlks if missing blocks per stripe is to be counted
    if (cntMissingBlksPerStrp) {
      for (AtomicLongArray numStrpMissingBlks : 
                          numStrpMissingBlksMap.values()) {
        for (int i = 0; i < numStrpMissingBlks.length(); i++) {
          numStrpMissingBlks.set(i, 0);
        }
      }
    }
    System.err.println(
      "Processing " + corruptFileCandidates.size() + " possibly corrupt files using " +
      numThreads + " threads");
    ExecutorService executor = null;
    ThreadFactory factory = new ThreadFactory() {
      final AtomicInteger tnum = new AtomicInteger();
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("Raidfsck-" + dateString + "-" + tnum.incrementAndGet());
        return t;
      }
    };
    if (numThreads > 1) {
      executor = Executors.newFixedThreadPool(numThreads, factory);
    } else {
      numThreads = 1;
    }
    final List<String> unRecoverableFiles = Collections.synchronizedList(new LinkedList<String>());
    final List<String> recoverableFiles = Collections.synchronizedList(new LinkedList<String>());
    final boolean finalCountOnly = countOnly;
    final boolean finalMissingBlksPerStrpCnt = cntMissingBlksPerStrp;
    final boolean finalListRecoverableFile = listRecoverableFile;
    final int step = numThreads;
    final AtomicInteger finishNum = new AtomicInteger(0);
    for (int i = 0; i < numThreads; i++) {
      if (!dfs.getClient().isOpen()) {
        throw new IOException("Filesystem closed.");
      }
      final int startIdx = i; 
      Runnable work = new Runnable() {
        public void run() {
          try {
            for (int idx = startIdx; idx < corruptFileCandidates.size();
                idx += step) {
              String corruptFileCandidate = corruptFileCandidates.get(idx);
              boolean corrupt = false;
              try {
                FileStatus corruptStat;
                try {
                  corruptStat = dfs.getFileStatus(new Path(corruptFileCandidate));
                } catch (FileNotFoundException fnfe) {
                  continue;
                }
                if (!dfs.getClient().isOpen()) {
                  LOG.warn("Filesystem closed.");
                  return;
                }
                corrupt = isFileCorrupt(dfs, corruptStat, finalMissingBlksPerStrpCnt);
                if (corrupt) {
                  incrCorruptCount();
                  if (!finalCountOnly && !finalListRecoverableFile) {
                    unRecoverableFiles.add(corruptFileCandidate);
                  }
                } else {
                  if (!finalCountOnly && finalListRecoverableFile) {
                    recoverableFiles.add(corruptFileCandidate);
                  }
                }
              } catch (Throwable e) {
                LOG.error("Error in processing " + corruptFileCandidate, e);
              }
            }
          } finally {
            finishNum.incrementAndGet();
          }
        }
      };
      if (executor != null) {
        executor.execute(work);
      } else {
        work.run();
      }
    }
    if (executor != null) {
      try {
        while (finishNum.get() < numThreads) {
          try {
            Thread.sleep(2000);
          } catch (InterruptedException ie) {
            LOG.warn("Raidfsck get exception ", ie);
            throw new IOException(ie);
          }
        }
      } finally {
        executor.shutdown(); // Waits for submitted tasks to finish.
      }
    }
    
    // If client is closed, fail the fsck check.
    if (!dfs.getClient().isOpen()) {
      throw new IOException("Filesystem closed.");
    }
    
    if (countOnly) {
      //Number of corrupt files (which cannot be fixed by Raid)
      out.println(getCorruptCount());
      LOG.info("Nubmer of corrupt files:" + getCorruptCount());
      //Number of files with at least one missing block
      out.println(corruptFileCandidates.size()); 
      LOG.info("Number of files with at least one block missing/corrupt: "+corruptFileCandidates.size());
    } else {
      if (listRecoverableFile) {
        for (String file : recoverableFiles) {
          out.println(file);
        }
      } else {
        for (String file : unRecoverableFiles) {
          out.println(file);
        }
      }
    }
    
    /*Number of stripes with missing blocks array, separated by each code id:
     * Number of missing blocks found from non-raided files.
     * codeId1
     * index 0: Number of stripes found with one block missing in this fsck
     * index 1: Number of stripes found with two block missing in this fsck
     * and so on
     * codeId2
     * index 0: Number of stripes found with one block missing in this fsck
     * index 1: Number of stripes found with two block missing in this fsck
     * and so on
     */
    if (cntMissingBlksPerStrp) {
      out.println(this.numNonRaidedMissingBlks);
      for (String codecId : numStrpMissingBlksMap.keySet()) {
        out.println(codecId);
        AtomicLongArray numStrpMissingBlks = 
                      numStrpMissingBlksMap.get(codecId);
        for (int j = 0; j < numStrpMissingBlks.length() ; j++) {
          long temp = numStrpMissingBlks.get(j);
          out.println(temp); 
          LOG.info("Number of stripes with missing blocks at index "+ j + " is " + temp);
        }
      }
    }
  }

  // For testing.
  private void incrCorruptCount() {
    corruptCounter.incrementAndGet();
  }

  // For testing.
  int getCorruptCount() {
    return corruptCounter.get();
  }
  
  long getStrpMissingBlks(String codecId, int index){
    return numStrpMissingBlksMap.get(codecId).get(index);
  }

  void usefulHar(String[] args, int startIndex) throws IOException {
    if (args.length - startIndex < 2) {
      printUsage("usefulHar");
      throw new IllegalArgumentException("Too few arguments");
    }
    Codec codec = Codec.getCodec(args[startIndex]);
    Path prefixPath = new Path(codec.parityDirectory);
    String prefix = prefixPath.toUri().getPath();
    FileSystem fs = new Path("/").getFileSystem(conf);
    for (int i = startIndex + 1; i < args.length; i++) {
      String harPath = args[i];
      if (harPath.startsWith(prefix)) {
        float usefulPercent =
          PurgeMonitor.usefulHar(
            codec, null, fs, fs, new Path(harPath), prefix, conf, null);
        out.println("Useful percent of " + harPath + " " + usefulPercent);
      } else {
        System.err.println("Har " + harPath + " is not located in " +
          prefix + ", ignoring");
      }
    }
  }

  public void checkFile(String cmd, String[] args, int startIndex)
      throws IOException {
    if (startIndex >= args.length) {
      printUsage(cmd);
      throw new IllegalArgumentException("Insufficient arguments");
    }
    for (int i = startIndex; i < args.length; i++) {
      Path p = new Path(args[i]);
      FileSystem fs = p.getFileSystem(conf);
      // if we got a raid fs, get the underlying fs 
      if (fs instanceof DistributedRaidFileSystem) {
        fs = ((DistributedRaidFileSystem) fs).getFileSystem();
      }
      // We should be able to cast at this point.
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      RemoteIterator<Path> corruptIt = dfs.listCorruptFileBlocks(p);
      int count = 0;
      while (corruptIt.hasNext()) {
        count++;
        Path corruptFile = corruptIt.next();
        // Result of checking.
        String result = null;
        FileStatus stat = fs.getFileStatus(corruptFile);
        if (stat.getReplication() < fs.getDefaultReplication()) {
          RaidInfo raidInfo = RaidUtils.getFileRaidInfo(stat, conf);
          if (raidInfo.codec == null) {
            result = "Below default replication but no parity file found";
          } else {
            boolean notRecoverable = isFileCorrupt(dfs, stat);
            if (notRecoverable) {
              result = "Missing too many blocks to be recovered " + 
                "using parity file " + raidInfo.parityPair.getPath();
            } else {
              result = "Has missing blocks but can be read using parity file " +
                raidInfo.parityPair.getPath();
            }
          }
        } else {
          result = "At default replication, not raided";
        }
        out.println("Result of checking " + corruptFile + " : " +
          result);
      }
      out.println("Found " + count + " files with missing blocks");
    }
  }

  public void purgeParity(String cmd, String[] args, int startIndex)
      throws IOException {
    if (startIndex + 1 >= args.length) {
      printUsage(cmd);
      throw new IllegalArgumentException("Insufficient arguments");
    }
    Path parityPath = new Path(args[startIndex]);
    AtomicLong entriesProcessed = new AtomicLong(0);
    System.err.println("Starting recursive purge of " + parityPath);

    Codec codec = Codec.getCodec(args[startIndex + 1]);
    FileSystem srcFs = parityPath.getFileSystem(conf);
    if (srcFs instanceof DistributedRaidFileSystem) {
      srcFs = ((DistributedRaidFileSystem)srcFs).getFileSystem();
    }
    FileSystem parityFs = srcFs;
    String parityPrefix = codec.parityDirectory;
    DirectoryTraversal obsoleteParityFileRetriever =
      new DirectoryTraversal(
        "Purge File ",
        java.util.Collections.singletonList(parityPath),
        parityFs,
        new PurgeMonitor.PurgeParityFileFilter(conf, codec, null,
            srcFs, parityFs,
          parityPrefix, null, entriesProcessed),
        1,
        false);
    FileStatus obsolete = null;
    while ((obsolete = obsoleteParityFileRetriever.next()) !=
              DirectoryTraversal.FINISH_TOKEN) {
      PurgeMonitor.performDelete(parityFs, obsolete.getPath(), false);
    }
  }

  public void checkParity(String cmd, String[] args, int startIndex)
      throws IOException {
    if (startIndex >= args.length) {
      printUsage(cmd);
      throw new IllegalArgumentException("Insufficient arguments");
    }
    for (int i = startIndex; i < args.length; i++) {
      Path p = new Path(args[i]);
      FileSystem fs = p.getFileSystem(conf);
      FileStatus stat = null;
      try {
        stat = fs.getFileStatus(p);
      } catch (FileNotFoundException e) {
        System.out.println("Path: " + p + " does not exist!");
        continue;
      }
      ParityFilePair ppair = null;
      int numParityFound = 0;
      for (Codec c : Codec.getCodecs()) {
        try {
          ppair = ParityFilePair.getParityFile(c, stat, conf);
          if (ppair != null) {
            System.out.println(c.id + " parity: " + ppair.getPath());
            numParityFound += 1;
          }
        } catch (FileNotFoundException ignore) {
        }
      }
      if (numParityFound == 0) {
        System.out.println("No parity file found");
      }
      if (numParityFound > 1) {
        System.out.println("Warning: multiple parity files found");
      }
    }
  }
  
  static private boolean isBlockCorrupt(BlockLocation fileBlock)
      throws IOException {
    if (fileBlock == null)
      // empty block
      return false;
    return fileBlock.isCorrupt() || 
        (fileBlock.getNames().length == 0 && fileBlock.getLength() > 0);
  }
  
  

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    RaidShell shell = null;
    try {
      shell = new RaidShell(new Configuration());
      int res = ToolRunner.run(shell, argv);
      System.exit(res);
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      System.exit(-1);
    } catch (IOException e) {
      System.err.
        println("Bad connection to RaidNode or NameNode. command aborted.");
      System.err.println(e.getMessage());
      System.exit(-1);
    } finally {
      shell.close();
    }
  }
}
