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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import java.net.SocketException;
import javax.security.auth.login.LoginException;

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
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.RemoteIterator;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.raid.protocol.PolicyList;
import org.apache.hadoop.raid.protocol.RaidProtocol;


/**
 * A {@link RaidShell} that allows browsing configured raid policies.
 */
public class RaidShell extends Configured implements Tool {
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  public static final Log LOG = LogFactory.getLog( "org.apache.hadoop.RaidShell");
  public RaidProtocol raidnode;
  RaidProtocol rpcRaidnode;
  private UnixUserGroupInformation ugi;
  volatile boolean clientRunning = true;
  private Configuration conf;
  AtomicInteger corruptCounter = new AtomicInteger();

  /**
   * Start RaidShell.
   * <p>
   * The RaidShell connects to the specified RaidNode and performs basic
   * configuration options.
   * @throws IOException
   */
  public RaidShell(Configuration conf) throws IOException {
    super(conf);
    this.conf = conf;
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

  private static RaidProtocol createRPCRaidnode(InetSocketAddress raidNodeAddr,
      Configuration conf, UnixUserGroupInformation ugi)
    throws IOException {
    LOG.info("RaidShell connecting to " + raidNodeAddr);
    return (RaidProtocol)RPC.getProxy(RaidProtocol.class,
        RaidProtocol.versionID, raidNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, RaidProtocol.class));
  }

  private static RaidProtocol createRaidnode(RaidProtocol rpcRaidnode)
    throws IOException {
    RetryPolicy createPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        5, 5000, TimeUnit.MILLISECONDS);

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

  private void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("RaidNode closed");
      throw result;
    }
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
    String prefix = "Usage: java " + RaidShell.class.getSimpleName();
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
    } else if ("-fsck".equals(cmd)) {
      System.err.println("Usage: java RaidShell [-fsck [path [-threads numthreads]]]");
    } else if ("-usefulHar".equals(cmd)) {
      System.err.println("Usage: java RaidShell [-usefulHar <XOR|RS> [path-to-raid-har]]");
    } else if ("-checkFile".equals(cmd)) {
      System.err.println("Usage: java RaidShell [-checkFile path]");
    } else if ("-purgeParity".equals(cmd)) {
      System.err.println("Usage: java RaidShell -purgeParity path <XOR|RS>");
    } else if ("-checkParity".equals(cmd)) {
      System.err.println("Usage: java RaidShell [-checkParity path]");
    } else {
      System.err.println("Usage: java RaidShell");
      System.err.println("           [-showConfig ]");
      System.err.println("           [-help [cmd]]");
      System.err.println("           [-recover srcPath1 corruptOffset]");
      System.err.println("           [-recoverBlocks path1 path2...]");
      System.err.println("           [-raidFile <path-to-file> <path-to-raidDir> <XOR|RS>");
      System.err.println("           [-fsck [path [-threads numthreads]]]");
      System.err.println("           [-usefulHar <XOR|RS> [path-to-raid-har]]");
      System.err.println("           [-checkFile path]");
      System.err.println("           -purgeParity path <XOR|RS>");
      System.err.println("           [-checkParity path]");
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
      if ((argv.length < 1) || (argv.length > 4)) {
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
      } else if ("-fsck".equals(cmd)) {
        fsck(cmd, argv, i);
        exitCode = 0;
      } else if ("-usefulHar".equals(cmd)) {
        usefulHar(argv, i);
        exitCode = 0;
      } else if ("-checkFile".equals(cmd)) {
        checkFile(cmd, argv, i);
      } else if ("-purgeParity".equals(cmd)) {
        purgeParity(cmd, argv, i);
      } else if ("-checkParity".equals(cmd)) {
        checkParity(cmd, argv, i);
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
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int showConfig(String cmd, String argv[], int startindex) throws IOException {
    int exitCode = 0;
    int i = startindex;
    PolicyList[] all = raidnode.getAllPolicies();
    for (PolicyList list: all) {
      for (PolicyInfo p : list.getAll()) {
        System.out.println(p);
      }
    }
    return exitCode;
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
      System.out.println(p);
    }
    return exitCode;
  }

  public void recoverBlocks(String[] args, int startIndex)
    throws IOException {
    LOG.info("Recovering blocks for " + (args.length - startIndex) + " files");
    BlockReconstructor.CorruptBlockReconstructor fixer = new BlockReconstructor.CorruptBlockReconstructor(conf);
    for (int i = startIndex; i < args.length; i++) {
      String path = args[i];
      fixer.reconstructFile(new Path(path), RaidUtils.NULL_PROGRESSABLE);
    }
  }

  public void raidFile(String[] args, int startIndex) throws IOException {
    Path file = new Path(args[startIndex]);
    Path destPath = new Path(args[startIndex + 1]);
    ErasureCodeType code = ErasureCodeType.valueOf(args[startIndex + 2]);
    LOG.info("Raiding file " + file + " to " + destPath + " using " + code);
    FileSystem fs = destPath.getFileSystem(conf);
    FileStatus stat = fs.getFileStatus(file);
    boolean doSimulate = false;
    int targetRepl = conf.getInt("raidshell.raidfile.targetrepl",
      stat.getReplication());
    int metaRepl = conf.getInt("raidshell.raidfile.metarepl", 2);
    int stripeLength = RaidNode.getStripeLength(conf);
    RaidNode.doRaid(conf, stat, destPath, code, new RaidNode.Statistics(),
      RaidUtils.NULL_PROGRESSABLE, doSimulate, targetRepl, metaRepl,
      stripeLength);
  }

  /**
   * checks whether a file has more than the allowable number of
   * corrupt blocks and must therefore be considered corrupt
   */
  protected boolean isFileCorrupt(final DistributedFileSystem dfs, 
                                final Path filePath) 
    throws IOException {
    try {
      // corruptBlocksPerStripe: 
      // map stripe # -> # of corrupt blocks in that stripe (data + parity)
      HashMap<Integer, Integer> corruptBlocksPerStripe =
        new LinkedHashMap<Integer, Integer>();

      // read conf
      final int stripeBlocks = RaidNode.getStripeLength(conf);

      // figure out which blocks are missing/corrupted
      final FileStatus fileStatus = dfs.getFileStatus(filePath);
      final long blockSize = fileStatus.getBlockSize();
      final long fileLength = fileStatus.getLen();
      final long fileLengthInBlocks = (fileLength / blockSize) +
        (((fileLength % blockSize) == 0) ? 0L : 1L);
      final long fileStripes = (fileLengthInBlocks / stripeBlocks) +
        (((fileLengthInBlocks % stripeBlocks) == 0) ? 0L : 1L);
      final BlockLocation[] fileBlocks = 
        dfs.getFileBlockLocations(fileStatus, 0, fileLength);
      
      // figure out which stripes these corrupted blocks belong to
      for (BlockLocation fileBlock: fileBlocks) {
        int blockNo = (int) (fileBlock.getOffset() / blockSize);
        final int stripe = (int) (blockNo / stripeBlocks);
        if (fileBlock.isCorrupt() || 
            (fileBlock.getNames().length == 0 && fileBlock.getLength() > 0)) {
          if (corruptBlocksPerStripe.get(stripe) == null) {
            corruptBlocksPerStripe.put(stripe, 1);
          } else {
            corruptBlocksPerStripe.put(stripe, corruptBlocksPerStripe.
                                       get(stripe) + 1);
          }
          LOG.debug("file " + filePath.toString() + " corrupt in block " + 
                   blockNo + "/" + fileLengthInBlocks + ", stripe " + stripe +
                   "/" + fileStripes);
        } else {
          LOG.debug("file " + filePath.toString() + " OK in block " + blockNo +
                   "/" + fileLengthInBlocks + ", stripe " + stripe + "/" +
                   fileStripes);
        }
      }

      RaidInfo raidInfo = getFileRaidInfo(filePath);

      // now check parity blocks
      if (raidInfo.raidType != RaidType.NONE) {
        checkParityBlocks(filePath, corruptBlocksPerStripe, blockSize,
                          fileStripes, raidInfo);
      }

      final int maxCorruptBlocksPerStripe = raidInfo.parityBlocksPerStripe;

      for (int corruptBlocksInStripe: corruptBlocksPerStripe.values()) {
        if (corruptBlocksInStripe > maxCorruptBlocksPerStripe) {
          return true;
        }
      }
      return false;
    } catch (SocketException e) {
      // Re-throw network-related exceptions.
      throw e;
    } catch (IOException e) {
      LOG.error("While trying to check isFileCorrupt " + filePath +
        " got exception ", e);
      return true;
    }
  }

  /**
   * holds the type of raid used for a particular file
   */
  private enum RaidType {
    XOR,
    RS,
    NONE
  }

  /**
   * holds raid type and parity file pair
   */
  private class RaidInfo {
    public RaidInfo(final RaidType raidType, 
                    final ParityFilePair parityPair,
                    final int parityBlocksPerStripe) {
      this.raidType = raidType;
      this.parityPair = parityPair;
      this.parityBlocksPerStripe = parityBlocksPerStripe;
    }
    public final RaidType raidType;
    public final ParityFilePair parityPair;
    public final int parityBlocksPerStripe;
  }

  /**
   * returns the raid for a given file
   */
  private RaidInfo getFileRaidInfo(final Path filePath)
    throws IOException {
    // now look for the parity file
    ParityFilePair ppair = null;
    try {
      // look for xor parity file first
      ppair = ParityFilePair.getParityFile(ErasureCodeType.XOR, filePath, conf);
    } catch (FileNotFoundException ignore) {
    }
    if (ppair != null) {
      return new RaidInfo(RaidType.XOR, ppair, 1);
    } else {
      // failing that, look for rs parity file
      try {
        ppair = ParityFilePair.getParityFile(
            ErasureCodeType.RS, filePath, conf);
      } catch (FileNotFoundException ignore) {
      }
      if (ppair != null) {
        return new RaidInfo(RaidType.RS, ppair, RaidNode.rsParityLength(conf));
      } else {
        return new RaidInfo(RaidType.NONE, null, 0);
      }
    }
  }

  /**
   * gets the parity blocks corresponding to file
   * returns the parity blocks in case of DFS
   * and the part blocks containing parity blocks
   * in case of HAR FS
   */
  private BlockLocation[] getParityBlocks(final Path filePath,
                                          final long blockSize,
                                          final long fileStripes,
                                          final RaidInfo raidInfo) 
    throws IOException {


    final String parityPathStr = raidInfo.parityPair.getPath().toUri().
      getPath();
    FileSystem parityFS = raidInfo.parityPair.getFileSystem();
    
    // get parity file metadata
    FileStatus parityFileStatus = parityFS.
      getFileStatus(new Path(parityPathStr));
    long parityFileLength = parityFileStatus.getLen();

    if (parityFileLength != fileStripes * raidInfo.parityBlocksPerStripe *
        blockSize) {
      throw new IOException("expected parity file of length" + 
                            (fileStripes * raidInfo.parityBlocksPerStripe *
                             blockSize) +
                            " but got parity file of length " + 
                            parityFileLength);
    }

    BlockLocation[] parityBlocks = 
      parityFS.getFileBlockLocations(parityFileStatus, 0L, parityFileLength);
    
    if (parityFS instanceof DistributedFileSystem ||
        parityFS instanceof DistributedRaidFileSystem) {
      long parityBlockSize = parityFileStatus.getBlockSize();
      if (parityBlockSize != blockSize) {
        throw new IOException("file block size is " + blockSize + 
                              " but parity file block size is " + 
                              parityBlockSize);
      }
    } else if (parityFS instanceof HarFileSystem) {
      LOG.debug("HAR FS found");
    } else {
      LOG.warn("parity file system is not of a supported type");
    }
    
    return parityBlocks;
  }

  /**
   * checks the parity blocks for a given file and modifies
   * corruptBlocksPerStripe accordingly
   */
  private void checkParityBlocks(final Path filePath,
                                 final HashMap<Integer, Integer>
                                 corruptBlocksPerStripe,
                                 final long blockSize,
                                 final long fileStripes,
                                 final RaidInfo raidInfo)
    throws IOException {

    // get the blocks of the parity file
    // because of har, multiple blocks may be returned as one container block
    BlockLocation[] containerBlocks = getParityBlocks(filePath, blockSize,
                                                      fileStripes, raidInfo);

    long parityStripeLength = blockSize *
      ((long) raidInfo.parityBlocksPerStripe);

    long parityFileLength = parityStripeLength * fileStripes;

    long parityBlocksFound = 0L;

    for (BlockLocation cb: containerBlocks) {
      if (cb.getLength() % blockSize != 0) {
        throw new IOException("container block size is not " +
                              "multiple of parity block size");
      }
      int blocksInContainer = (int) (cb.getLength() / blockSize);
      LOG.debug("found container with offset " + cb.getOffset() +
               ", length " + cb.getLength());

      for (long offset = cb.getOffset();
           offset < cb.getOffset() + cb.getLength();
           offset += blockSize) {
        long block = offset / blockSize;
        
        int stripe = (int) (offset / parityStripeLength);

        if (stripe < 0) {
          // before the beginning of the parity file
          continue;
        }
        if (stripe >= fileStripes) {
          // past the end of the parity file
          break;
        }

        parityBlocksFound++;

        if (cb.isCorrupt() ||
            (cb.getNames().length == 0 && cb.getLength() > 0)) {
          LOG.debug("parity file for " + filePath.toString() + 
                   " corrupt in block " + block +
                   ", stripe " + stripe + "/" + fileStripes);
          
          if (corruptBlocksPerStripe.get(stripe) == null) {
            corruptBlocksPerStripe.put(stripe, 1);
          } else {
            corruptBlocksPerStripe.put(stripe, 
                                       corruptBlocksPerStripe.get(stripe) +
                                       1);
          }
        } else {
          LOG.debug("parity file for " + filePath.toString() + 
                   " OK in block " + block +
                   ", stripe " + stripe + "/" + fileStripes);
        }
      }
    }

    long parityBlocksExpected = raidInfo.parityBlocksPerStripe * fileStripes;
    if (parityBlocksFound != parityBlocksExpected ) {
      throw new IOException("expected " + parityBlocksExpected + 
                            " parity blocks but got " + parityBlocksFound);
    }
  }


  /**
   * checks the raided file system, prints a list of corrupt files to
   * System.out and returns the number of corrupt files
   */
  public void fsck(String cmd, String[] args, int startIndex) throws IOException {
    final int numFsckArgs = args.length - startIndex;
    int numThreads = 1;
    String path = "/";
    if (numFsckArgs >= 1) {
      path = args[startIndex];
    } else if (numFsckArgs == 3 && args[startIndex + 1].equals("-threads")) {
      numThreads = Integer.parseInt(args[startIndex + 2]);
    } else {
      printUsage(cmd);
      return;
    }
    System.err.println("Running RAID FSCK with " + numThreads + 
      " threads on " + path);

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

    // get conf settings
    String xorPrefix = RaidNode.xorDestinationPath(conf).toUri().getPath();
    String rsPrefix = RaidNode.rsDestinationPath(conf).toUri().getPath();
    if (!xorPrefix.endsWith("/")) {
      xorPrefix = xorPrefix + "/";
    }
    if (!rsPrefix.endsWith("/")) {
      rsPrefix = rsPrefix + "/";
    }

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
      if (!f.startsWith(xorPrefix) &&
          !f.startsWith(rsPrefix)) {
        corruptFileCandidates.add(f);
      }
    }
    // filter files marked for deletion
    RaidUtils.filterTrash(conf, corruptFileCandidates);
    System.err.println(
      "Processing " + corruptFileCandidates.size() + " possibly corrupt files using " +
      numThreads + " threads");
    ExecutorService executor = null;
    if (numThreads > 1) {
      executor = Executors.newFixedThreadPool(numThreads);
    }
    for (final String corruptFileCandidate: corruptFileCandidates) {
      Runnable work = new Runnable() {
        public void run() {
          try {
            if (isFileCorrupt(dfs, new Path(corruptFileCandidate))) {
              incrCorruptCount();
              System.out.println(corruptFileCandidate);
            }
          } catch (IOException e) {
            LOG.error("Error in processing " + corruptFileCandidate, e);
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
      executor.shutdown(); // Waits for submitted tasks to finish.
      try {
        executor.awaitTermination(3600, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
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

  void usefulHar(String[] args, int startIndex) throws IOException {
    if (args.length - startIndex < 2) {
      printUsage("usefulHar");
      throw new IllegalArgumentException("Too few arguments");
    }
    ErasureCodeType code = ErasureCodeType.valueOf(args[startIndex]);
    Path prefixPath = RaidNode.getDestinationPath(code, conf);
    String prefix = prefixPath.toUri().getPath();
    FileSystem fs = new Path("/").getFileSystem(conf);
    for (int i = startIndex + 1; i < args.length; i++) {
      String harPath = args[i];
      if (harPath.startsWith(prefix)) {
        float usefulPercent =
          PurgeMonitor.usefulHar(
            code, fs, fs, new Path(harPath), prefix, conf, null);
        System.out.println("Useful percent of " + harPath + " " + usefulPercent);
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
        FileStatus stat = fs.getFileStatus(p);
        if (stat.getReplication() < fs.getDefaultReplication()) {
          RaidInfo raidInfo = getFileRaidInfo(corruptFile);
          if (raidInfo.raidType == RaidType.NONE) {
            result = "Below default replication but no parity file found";
          } else {
            boolean notRecoverable = isFileCorrupt(dfs, corruptFile);
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
        System.out.println("Result of checking " + corruptFile + " : " +
          result);
      }
      System.out.println("Found " + count + " files with missing blocks");
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

    ErasureCodeType code = ErasureCodeType.valueOf(args[startIndex + 1]);
    FileSystem srcFs = parityPath.getFileSystem(conf);
    if (srcFs instanceof DistributedRaidFileSystem) {
      srcFs = ((DistributedRaidFileSystem)srcFs).getFileSystem();
    }
    FileSystem parityFs = srcFs;
    String parityPrefix = RaidNode.getDestinationPath(code, conf).toUri().getPath();
    DirectoryTraversal obsoleteParityFileRetriever =
      new DirectoryTraversal(
        "Purge File ",
        java.util.Collections.singletonList(parityPath),
        parityFs,
        new PurgeMonitor.PurgeParityFileFilter(conf, code, srcFs, parityFs,
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
    boolean xorParityFound = false;
    boolean rsParityFound = false;
    for (int i = startIndex; i < args.length; i++) {
      Path p = new Path(args[i]);
      // look for xor parity file first
      ParityFilePair ppair = null;
      try {
        ppair = ParityFilePair.getParityFile(ErasureCodeType.XOR, p, conf);
        if (ppair != null) {
          System.out.println("XOR parity: " + ppair.getPath());
          xorParityFound = true;
        }
      } catch (FileNotFoundException ignore) {
      }
      // look for rs parity file
      try {
        ppair = ParityFilePair.getParityFile(ErasureCodeType.RS, p, conf);
        if (ppair != null) {
          System.out.println("RS parity: " + ppair.getPath());
          rsParityFound = true;
        }
      } catch (FileNotFoundException ignore) {
      }
      if (!xorParityFound && !rsParityFound) {
        System.out.println("No parity file found");
      }
      if (xorParityFound && rsParityFound) {
        System.out.println("Warning: XOR and RS parity file found");
      }
    }
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
