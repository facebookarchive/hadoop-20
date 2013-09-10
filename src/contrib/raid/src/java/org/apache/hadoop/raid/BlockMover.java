package org.apache.hadoop.raid;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.Comparator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.ReplaceBlockHeader;
import org.apache.hadoop.hdfs.protocol.VersionAndOpcode;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;

class BlockMover {
  public static final Log LOG = LogFactory.getLog(BlockMover.class);

  final private BlockingQueue<Runnable> movingQueue;
  final private int maxQueueSize;
  final private RaidNodeMetrics metrics;
  final private boolean simulate;
  final private Random rand;
  final private Configuration conf;
  final private int alwaysSubmitPriorityLevel;

  final ClusterInfo cluster;
  final Thread clusterUpdater;
  ExecutorService executor;
  final int chooseNodeMaxRetryTimes;
  // this is for test only.
  final boolean treatNodesOnDifferentRack;
  
  final static String RAID_CHOOSE_NODE_RETRY_TIMES_KEY = "raid.block.mover.choose.node.retry";
  final static int RAID_CHOOSE_NODE_RETRY_TIMES_DEFAULT = 100;
  
  final static String RAID_TEST_TREAT_NODES_ON_DEFAULT_RACK_KEY = "raid.test.treat.nodes.on.different.rack";
  

  BlockMover(int numMovingThreads, int maxQueueSize,
      boolean simulate, int alwaysSubmitPriorityLevel, Configuration conf) throws IOException {

    this.movingQueue = new PriorityBlockingQueue<Runnable>(
        1000, new BlockMoveActionComparator());

    ThreadFactory factory = new ThreadFactory() {
      final AtomicInteger numThreads = new AtomicInteger();
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("BLockMoveExecutor-" + numThreads.getAndIncrement());
        return t;
      }
    };

    this.executor = new ThreadPoolExecutor(numMovingThreads,
        numMovingThreads, 0L, TimeUnit.MILLISECONDS, movingQueue, factory);

    this.maxQueueSize = maxQueueSize;
    this.metrics = RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
    this.cluster = new ClusterInfo();
    this.clusterUpdater = new Thread(cluster);
    this.simulate = simulate;
    this.rand = new Random();
    this.conf = conf;
    this.alwaysSubmitPriorityLevel = alwaysSubmitPriorityLevel;
    this.chooseNodeMaxRetryTimes = conf.getInt(RAID_CHOOSE_NODE_RETRY_TIMES_KEY, RAID_CHOOSE_NODE_RETRY_TIMES_DEFAULT);
    this.treatNodesOnDifferentRack = conf.getBoolean(RAID_TEST_TREAT_NODES_ON_DEFAULT_RACK_KEY, false);
  }

  public void start() {
    clusterUpdater.setDaemon(true);
    clusterUpdater.start();
  }

  public void stop() {
    cluster.stop();
    clusterUpdater.interrupt();
    executor.shutdown();
  }

  public int getQueueSize() {
    return movingQueue.size();
  }
  
  public boolean isOnSameRack(DatanodeInfo n1, DatanodeInfo n2) {
    if (treatNodesOnDifferentRack) {
      // this is for test only
    	if (n1 == null) {
    		return false;
    	} else if (n1.equals(n2)) {
    		return true;
    	} else {
    		return false;
    	}
    }
    return cluster.isOnSameRack(n1, n2);
  }

  public void move(LocatedBlock block, DatanodeInfo node, DatanodeInfo target,
      Set<DatanodeInfo> excludedNodes, int priority,
      int dataTransferProtocolVersion, int namespaceId) {
    BlockMoveAction action = new BlockMoveAction(
        block, node, target, excludedNodes, priority,
        dataTransferProtocolVersion, namespaceId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Bad block placement: " + action);
    }
    int movingQueueSize = movingQueue.size();
    //For high-pri moves, the queue limit is 2*maxQueueSize
    if (movingQueueSize < maxQueueSize ||
        movingQueueSize < 2 * maxQueueSize &&
        action.priority >= alwaysSubmitPriorityLevel) {
      executor.execute(action);
      metrics.blockMoveScheduled.inc();
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Block move queue is full. Skip the action." +
          " size:" + movingQueueSize +
          " maxSize:" + maxQueueSize);
      }
      metrics.blockMoveSkipped.inc();
    }
  }

  /**
   * Sort BlockMoveAction based on the priority in descending order
   */
  static class BlockMoveActionComparator implements Comparator<Runnable> {
    @Override
    public int compare(Runnable o1, Runnable o2) {
      BlockMoveAction a1 = (BlockMoveAction) o1;
      BlockMoveAction a2 = (BlockMoveAction) o2;
      if (a1.priority > a2.priority) {
        return -1;
      }
      if (a1.priority < a2.priority) {
        return 1;
      }
      // if tie, sort based on the time in ascending order
      return a1.createTime > a2.createTime ? 1 : -1;
    }
  }
  
  /**
   * explicitly choose the target nodes. 
   * @throws IOException
   */
  public DatanodeInfo chooseTargetNodes(Set<DatanodeInfo> excludedNodes) 
      throws IOException {
    DatanodeInfo target = cluster.getNodeOnDifferentRack(excludedNodes);
    if (target == null) {
      throw new IOException ("Error choose datanode");
    }
    return target;
  }

  /**
   * Create one more replication of the block
   */
  class BlockMoveAction implements Runnable {
    final LocatedBlock block;
    final Set<DatanodeInfo> excludedNodes;
    final DatanodeInfo source;  // The datanode where this block will be removed
    DatanodeInfo target;  // The destination for this block
    DatanodeInfo proxySource; // The datanode that copies this block to target
    final int priority;
    final long createTime;
    final int dataTransferProtocolVersion; // data transfer protocol supported by HDFS cluster
    final int namespaceId; // name space that the block belongs to
    BlockMoveAction(LocatedBlock block,
        DatanodeInfo source,
        Set<DatanodeInfo> excludedNodes,
        int priority,
        int dataTransferProtocol,
        int namespaceId) {
     this(block, source, null, excludedNodes, priority, 
         dataTransferProtocol, namespaceId); 
    }
    
    BlockMoveAction(LocatedBlock block,
        DatanodeInfo source,
        DatanodeInfo target,
        Set<DatanodeInfo> excludedNodes,
        int priority,
        int dataTransferProtocol,
        int namespaceId) {
      this.block = block;
      this.excludedNodes = excludedNodes;
      for (DatanodeInfo d : block.getLocations()) {
        // Also exclude the original locations
        excludedNodes.add(d);
      }
      this.source = source;
      this.target = target;
      this.createTime = System.currentTimeMillis();
      this.priority = priority;
      this.dataTransferProtocolVersion = dataTransferProtocol;
      this.namespaceId = namespaceId;
    }
    
    /**
     * Choose target, source and proxySource for the move
     * @throws IOException
     */
    void chooseNodes() throws IOException {
      if (target == null) {
        target = cluster.getNodeOnDifferentRack(excludedNodes);
        if (target == null) {
          throw new IOException("Error choose datanode");
        }
      }
      for (DatanodeInfo n : block.getLocations()) {
        if (cluster.isOnSameRack(target, n)) {
          proxySource = n;
          return;
        }
      }
      proxySource =
        block.getLocations()[rand.nextInt(block.getLocations().length)];
    }
    @Override
    public void run() {
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      String threadName = "[" + Thread.currentThread().getName() + "] ";
      try {
        chooseNodes();
        if (simulate) {
          LOG.debug("Simulate mode. Skip move target:" + target +
              " source:" + source + " proxySource:" + proxySource);
          metrics.blockMove.inc();
          return;
        }
        sock = new Socket();
        sock.connect(NetUtils.createSocketAddr(
            target.getName()), HdfsConstants.READ_TIMEOUT);
        sock.setKeepAlive(true);
        sock.setSoTimeout(3600000); // set the timeout to be 1 hour
        out = new DataOutputStream( new BufferedOutputStream(
            sock.getOutputStream(), FSConstants.BUFFER_SIZE));
        if (LOG.isDebugEnabled()) {
          LOG.debug( "Start moving block " + block.getBlock().getBlockId() +
              " from "+ source.getName() +
              " to " + target.getName() +
              " through " + proxySource.getName());
        }
        sendRequest(out);
        in = new DataInputStream( new BufferedInputStream(
            sock.getInputStream(), FSConstants.BUFFER_SIZE));
        receiveResponse(in);
        metrics.blockMove.inc();
        LOG.info(threadName + "Moving block " + block.getBlock().getBlockId());
        LOG.info(threadName + "priority " + priority); 
        LOG.info(threadName + "from "+ source.getName());
        LOG.info(threadName + "to " + target.getName());
        LOG.info(threadName + "through " + proxySource.getName() + " succeed.");
      } catch (Exception e) {
        try {
          LOG.warn(threadName, e);
          LOG.warn(threadName + "Error moving block " + block.getBlock().getBlockId());
          LOG.warn(threadName + "from " + source.getName() + " to ");
          LOG.warn(threadName + target.getName() + " through " + proxySource.getName());
          if (e instanceof EOFException) {
            LOG.warn(threadName + "Moving block " + block.getBlock().getBlockId() +
              " was cancelled because the time exceeded the limit");
          }
        } catch (Exception newE) {
          LOG.warn(threadName + "New error ", newE);
        }
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }
    }
    @Override
    public String toString() {
      StringBuilder ret = new StringBuilder();
      ret.append("block:").append(block.getBlock()).append("\t");
      ret.append("locations:");
      boolean first = true;
      for (DatanodeInfo n : block.getLocations()) {
        if (first) {
          ret.append(n.getHostName());
          first = false;
          continue;
        }
        ret.append(",").append(n.getHostName());
      }
      ret.append("\t");
      ret.append("priority:");
      ret.append(priority);
      ret.append("\t");
      ret.append("source:");
      ret.append(source);
      ret.append("\t");
      ret.append("target:");
      ret.append(target);
      ret.append("\t");
      ret.append("createTime:");
      ret.append(createTime);
      ret.append("\t");
      ret.append("excludeNodes:");
      ret.append(excludedNodes.size());
      return ret.toString();
    }

    /**
     * Send a block replace request to the output stream
     */
    private void sendRequest(DataOutputStream out) throws IOException {
      ReplaceBlockHeader header = new ReplaceBlockHeader(new VersionAndOpcode(
          dataTransferProtocolVersion, DataTransferProtocol.OP_REPLACE_BLOCK));
      header.set(namespaceId, block.getBlock().getBlockId(), block.getBlock()
          .getGenerationStamp(), source.getStorageID(), proxySource);
      header.writeVersionAndOpCode(out);
      header.write(out);
      out.flush();
    }

    /**
     * Receive a block copy response from the input stream
     */
    private void receiveResponse(DataInputStream in) throws IOException {
      short status = in.readShort();
      if (status != DataTransferProtocol.OP_STATUS_SUCCESS) {
        throw new IOException("block move is failed");
      }
    }
  }

  /**
   * Periodically obtain node information from the cluster
   */
  class ClusterInfo implements Runnable {
    NetworkTopology topology = new NetworkTopology();
    DatanodeInfo liveNodes[];
    static final long UPDATE_PERIOD = 60000L;
    volatile boolean running = true;
    long lastUpdate = -1L;

    @Override
    public void run() {
      DistributedFileSystem dfs = null;
      do {
        try {
          dfs = DFSUtil.convertToDFS(FileSystem.get(conf));
        } catch (IOException e) {
          LOG.warn("Failed to init file system", e);
          try {
            Thread.sleep(500); // sleep for half second
          } catch (InterruptedException ie) {
            LOG.info("Got interrupted", ie);
            return;
          }
        }
      } while (dfs == null);
      // Update the information about the datanodes in the cluster
      while (running) {
        try {
          long now = System.currentTimeMillis();
          if (now - lastUpdate > UPDATE_PERIOD) {
            lastUpdate = now;
            synchronized (this) {
              // This obtain the datanodes from the HDFS cluster in config file.
              // If we need to support parity file in a different cluster, this
              // has to change.
              liveNodes = dfs.getLiveDataNodeStats();
              for (DatanodeInfo n : liveNodes) {
                topology.add(n);
              }
            }
          }
          Thread.sleep(UPDATE_PERIOD / 10);
        } catch (InterruptedException e) {
          LOG.warn("Error update datanodes ", e);
        } catch (IOException e) {
          LOG.warn("Error update datanodes ", e);
        }
      }
    }
    public void stop() {
      running = false;
    }
    
    public synchronized DatanodeInfo getRandomNode(Set<DatanodeInfo> excluded) {
      if (liveNodes == null || liveNodes.length == 0) {
        return null;
      }
      if (liveNodes.length <= excluded.size()) {
        return liveNodes[rand.nextInt(liveNodes.length)];
      }
      for (;;) {
        DatanodeInfo target = liveNodes[rand.nextInt(liveNodes.length)];
        if (!excluded.contains(target)) {
          return target;
        }
      }
    }
    
    /**
     * Choose a node on different rack
     */
    public synchronized DatanodeInfo getNodeOnDifferentRack(
                                      Set<DatanodeInfo> excluded) {
      if (liveNodes == null || liveNodes.length == 0) {
        return null;
      }
      if (liveNodes.length <= excluded.size()) {
        return liveNodes[rand.nextInt(liveNodes.length)];
      }
      int retry = 0;
      for (;;) {
        retry ++;
        DatanodeInfo target = liveNodes[rand.nextInt(liveNodes.length)];
        if (!excluded.contains(target)) {
          if (retry >= chooseNodeMaxRetryTimes) {
            return target;
          }
          if (topology.getNumOfRacks() <= 1) {
            return target;
          } else {
            boolean sameRack = false;
            for (DatanodeInfo node : excluded) {
              if (isOnSameRack(node, target)) {
                sameRack = true;
                break;
              }
            }
            if (!sameRack) {
              return target;
            }
          }
        }
      }
    }
    
    public synchronized boolean isOnSameRack(DatanodeInfo n1, DatanodeInfo n2) {
    	topology.add(n1);
    	topology.add(n2);
      return topology.isOnSameRack(n1, n2);
    }
  }
}
