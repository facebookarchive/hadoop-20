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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
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
  final int dataTransferProtocolVersion;
  

  BlockMover(int numMovingThreads, int maxQueueSize,
      boolean simulate, int alwaysSubmitPriorityLevel, Configuration conf) throws IOException {

    this.movingQueue = new PriorityBlockingQueue<Runnable>(
        1000, new BlockMoveActionComparator());

    ThreadFactory factory = new ThreadFactory() {
      final AtomicInteger numThreads = new AtomicInteger();
      public Thread newThread(Runnable r) {
        Thread t = new Thread();
        t.setName("BLockMoveExecutor-" + numThreads.getAndIncrement());
        return t;
      }
    };

    this.executor = new ThreadPoolExecutor(numMovingThreads,
        numMovingThreads, 0L, TimeUnit.MILLISECONDS, movingQueue, factory);

    this.maxQueueSize = maxQueueSize;
    this.metrics = RaidNodeMetrics.getInstance();
    this.cluster = new ClusterInfo();
    this.clusterUpdater = new Thread(cluster);
    this.simulate = simulate;
    this.rand = new Random();
    this.conf = conf;
    this.alwaysSubmitPriorityLevel = alwaysSubmitPriorityLevel;
    this.dataTransferProtocolVersion = RaidUtils.getDataTransferProtocolVersion(conf);
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

  public void move(LocatedBlock block, DatanodeInfo node,
      Set<DatanodeInfo> excludedNodes, int priority) {
    BlockMoveAction action = new BlockMoveAction(
        block, node, excludedNodes, priority);
    LOG.debug("Bad block placement: " + action);
    int movingQueueSize = movingQueue.size();
    if (movingQueueSize < maxQueueSize ||
        action.priority >= alwaysSubmitPriorityLevel) {
      executor.execute(action);
      metrics.blockMoveScheduled.inc();
    } else {
      LOG.warn("Block move queue is full. Skip the action." +
          " size:" + movingQueueSize +
          " maxSize:" + maxQueueSize);
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
    BlockMoveAction(LocatedBlock block,
        DatanodeInfo source,
        Set<DatanodeInfo> excludedNodes,
        int priority) {
      this.block = block;
      this.excludedNodes = excludedNodes;
      for (DatanodeInfo d : block.getLocations()) {
        // Also exclude the original locations
        excludedNodes.add(d);
      }
      this.source = source;
      this.createTime = System.currentTimeMillis();
      this.priority = priority;
    }
    /**
     * Choose target, source and proxySource for the move
     * @throws IOException
     */
    void chooseNodes() throws IOException {
      target = cluster.getRandomNode(excludedNodes);
      if (target == null) {
        throw new IOException("Error choose datanode");
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
        if (LOG.isDebugEnabled()) {
          LOG.debug( "Moving block " + block.getBlock().getBlockId() +
              " from "+ source.getName() +
              " to " + target.getName() +
              " through " + proxySource.getName() + " succeed.");
        }
      } catch (IOException e) {
        LOG.warn("Error moving block " + block.getBlock().getBlockId() +
            " from " + source.getName() + " to " +
            target.getName() + " through " + proxySource.getName(), e);
        if (e instanceof EOFException) {
          LOG.warn("Moving block " + block.getBlock().getBlockId() +
            " was cancelled because the time exceeded the limit");
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
      out.writeShort(dataTransferProtocolVersion);
      out.writeByte(DataTransferProtocol.OP_REPLACE_BLOCK);
      out.writeLong(block.getBlock().getBlockId());
      out.writeLong(block.getBlock().getGenerationStamp());
      Text.writeString(out, source.getStorageID());
      proxySource.write(out);
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
              DFSClient client = new DFSClient(conf);
              liveNodes =
                  client.namenode.getDatanodeReport(DatanodeReportType.LIVE);
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
    public synchronized boolean isOnSameRack(DatanodeInfo n1, DatanodeInfo n2) {
      return topology.isOnSameRack(n1, n2);
    }
  }
}
