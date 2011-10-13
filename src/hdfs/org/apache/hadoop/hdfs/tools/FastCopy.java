package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Level;

/**
 * There is a need to perform fast file copy on HDFS (primarily for the purpose
 * of HBase Snapshot). The fast copy mechanism for a file works as follows :
 *
 * 1) Query metadata for all blocks of the source file.
 *
 * 2) For each block 'b' of the file, find out its datanode locations.
 *
 * 3) For each block of the file, add an empty block to the namesystem for the
 * destination file.
 *
 * 4) For each location of the block, instruct the datanode to make a local copy
 * of that block.
 *
 * 5) Once each datanode has copied over the its respective blocks, they report
 * to the namenode about it.
 *
 * 6) Wait for all blocks to be copied and exit.
 *
 * This would speed up the copying process considerably by removing top of the
 * rack data transfers.
 *
 * @author pritam
 *
 **/
public class FastCopy {

  private static final Log LOG = LogFactory.getLog(FastCopy.class);
  private Configuration conf;
  private ClientProtocol namenode = null;
  private Random random = new Random();
  private String clientName;
  private int socketTimeout;
  public static int THREAD_POOL_SIZE = 10;
  private final ExecutorService executor;
  // Maximum time to wait for a file copy to complete.
  public static final long MAX_WAIT_TIME = 30 * 60 * 1000; // 10 minutes

  public FastCopy() {
    this(new Configuration());
  }

  public FastCopy(Configuration conf) {
    this(conf, THREAD_POOL_SIZE);
  }

  public FastCopy(Configuration conf, int threadPoolSize) {
    this.conf = conf;
    this.executor = Executors.newFixedThreadPool(threadPoolSize);
    this.clientName = "FastCopy" + random.nextInt();
    this.socketTimeout = conf.getInt("dfs.socket.timeout",
        HdfsConstants.READ_TIMEOUT);
    try {
      this.namenode = DFSClient.createNamenode(conf);
    } catch (Exception e) {
      LOG.error("Could not instantiate FastCopier : " + e);
    }
  }

  private class FastFileCopy implements Callable<Boolean> {
    private final String src;
    private final String destination;

    public FastFileCopy(String src, String destination) {
      this.src = src;
      this.destination = destination;
    }

    public Boolean call() throws IOException {
      return copy(src, destination);
    }

    private boolean copy(String src, String destination)
      throws IOException {
      // Get source file information and create empty destination file.
      FileStatus srcFileStatus = namenode.getFileInfo(src);
      namenode.create(destination, srcFileStatus.getPermission(), clientName,
          true, true, srcFileStatus.getReplication(),
          srcFileStatus.getBlockSize());
      try {
        List<LocatedBlock> locatedBlocks = namenode.getBlockLocations(src, 0,
            Long.MAX_VALUE).getLocatedBlocks();

        LOG.debug("FastCopy : Block locations retrieved for : " + src);

        // Instruct each datanode to create a copy of the respective block.
        for (LocatedBlock srcLocatedBlock : locatedBlocks) {
          DatanodeInfo[] blockLocations = srcLocatedBlock.getLocations();
          List<DatanodeInfo> favoredNodes = new ArrayList<DatanodeInfo>();

          LOG.debug("Block locations for src block : "
              + srcLocatedBlock.getBlock());
          // Build a list of favored datanodes to copy blocks to.
          for (int i = 0; i < blockLocations.length; i++) {
            LOG.debug("Fast Copy : Location " + i + " for src block : "
                + srcLocatedBlock.getBlock() + " is : "
                + blockLocations[i].getHostName());
            favoredNodes.add(blockLocations[i]);
          }

          DatanodeInfo[] favoredNodesArr = new DatanodeInfo[favoredNodes
            .size()];

          LocatedBlock destinationLocatedBlock = namenode.addBlock(destination,
              clientName, null, favoredNodes.toArray(favoredNodesArr), false);

          LOG.debug("Fast Copy : Block "
              + destinationLocatedBlock.getBlock()
              + " added to namenode");

          // Iterate through each datanode and instruct the datanode to copy the
          // block.
          copyBlocks(srcLocatedBlock, destinationLocatedBlock);
        }

        // Wait for all blocks of the file to be copied.
        waitForBlockCopy(src, destination);
      } catch (IOException e) {
        // If we fail to copy, cleanup destination.
        namenode.delete(destination, false);
        throw e;
      }
      return true;
    }

    /**
     * Copies over a source block to several destinations.
     *
     * @param srcLocatedBlock
     *          the source block
     * @param destinationLocatedBlock
     *          the destination block
     * @throws IOException
     */
    private void copyBlocks(LocatedBlock srcLocatedBlock,
        LocatedBlock destinationLocatedBlock) throws IOException {
      int exceptions = 0;
      IOException lastException = null;
      DatanodeInfo[] destinationBlockLocations = destinationLocatedBlock
          .getLocations();
      for (int i = 0; i < destinationBlockLocations.length; i++) {
        ClientDatanodeProtocol cdp = null;
        try {
          cdp = DFSClient
            .createClientDatanodeProtocolProxy(destinationBlockLocations[i],
                conf, socketTimeout);
          LOG.debug("Fast Copy : Copying block "
              + srcLocatedBlock.getBlock() + " to "
              + destinationLocatedBlock.getBlock() + " on "
              + destinationBlockLocations[i].getHostName());
          cdp.copyBlock(srcLocatedBlock.getBlock(),
              destinationLocatedBlock.getBlock(), destinationBlockLocations[i]);
        } catch (IOException e) {
          exceptions++;
          lastException = e;
          LOG.warn("Fast Copy : Failed for Copying block "
              + srcLocatedBlock.getBlock() + " to "
              + destinationLocatedBlock.getBlock() + " on "
              + destinationBlockLocations[i].getHostName(), e);
        } finally {
          if (cdp != null) {
            RPC.stopProxy(cdp);
          }
        }
      }

      // If copy of all blocks fail, throw an exception.
      if (exceptions == destinationBlockLocations.length) {
        throw lastException;
      }
    }

    /**
     * Waits for all blocks of the file to be copied over.
     *
     * @param src
     *          the source file
     * @param destination
     *          the destination file
     * @throws IOException
     */
    private void waitForBlockCopy(String src, String destination)
        throws IOException {
      boolean flag = namenode.complete(destination, clientName);
      long startTime = System.currentTimeMillis();
      while (!flag) {
        LOG.debug("Fast Copy : Waiting for all blocks of file " + destination
            + " to be replicated");
        try {
          Thread.sleep(5000);
          if (System.currentTimeMillis() - startTime > MAX_WAIT_TIME) {
            throw new IOException("Fast Copy : Could not complete file copy, "
                + "timedout while waiting for blocks to be copied");
          }
        } catch (InterruptedException e) {
          LOG.warn("Fast Copy : Could not sleep thread waiting for file copy "
              + e);
        }
        flag = namenode.complete(destination, clientName);
      }
      LOG.debug("Fast Copy succeeded for files src : " + src + " destination "
          + destination);
    }
  }

  /**
   * Performs a fast copy of the src file to the destination file. This method
   * tries to copy the blocks of the source file on the same local machine and
   * then stitch up the blocks to build the new destination file
   *
   * @param src
   *          the source file, this should be the full HDFS URI
   *
   * @param destination
   *          the destination file, this should be the full HDFS URI
   */
  public void copy(String src, String destination)
      throws Exception {
    Callable<Boolean> fastFileCopy = new FastFileCopy(src, destination);
    Future<Boolean> f = executor.submit(fastFileCopy);
    f.get();
  }

  /**
   * Performs fast copy for a list of fast file copy requests. Uses a thread
   * pool to perform fast file copy in parallel.
   *
   * @param requests
   *          the list of fast file copy requests
   * @throws Exception
   */
  public void copy(List<FastFileCopyRequest> requests) throws Exception {
    List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
    for (FastFileCopyRequest r : requests) {
      Callable<Boolean> fastFileCopy = new FastFileCopy(r.getSrc(),
          r.getDestination());
      Future<Boolean> f = executor.submit(fastFileCopy);
      results.add(f);
    }
    for (Future<Boolean> f : results) {
      f.get();
    }
  }

  public static class FastFileCopyRequest {
    private final String src;
    private final String destination;

    public FastFileCopyRequest(String src, String destination) {
      this.src = src;
      this.destination = destination;
    }

    /**
     * @return the src
     */
    public String getSrc() {
      return src;
    }

    /**
     * @return the destination
     */
    public String getDestination() {
      return destination;
    }
  }

  public static void printUsage() {
    System.err.println("Usage : FastCopy <srcs....> <dst>");
  }

  public static void main(String args[]) throws Exception {
    List<String> srcs = new ArrayList<String>();
    String dst;

    if (args.length < 2) {
      printUsage();
      System.exit(1);
    }

    for (int i = 0; i < args.length - 1; i++) {
      srcs.add(args[i]);
    }
    dst = args[args.length - 1];

    Path dstPath = new Path(dst);
    Configuration conf = new Configuration();
    FileSystem dstFileSys = dstPath.getFileSystem(conf);

    if (dstFileSys.exists(dstPath) && !dstFileSys.getFileStatus(dstPath).isDir()
        && args.length > 2) {
      printUsage();
      throw new IOException("Path : " + dstPath + " is not a directory");
    }

    FileSystem srcFileSys = new Path(srcs.get(0)).getFileSystem(conf);
    List<FastFileCopyRequest> requests = new ArrayList<FastFileCopyRequest>();
    FastCopy fcp = new FastCopy(new Configuration());


    for (String src : srcs) {
      try {
        // Perform some error checking and path manipulation.
        Path srcPath = new Path(src);

        if (!srcFileSys.exists(srcPath)) {
          throw new IOException("File : " + src + " does not exists on "
              + srcFileSys);
        }

        String destination = dst;
        // Destination is a directory ? Then copy into the directory.
        if (dstFileSys.exists(dstPath)
            && dstFileSys.getFileStatus(dstPath).isDir()) {
          destination = new Path(dstPath, srcPath.getName()).toUri().toString();
        }
        requests.add(new FastFileCopyRequest(src, destination));
      } catch (Exception e) {
        LOG.warn("Fast Copy failed : ", e);
      }
      fcp.copy(requests);
    }
    LOG.info("Finished copying");
    System.exit(0);
  }
}
