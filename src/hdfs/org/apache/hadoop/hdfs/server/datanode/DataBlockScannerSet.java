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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.metrics.DatanodeThreadLivenessReporter.BackgroundThread;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * DataBlockScannerSet manages block scanning for all the namespaces For each
 * namespace a {@link DataBlockScannerScanner} is created to scan the blocks for
 * that block namespace. When a {@link NSOfferService} becomes alive or dies,
 * namespaceScannerMap in this class is updated.
 */
public class DataBlockScannerSet implements Runnable {
  public static final Log LOG = LogFactory.getLog(DataBlockScannerSet.class);
  
  public static long TIME_SLEEP_BETWEEN_SCAN = 5000;
  
  private final DataNode datanode;
  private final FSDataset dataset;
  private final Configuration conf;

  /**
   * Map to find the BlockDataScanner for a given namespaceId. This is updated
   * when a NSOfferService becomes alive or dies.
   */
  private final TreeMap<Integer, DataBlockScanner> namespaceScannerMap = new TreeMap<Integer, DataBlockScanner>();
  Thread blockScannerThread = null;
  private boolean initialized = false;

  DataBlockScannerSet(DataNode datanode, FSDataset dataset, Configuration conf) {
    this.datanode = datanode;
    this.dataset = dataset;
    this.conf = conf;
  }

  public void run() {
    try {
      int currentNamespaceId = -1;
      boolean firstRun = true;
      while (datanode.shouldRun && !Thread.interrupted()) {
        datanode.updateAndReportThreadLiveness(BackgroundThread.BLOCK_SCANNER);
        
        // Sleep everytime except in the first interation.
        if (!firstRun) {
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Sleep " + TIME_SLEEP_BETWEEN_SCAN
                  + " ms before next round of scanning");
            }
            Thread.sleep(TIME_SLEEP_BETWEEN_SCAN);
          } catch (InterruptedException ex) {
            // Interrupt itself again to set the interrupt status
            blockScannerThread.interrupt();
            continue;
          }
        } else {
          firstRun = false;
        }

        DataBlockScanner nsScanner = getNextNamespaceSliceScanner(currentNamespaceId);
        if (nsScanner == null) {
          // Possible if thread is interrupted
          continue;
        }
        currentNamespaceId = nsScanner.getNamespaceId();
        waitForUpgradeDone(currentNamespaceId);

        if (!datanode.isNamespaceAlive(currentNamespaceId)) {
          LOG.warn("Namespace: " + currentNamespaceId + " is not alive");
          // Remove in case NS service died abruptly without proper shutdown
          removeNamespace(currentNamespaceId);
          continue;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Start scan namespace " + currentNamespaceId);
        }
        nsScanner.scanNamespace();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finish scan namespace " + currentNamespaceId);
        }
      }
    } finally {
      LOG.info("DataBlockScannerSet exited...");
    }
  }

  private void waitForOneNameSpaceUp() {
    while (getNamespaceSetSize() < 1 && datanode.shouldRun) {
      try {
        Thread.sleep(5000);
        LOG.info("sleeping ............");
      } catch (InterruptedException e) {
        blockScannerThread.interrupt();
        return;
      }
    }
  }

  // Wait for upgrading done for the given namespace
  private void waitForUpgradeDone(int namespaceId) {
    UpgradeManagerDatanode um = datanode.getUpgradeManager(namespaceId);
    while (!um.isUpgradeCompleted()) {
      try {
        datanode.updateAndReportThreadLiveness(BackgroundThread.BLOCK_SCANNER);
        Thread.sleep(5000);
        LOG.info("sleeping ............");
      } catch (InterruptedException e) {
        blockScannerThread.interrupt();
        return;
      }
    }
  }

  /**
   * Find next namespaceId to scan. There should be only one current
   * verification log file. Find which namespace contains the current
   * verification log file and that is used as the starting namespaceId. If no
   * current files are found start with first namespace. However, if more than
   * one current files are found, the one with latest modification time is used
   * to find the next namespaceId.
   */
  private DataBlockScanner getNextNamespaceSliceScanner(int currentNamespaceId) {

    Integer nextNsId = null;
    while ((nextNsId == null) && datanode.shouldRun
        && !blockScannerThread.isInterrupted()) {
      waitForOneNameSpaceUp();
      synchronized (this) {
        if (getNamespaceSetSize() > 0) {
          // Find nextNsId by finding the last modified current log file, if any
          long lastScanTime = -1;
          Iterator<Integer> nsidIterator = namespaceScannerMap.keySet()
              .iterator();
          while (nsidIterator.hasNext()) {
            int nsid = nsidIterator.next();
            for (FSDataset.FSVolume vol : dataset.volumes.getVolumes()) {
              try {
                File currFile = DataBlockScanner.getCurrentFile(vol, nsid);
                if (currFile.exists()) {
                  long lastModified = currFile.lastModified();
                  if (lastScanTime < lastModified) {
                    lastScanTime = lastModified;
                    nextNsId = nsid;
                  }
                }
              } catch (IOException e) {
                LOG.warn("Received exception: ", e);
              }
            }
          }

          // nextNsId can still be -1 if no current log is found,
          // find nextNsId sequentially.
          if (nextNsId == null) {
            try {
              if (currentNamespaceId == -1) {
                nextNsId = namespaceScannerMap.firstKey();
              } else {
                nextNsId = namespaceScannerMap.higherKey(currentNamespaceId);
                if (nextNsId == null) {
                  nextNsId = namespaceScannerMap.firstKey();
                }
              }
            } catch (NoSuchElementException e) {
              // if firstKey throws an exception
              continue;
            }
          }
          if (nextNsId != null) {
            return getNSScanner(nextNsId);
          }
        }
      }
      LOG.warn("No namespace is up, going to wait");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ex) {
        LOG.warn("Received exception: " + ex);
        blockScannerThread.interrupt();
        return null;
      }
    }
    return null;
  }

  private synchronized int getNamespaceSetSize() {
    return namespaceScannerMap.size();
  }

  private synchronized DataBlockScanner getNSScanner(int namespaceId) {
    return namespaceScannerMap.get(namespaceId);
  }

  private synchronized Integer[] getNsIdList() {
    return namespaceScannerMap.keySet().toArray(
        new Integer[namespaceScannerMap.keySet().size()]);
  }

  public void addBlock(int namespaceId, Block block) {
    DataBlockScanner nsScanner = getNSScanner(namespaceId);
    if (nsScanner != null) {
      nsScanner.addBlock(block);
    } else {
      LOG.warn("No namespace scanner found for namespace id: " + namespaceId);
    }
  }

  public synchronized boolean isInitialized(int namespaceId) {
    DataBlockScanner nsScanner = getNSScanner(namespaceId);
    if (nsScanner != null) {
      return nsScanner.isInitialized();
    }
    return false;
  }

  public synchronized void printBlockReport(StringBuilder buffer,
      boolean summary) {
    Integer[] nsIdList = getNsIdList();
    if (nsIdList == null || nsIdList.length == 0) {
      buffer.append("Periodic block scanner is not yet initialized. "
          + "Please check back again after some time.");
      return;
    }
    for (Integer nsId : nsIdList) {
      DataBlockScanner nsScanner = getNSScanner(nsId);
      buffer.append("\n\nBlock report for namespace: " + nsId + "\n");
      nsScanner.printBlockReport(buffer, summary);
      buffer.append("\n");
    }
  }

  public void deleteBlock(int namespaceId, Block toDelete) {
    DataBlockScanner nsScanner = getNSScanner(namespaceId);
    if (nsScanner != null) {
      nsScanner.deleteBlock(toDelete);
    } else {
      LOG.warn("No namespace scanner found for namespaceId: " + nsScanner);
    }
  }

  public void deleteBlocks(int namespaceId, Block[] toDelete) {
    DataBlockScanner nsScanner = getNSScanner(namespaceId);
    if (nsScanner != null) {
      nsScanner.deleteBlocks(toDelete);
    } else {
      LOG.warn("No namespace scanner found for namespaceId: " + nsScanner);
    }
  }

  public synchronized void shutdown() {
    if (blockScannerThread != null) {
      blockScannerThread.interrupt();
    }
  }

  public synchronized void addNamespace(int namespaceId) {
    if (namespaceScannerMap.get(namespaceId) != null) {
      return;
    }
    DataBlockScanner nsScanner = new DataBlockScanner(datanode, dataset, conf,
        namespaceId);
    try {
      nsScanner.init();
    } catch (IOException ex) {
      LOG.warn("Failed to initialized block scanner for namespace id="
          + namespaceId);
      return;
    }
    namespaceScannerMap.put(namespaceId, nsScanner);
    LOG.info("Added namespaceId=" + namespaceId
        + " to namespaceScannerMap, new size=" + namespaceScannerMap.size());
  }

  public synchronized void removeNamespace(int namespaceId) {
    namespaceScannerMap.remove(namespaceId);
    LOG.info("Removed namespaceId=" + namespaceId + " from namespaceScannerMap");
  }

  public synchronized void start() {
    if(initialized ){
      return;
    }
    initialized = true;
    blockScannerThread = new Thread(this);
    blockScannerThread.setDaemon(true);
    blockScannerThread.start();
  }

  public static class Servlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      response.setContentType("text/plain");

      DataNode datanode = (DataNode) getServletContext().getAttribute(
          "datanode");
      DataBlockScannerSet blockScanner = datanode.blockScanner;

      boolean summary = (request.getParameter("listblocks") == null);

      StringBuilder buffer = new StringBuilder(8 * 1024);
      if (blockScanner == null) {
        LOG.warn("Periodic block scanner is not running");
        buffer.append("Periodic block scanner is not running. "
            + "Please check the datanode log if this is unexpected.");
      } else {
        blockScanner.printBlockReport(buffer, summary);
      }
      response.getWriter().write(buffer.toString()); // extra copy!
    }
  }

  public long getLastScanTime(int namespaceId, Block block) {
    DataBlockScanner nsScanner = getNSScanner(namespaceId);
    if (nsScanner != null) {
      return nsScanner.getLastScanTime(block);
    } else {
      LOG.warn("No namespace scanner found for namespaceId: " + nsScanner);
      return -1;
    }
  }

  public int getBlockMapSize(int namespaceId) {
    DataBlockScanner nsScanner = getNSScanner(namespaceId);
    if (nsScanner != null) {
      return nsScanner.getBlockCount();
    } else {
      LOG.warn("No namespace scanner found for namespaceId: " + nsScanner);
      return -1;
    }
  }

  /*
   * A reader will try to indicate a block is verified and will add blocks to
   * the DataBlockScannerSet before they are finished (due to concurrent
   * readers).
   * 
   * fixed so a read verification can't add the block
   */
  synchronized void verifiedByClient(int namespaceId, Block block) {
    DataBlockScanner nsScanner = getNSScanner(namespaceId);
    if (nsScanner != null) {
      nsScanner.updateScanStatusUpdateOnly(block,
          DataBlockScanner.ScanType.REMOTE_READ, true);
    } else {
      LOG.warn("No namespace scanner found for namespaceId: " + nsScanner);
    }
  }

}
