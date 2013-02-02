/*
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

package org.apache.hadoop.corona;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CoronaNodeRestarter extends Thread {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(CoronaNodeRestarter.class);

  enum RestartStatus {
    INITIAL,
    READY,
    DONE,
  }

  public static class NodeRestartInfo {

    ClusterNode node;
    RestartStatus status;

    NodeRestartInfo(ClusterNode node) {
      this.node = node;
      status = RestartStatus.INITIAL;
    }
  }

  private final CoronaConf conf;
  private final NodeManager nodeManager;
  // The interval that each bacth will be set to be restarted
  private final long restartInterval;
  // All the nodes will be set to be restarted in batches
  private int restartBatch;
  private List<NodeRestartInfo> workingList;

  public CoronaNodeRestarter(CoronaConf conf, NodeManager nodeManager) {
    this.conf = conf;
    this.nodeManager = nodeManager;
    this.restartBatch = 1000;
    this.restartInterval = conf.getCoronaNodeRestartInterval();
    workingList = new ArrayList<NodeRestartInfo>();
  }

  public boolean checkStatus(ClusterNodeInfo nodeToCheck) {
    synchronized (workingList) {
      for (NodeRestartInfo workingNode: workingList) {
        if (workingNode.node.getName().toString().equals(
          nodeToCheck.getName().toString())) {
          if (workingNode.status == RestartStatus.READY) {
            // nodeManager.deleteNode is used instead of ClusterManager.nodeTimeout
            // due to potential deadlock since that one calls delete()
            nodeManager.deleteNode(workingNode.node);
            workingNode.status = RestartStatus.DONE;
            LOG.info("Notify " + nodeToCheck.getName().toString() +
              " to restart");
            return true;
          }
        }
      }
    }
    return false;
  }

  public void delete(String nodeToDelete) {
    synchronized (workingList) {
      Iterator<NodeRestartInfo> workingIt = workingList.iterator();
      while (workingIt.hasNext()) {
        NodeRestartInfo tmpnode = workingIt.next();
        if (tmpnode.node.getName().toString().equals(nodeToDelete)) {
          LOG.info("Delete " + nodeToDelete + " from the working list");
          workingIt.remove();
          break;
        }
      }
    }
  }

  public void add(List<ClusterNode> nodesToKill, boolean forceFlag,
    int batchSize) {
    synchronized (workingList) {
      this.restartBatch = batchSize;
      if (forceFlag) {
        workingList.clear();
      }
      for (ClusterNode node: nodesToKill) {
        workingList.add(new NodeRestartInfo(node));
      }
      this.interrupt();
    }
  }

  @Override
  public void run() {
    while (true) {
      synchronized (workingList) {
        try {
          if (workingList.size() == 0) {
            workingList.wait();
          }
        } catch (InterruptedException e) {
        }
        int currentIndex = 0;
        int changed = 0;
        while (currentIndex < workingList.size() && changed < restartBatch) {
          NodeRestartInfo workingNode = workingList.get(currentIndex);
          if (workingNode.status == RestartStatus.INITIAL) {
            workingNode.status = RestartStatus.READY;
            LOG.info("set " + workingNode.node.getName().toString() +
              " to be ready for restart");
            changed++;
          }
          currentIndex++;
        }
        // cleanup the notified ones
        Iterator<NodeRestartInfo> workingIt = workingList.iterator();
        while (workingIt.hasNext()) {
          NodeRestartInfo tmpnode = workingIt.next();
          if (tmpnode.status == RestartStatus.DONE) {
            workingIt.remove();
          }
        }
      }

      try {
        Thread.sleep(restartInterval);
      } catch (InterruptedException e) {
      }
    }
  }
}
