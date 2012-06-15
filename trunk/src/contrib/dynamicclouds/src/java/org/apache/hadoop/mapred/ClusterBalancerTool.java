package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ClusterBalancerTool extends Configured implements Tool {

  private boolean rebalanceClusters = false;
  private boolean moveMachines = false;
  private boolean getStatus = false;

  public void displayUsage(String cmd) {
    String prefix = "Usage: ClusterBalancerTool ";
    if (cmd.equals("-rebalance")) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if (cmd.equals("-move")) {
      System.err.println(prefix + "[" + cmd + " <from-cluster-name> " +
              "<to-cluster-name> <num-machines>]");
    } else if (cmd.endsWith("status")) {
      System.err.println(prefix + "[ " + cmd + " <cluster-name>]");
    } else {
      System.err.println(prefix + " <command> <args>");
      System.err.println("\t[-move <from-cluster-name> <to-cluster-name> " +
              "<num-machines>]");
      System.err.println("\t[-rebalance]");
      System.err.println("\t[-status <cluster-name>]");
    }
  }

  public int run(String[] args) throws Exception {
    int exitCode = -1;
    if (args.length < 1) {
      displayUsage("");
      return exitCode;
    }

    String cmd = args[0];

    if (cmd.equals("-move")) {
      if (args.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      moveMachines = true;
    } else if (cmd.equals("-rebalance")) {
      if (args.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      rebalanceClusters = true;
    } else if (cmd.equals("-status")) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      getStatus = true;
    } else {
      displayUsage(cmd);
      return exitCode;
    }

    if (rebalanceClusters) {
      exitCode = getClusterBalancerAdmin().rebalance();
    } else if (moveMachines) {
      exitCode = getClusterBalancerAdmin().moveMachines(args[1], args[2],
              Integer.valueOf(args[3]));
    } else if (getStatus) {
      String status = getClusterBalancerAdmin().getCurrentStatus(args[1]);
      System.out.println(status);
      exitCode = 0;
    }
    return exitCode;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    Configuration.addDefaultResource(
            DynamicCloudsDaemon.CLUSTER_BALANCER_CONF_FILE);
  }

  private ClusterBalancerAdminProtocol getClusterBalancerAdmin()
          throws IOException {
    Configuration.addDefaultResource(
            DynamicCloudsDaemon.CLUSTER_BALANCER_CONF_FILE);
    Configuration conf = this.getConf();
    String addr = conf.get(DynamicCloudsDaemon.CLUSTER_BALANCER_ADDR);

    InetSocketAddress isAddr = NetUtils.createSocketAddr(addr);

    return (ClusterBalancerAdminProtocol) RPC.getProxy(
            ClusterBalancerAdminProtocol.class,
            ClusterBalancerAdminProtocol.versionID, isAddr, conf);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ClusterBalancerTool(), args);
    System.exit(res);
  }
}
