package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.protocal.FairSchedulerProtocol;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Contains a main function that uses RPC client to control the slots of
 * {@link FairScheduler}
 */
public class FairSchedulerShell extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(FairSchedulerShell.class);
  private FairSchedulerProtocol client = null;

  public FairSchedulerShell() throws IOException {
    super(null);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      try {
        initialize();
      } catch (IOException e) {
        // Failed to connect to server
        e.printStackTrace();
      }
    }
  }

  private void initialize() throws IOException {
    if (client != null) {
      return;
    }
    InetSocketAddress address = FairScheduler.getAddress(getConf());
    UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
    client = createClient(address, getConf(), ugi);
  }
  
  private static FairSchedulerProtocol createClient(InetSocketAddress addr,
      Configuration conf, UserGroupInformation ugi)
      throws IOException {
    return (FairSchedulerProtocol) RPC.getProxy(FairSchedulerProtocol.class,
        FairSchedulerProtocol.versionID, addr, ugi, conf,
        NetUtils.getSocketFactory(conf, FairSchedulerProtocol.class));
  }

  public int getFSMaxSlots(String trackerName, TaskType type)
      throws IOException {
    return client.getFSMaxSlots(trackerName, type);
  }


  public void setFSMaxSlots(String trackerName, TaskType type, int slots)
      throws IOException {
    client.setFSMaxSlots(trackerName, type, slots);
  }

  public void resetFSMaxSlots() throws IOException {
    client.resetFSMaxSlots();
  }

  public int[] getPoolMaxTasks(String pool) throws IOException {
    return client.getPoolMaxTasks(pool);
  }

  public int[] getPoolRunningTasks(String pool) throws IOException {
    return client.getPoolRunningTasks(pool);
  }

  /**
   * Displays format of commands.
   */
  private static void printUsage(String cmd) {
    String prefix = "Usage: java " + FairSchedulerShell.class.getSimpleName();
    if ("-getfsmaxslots".equalsIgnoreCase(cmd)) {
      System.err.println(prefix + " [-getfsmaxslots trackerName1, trackerName2 ... ]");
    } else if ("-setfsmaxslots".equalsIgnoreCase(cmd)) {
      System.err.println(prefix + " [-setfsmaxslots trackerName #maps #reduces trackerName2 #maps #reduces ... ]");
    } else {
      System.err.println(prefix);
      System.err.println("          [-setfsmaxslots trackerName #maps #reduces trackerName2 #maps #reduces ... ]");
      System.err.println("          [-getfsmaxslots trackerName1, trackerName2 ... ]");
      System.err.println("          [-resetfsmaxslots]");
      System.err.println("          [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  @Override
  public int run(String[] arg) throws Exception {
    int curr = 0;
    if (arg.length == 0) {
      printUsage("");
      return -1;
    }
    if (arg[curr].equalsIgnoreCase("-help")) {
      curr += 1;
      if (arg.length == curr) {
        printUsage("");
        return 0;
      }
      printUsage(arg[curr]);
      return 0;
    }
    if (arg[curr].equalsIgnoreCase("-resetfsmaxslots")) {
      resetFSMaxSlots();
      return 0;
    }
    if (arg[curr].equalsIgnoreCase("-getfsmaxslots")) {
      curr += 1;
      if (arg.length == curr) {
        printUsage(arg[0]);
        return -1;
      }
      for (int i = curr; i < arg.length; ++i) {
        String trackerName = arg[i];
        int maxMap = getFSMaxSlots(trackerName, TaskType.MAP);
        int maxReduce = getFSMaxSlots(trackerName, TaskType.REDUCE);
        System.out.println(trackerName + " " + maxMap + " " +  maxReduce);
      }
      return 0;
    }
    if (arg[curr].equalsIgnoreCase("-setfsmaxslots")) {
      curr += 1;
      if (arg.length <= curr + 2) {
        printUsage(arg[0]);
        return -1;
      }
      for (int i = curr; i < arg.length; i += 3) {
        String trackerName = arg[i];
        int maxMap = Integer.parseInt(arg[i + 1]);
        int maxReduce = Integer.parseInt(arg[i + 2]);
        setFSMaxSlots(trackerName, TaskType.MAP, maxMap);
        setFSMaxSlots(trackerName, TaskType.REDUCE, maxReduce);
        System.out.println("set " + trackerName + " " +
            maxMap + " " +  maxReduce);
      }
      return 0;
    }
    if (arg[curr].equalsIgnoreCase("-pooltasks")) {
      curr += 1;
      if (arg.length == curr) {
        printUsage(arg[0]);
        return -1;
      }
      String pool = arg[curr];
      int[] result = getPoolRunningTasks(pool);
      System.out.println(pool + " " + result[0] + " " + result[1]);
      return 0;
    }
    if (arg[curr].equalsIgnoreCase("-poolmaxtasks")) {
      curr += 1;
      if (arg.length == curr) {
        printUsage(arg[0]);
        return -1;
      }
      String pool = arg[curr];
      int[] result = getPoolMaxTasks(pool);
      System.out.println(pool + " " + result[0] + " " + result[1]);
      return 0;
    }
    printUsage("");
    return -1;
  }

  public static void main(String argv[]) throws Exception {
    FairSchedulerShell shell = null;
    try {
      shell = new FairSchedulerShell();
      int res = ToolRunner.run(shell, argv);
      System.exit(res);
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      System.exit(-1);
    } catch (IOException e) {
      System.err.
        println("Bad connection to FairScheduler. command aborted.");
      System.err.println(e.getMessage());
      System.exit(-1);
    }
  }

}
