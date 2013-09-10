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
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.lang.reflect.Proxy;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithFileName;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class provides some DFS administrative access.
 */
public class DFSAdmin extends FsShell {

  protected DistributedFileSystem getDFS() throws IOException{
    FileSystem fs = getFS();
    DistributedFileSystem dfs = null;
    if (fs instanceof DistributedFileSystem) {
      dfs = (DistributedFileSystem) fs;
    } else {
      if (fs instanceof FilterFileSystem) {
        FilterFileSystem ffs = (FilterFileSystem) fs;
        if (ffs.getRawFileSystem() instanceof DistributedFileSystem) {
          dfs = (DistributedFileSystem) ffs.getRawFileSystem();
        }
      }
    }
    return dfs;
  }

  /**
   * An abstract class for the execution of a file system command
   */
  abstract private static class DFSAdminCommand extends Command {
    final DistributedFileSystem dfs;
    /** Constructor */
    public DFSAdminCommand(FileSystem fs) {
      super(fs.getConf());
      DistributedFileSystem dfs = DFSUtil.convertToDFS(fs);      
      if (dfs == null) {
        throw new IllegalArgumentException("FileSystem " + fs.getUri() + 
            " is not a distributed file system");
      }
      this.dfs = dfs;
    }
  }
  
  /** A class that supports command recount */
  private static class RecountCommand extends DFSAdminCommand {
    private static final String NAME = "recount";
    private static final String USAGE = 
        "-"+NAME+" [-service serviceName]";
    private static final String DESCRIPTION = USAGE + ": " +
    "Repopulate the namesapce & diskspace usage for every quotaed node.\n";
    
    /** Constructor */
    RecountCommand(FileSystem fs) {
      super(fs);
      args = null;
    }
    
    /** Check if a command is the recount command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.recount();
    }    
  }
  
  /** A class that supports command clearQuota */
  private static class ClearQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "clrQuota";
    private static final String USAGE = 
        "-"+NAME+" <dirname>...<dirname> [-service serviceName]";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the quota for each directory <dirName>.\n" +
    "\t\tBest effort for the directory. with fault reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** Constructor */
    ClearQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 1, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the clrQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, FSConstants.QUOTA_RESET, FSConstants.QUOTA_DONT_SET);
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname> [-service serviceName]";
    private static final String DESCRIPTION = 
      "-setQuota <quota> <dirname>...<dirname> [-service serviceName]: " +
      "Set the quota <quota> for each directory <dirName>.\n" + 
      "\t\tThe directory quota is a long integer that puts a hard limit\n" +
      "\t\ton the number of names in the directory tree\n" +
      "\t\tBest effort for the directory, with faults reported if\n" +
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. user is not an administrator, or\n" +
      "\t\t3. the directory does not exist or is a file, or\n";

    private final long quota; // the quota to be set
    
    /** Constructor */
    SetQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.quota = Long.parseLong(parameters.remove(0));
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, quota, FSConstants.QUOTA_DONT_SET);
    }
  }
  
  /** A class that supports command clearSpaceQuota */
  private static class ClearSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "clrSpaceQuota";
    private static final String USAGE = 
        "-"+NAME+" <dirname>...<dirname> [-service serviceName]";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the disk space quota for each directory <dirName>.\n" +
    "\t\tBest effort for the directory. with fault reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** Constructor */
    ClearSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 1, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the clrQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, FSConstants.QUOTA_DONT_SET, FSConstants.QUOTA_RESET);
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setSpaceQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname> [-service serviceName]";
    private static final String DESCRIPTION = USAGE + ": " +
      "Set the disk space quota <quota> for each directory <dirName>.\n" + 
      "\t\tThe space quota is a long integer that puts a hard limit\n" +
      "\t\ton the total size of all the files under the directory tree.\n" +
      "\t\tThe extra space required for replication is also counted. E.g.\n" +
      "\t\ta 1GB file with replication of 3 consumes 3GB of the quota.\n\n" +
      "\t\tQuota can also be speciefied with a binary prefix for terabytes,\n" +
      "\t\tpetabytes etc (e.g. 50t is 50TB, 5m is 5MB, 3p is 3PB).\n" + 
      "\t\tBest effort for the directory, with faults reported if\n" +
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. user is not an administrator, or\n" +
      "\t\t3. the directory does not exist or is a file, or\n";

    private long quota; // the quota to be set
    
    /** Constructor */
    SetSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      String str = parameters.remove(0).trim();
      quota = StringUtils.TraditionalBinaryPrefix.string2long(str);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, FSConstants.QUOTA_DONT_SET, quota);
    }
  }
  
  /**
   * Construct a DFSAdmin object.
   */
  public DFSAdmin() {
    this(null);
  }

  /**
   * Construct a DFSAdmin object.
   */
  public DFSAdmin(Configuration conf) {
    super(conf);
  }
  
  /**
   * Gives a report on how the FileSystem is doing.
   * @exception IOException if the filesystem does not exist.
   */
  public void report() throws IOException {
    DistributedFileSystem dfs = getDFS();
    if (dfs != null) {
      DiskStatus ds = dfs.getDiskStatus();
      long capacity = ds.getCapacity();
      long used = ds.getDfsUsed();
      long remaining = ds.getRemaining();
      long presentCapacity = used + remaining;
      boolean mode = dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_GET);
      UpgradeStatusReport status = 
                      dfs.distributedUpgradeProgress(UpgradeAction.GET_STATUS);

      if (mode) {
        System.out.println("Safe mode is ON");
      }
      if (status != null) {
        System.out.println(status.getStatusText(false));
      }
      System.out.println("Configured Capacity: " + capacity
                         + " (" + StringUtils.byteDesc(capacity) + ")");
      System.out.println("Present Capacity: " + presentCapacity
          + " (" + StringUtils.byteDesc(presentCapacity) + ")");
      System.out.println("DFS Remaining: " + remaining
          + " (" + StringUtils.byteDesc(remaining) + ")");
      System.out.println("DFS Used: " + used
                         + " (" + StringUtils.byteDesc(used) + ")");
      System.out.println("DFS Used%: "
                         + StringUtils.limitDecimalTo2(((1.0 * used) / presentCapacity) * 100)
                         + "%");
      
      /* These counts are not always upto date. They are updated after  
       * iteration of an internal list. Should be updated in a few seconds to 
       * minutes. Use "-metaSave" to list of all such blocks and accurate 
       * counts.
       */
      System.out.println("Under replicated blocks: " + 
                         dfs.getUnderReplicatedBlocksCount());
      System.out.println("Blocks with corrupt replicas: " + 
                         dfs.getCorruptBlocksCount());
      System.out.println("Missing blocks: " + 
                         dfs.getMissingBlocksCount());
                           
      System.out.println();

      System.out.println("-------------------------------------------------");
      
      DatanodeInfo[] live = dfs.getClient().datanodeReport(
                                                   DatanodeReportType.LIVE);
      DatanodeInfo[] dead = dfs.getClient().datanodeReport(
                                                   DatanodeReportType.DEAD);
      System.out.println("Datanodes available: " + live.length +
                         " (" + (live.length + dead.length) + " total, " + 
                         dead.length + " dead)\n");
      
      for (DatanodeInfo dn : live) {
        System.out.println(dn.getDatanodeReport());
        System.out.println();
      }
      for (DatanodeInfo dn : dead) {
        System.out.println(dn.getDatanodeReport());
        System.out.println();
      }      
    }
  }

  /**
   * Safe mode maintenance command.
   * Usage: java DFSAdmin -safemode [enter | leave | get | wait | initqueues]
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException if the filesystem does not exist.
   */
  public void setSafeMode(String[] argv, int idx) throws IOException {
    DistributedFileSystem dfs = getDFS();
    if (dfs == null) {
      System.err.println("FileSystem is " + getFS().getUri());
      return;
    }
    if (idx != argv.length - 1) {
      printUsage("-safemode");
      return;
    }
    FSConstants.SafeModeAction action;
    Boolean waitExitSafe = false;

    if ("leave".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_LEAVE;
    } else if ("enter".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_ENTER;
    } else if ("get".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_GET;
    } else if ("initqueues".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_INITQUEUES;
    } else if ("wait".equalsIgnoreCase(argv[idx])) {
      action = FSConstants.SafeModeAction.SAFEMODE_GET;
      waitExitSafe = true;
    } else {
      printUsage("-safemode");
      return;
    }
    boolean inSafeMode = dfs.setSafeMode(action);

    //
    // If we are waiting for safemode to exit, then poll and
    // sleep till we are out of safemode.
    //
    if (waitExitSafe) {
      while (inSafeMode) {
        try {
          Thread.sleep(5000);
        } catch (java.lang.InterruptedException e) {
          throw new IOException("Wait Interrupted");
        }
        inSafeMode = dfs.setSafeMode(action);
      }
    }

    System.out.println("Safe mode is " + (inSafeMode ? "ON" : "OFF"));
  }

  /**
   * Command to ask the namenode to save the namespace.
   * Usage: java DFSAdmin -saveNamespace [force] [uncompressed]
   *
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException 
   */
  public int saveNamespace(String[] argv, int idx) throws IOException {
    int exitCode = -1;

    DistributedFileSystem dfs = getDFS();
    if (dfs == null) {
      System.err.println("FileSystem is " + getFS().getUri());
      return exitCode;
    }

    boolean force = false;
    boolean uncompressed = false;
    for( ; idx < argv.length; idx++) {
      if (argv[idx].equals("force")) {
        force = true;
      } else if (argv[idx].equals("uncompressed")) {
        uncompressed = true;
      } else {
        printUsage("saveNamespace");
        return exitCode;
      }
    }
    dfs.saveNamespace(force, uncompressed);
    return 0;
  }

  /**
   * Command to ask the namenode to reread the hosts and excluded hosts 
   * file.
   * Usage: java DFSAdmin -refreshNodes
   * @exception IOException 
   */
  public int refreshNodes() throws IOException {
    int exitCode = -1;

    DistributedFileSystem dfs = getDFS();
    if (dfs == null) {
      System.err.println("FileSystem is " + getFS().getUri());
      return exitCode;
    }

    dfs.refreshNodes();
    exitCode = 0;
   
    return exitCode;
  }
  
  /**
   * Roll edit log at the namenode manually.
   */
  public int rollEditLog() throws IOException {
    int exitCode = -1;

    DistributedFileSystem dfs = getDFS();
    if (dfs == null) {
      System.err.println("FileSystem is " + getFS().getUri());
      return exitCode;
    }

    dfs.rollEditLog();
    exitCode = 0;
   
    return exitCode;
  }

  private void printHelp(String cmd) {
    String summary = "hadoop dfsadmin is the command to execute DFS administrative commands.\n" +
      "The full syntax is: \n\n" +
      "hadoop dfsadmin [-report] [-service serviceName]\n" +
      "\t[-safemode <enter | leave | get | wait | initqueues>] [-service serviceName]\n" +
      "\t[-saveNamespace [force] [uncompressed] [-service serviceName]\n" +
      "\t[-refreshNodes] [-service serviceName]\n" +
      "\t[-refreshOfferService -service serviceName]" +
      "\t[" + SetQuotaCommand.USAGE + "] \n" +
      "\t[" + ClearQuotaCommand.USAGE +"] \n" +
      "\t[" + SetSpaceQuotaCommand.USAGE + "] \n" +
      "\t[" + ClearSpaceQuotaCommand.USAGE +"] \n" +
      "\t[-refreshServiceAcl] [-service serviceName]\n" +
      "\t[-refreshNamenodes] datanodehost:port\n" +
      "\t[-removeNamespace nameserviceId [datanodehost:port]]" +
      "\t[-refreshDatanodeDataDirs] [confFilePath | --defaultPath] [datanodehost:port]\n" +
      "\t[-help [cmd]]\n";

    String report ="-report [-service serviceName]: " + 
      "\tReports basic filesystem information and statistics.\n";
        
    String safemode = "-safemode <enter|leave|get|wait|initqueues> [-service serviceName]: " +
      "\tSafe mode maintenance command.\n" + 
      "\t\tSafe mode is a Namenode state in which it\n" +
      "\t\t\t1.  does not accept changes to the name space (read-only)\n" +
      "\t\t\t2.  does not replicate or delete blocks.\n" +
      "\t\tSafe mode is entered automatically at Namenode startup, and\n" +
      "\t\tleaves safe mode automatically when the configured minimum\n" +
      "\t\tpercentage of blocks satisfies the minimum replication\n" +
      "\t\tcondition.  Safe mode can also be entered manually, but then\n" +
      "\t\tit can only be turned off manually as well.\n";

    String saveNamespace = "-saveNamespace [force] [uncompressed] [-service serviceName]:\t" +
    "Save current namespace into storage directories and reset edits log.\n" +
    "\t\tRequires superuser permissions.\n" +
    "\t\tIf force is not specified that it requires namenode to already be in safe mode.\n" +
    "\t\tIf force is specified that namenode need not be in safe mode.\n" +
    "\t\tIf uncompressed is specified that namespace will be saved uncompressed.\n" +
    "\t\tIf uncompressed is not specified that namespace will be saved " +
    " in a format specified in namenode configuration.\n";

    String refreshNodes = "-refreshNodes [-service serviceName]: " +
    "\tUpdates the namenode with the " +
    "set of datanodes allowed to connect to the namenode.\n" +
    "\t\tNamenode re-reads datanode hostnames from the file defined by \n" +
    "\t\tdfs.hosts, dfs.hosts.exclude configuration parameters.\n" +
    "\t\tHosts defined in dfs.hosts are the datanodes that are part of \n" +
    "\t\tthe cluster. If there are entries in dfs.hosts, only the hosts \n" +
    "\t\tin it are allowed to register with the namenode.\n" +
    "\t\tEntries in dfs.hosts.exclude are datanodes that need to be \n" +
    "\t\tdecommissioned. Datanodes complete decommissioning when \n" + 
    "\t\tall the replicas from them are replicated to other datanodes.\n" +
    "\t\tDecommissioned nodes are not automatically shutdown and \n" +
    "\t\tare not chosen for writing new replicas.\n"; 

    String finalizeUpgrade = "-finalizeUpgrade [-service serviceName]: " + 
      "\tFinalize upgrade of HDFS.\n" +
      "\t\tDatanodes delete their previous version working directories,\n" +
      "\t\tfollowed by Namenode doing the same.\n" + 
      "\t\tThis completes the upgrade process.\n";

    String upgradeProgress = "-upgradeProgress <status|details|force> " +
      "[-service serviceName]: \n" +
      "\t\trequest current distributed upgrade status, \n" +
      "\t\ta detailed status or force the upgrade to proceed.\n";

    String metaSave = "-metasave <filename> [-service serviceName]:" + 
      "\tSave Namenode's primary data structures\n" +
      "\t\tto <filename> in the directory specified by hadoop.log.dir property.\n" +
      "\t\t<filename> will contain one line for each of the following\n" +
      "\t\t\t1. Datanodes heart beating with Namenode\n" +
      "\t\t\t2. Blocks waiting to be replicated\n" +
      "\t\t\t3. Blocks currrently being replicated\n" +
      "\t\t\t4. Blocks waiting to be deleted\n";

    String blockReplication = "-blockReplication <enable|disable>: " +
      "\tEnable/Disable Block Replication\n" +
      "\t\tNameNode will start/stop replicating UnderReplicatedBlocks\n";

    String refreshServiceAcl = "-refreshServiceAcl [-service serviceName]:" +
      "\tReload the service-level authorization policy file\n" +
      "\t\tNamenode will reload the authorization policy file.\n";

    String refreshNamenodes = "-refreshNamenodes datanodehost:port: \tGiven datanode reloads\n" +
      "\t\tthe configuration file, starts serving new namenodes and stops serving the removed ones.\n" +
      "\t\tIf the datanode parameter is omitted, it will connect to the datanode running on the " +
      "\t\tlocalhost.";
    
    String removeNamespace = "-removeNamespace nameserviceId [datanodehost:port]: \tStops the\n" +
      "\t\tgiven namespace by being served by the given datanode. The namespace is given trough\n" +
      "\t\tnameserviceId argument. If no datanode is given, then the one running on the local\n" +
      "\t\tmachine will be used.";
    
    String refreshDatanodeDataDirs = "-refreshDatanodeDataDirs [confFilePath | --defaultPath] datanodehost:port:\t" +
      "Given\n\t\tdatanode refreshes the list of mnts available for the datanode.\n" +
      "\t\tIf the datanode parameter is omitted, it will connect to the datanode running on the " +
      "\t\tlocalhost. You can provide a config path or put the tag --defaultPath instead to use " +
      "\t\tthe datanode's default path.";

    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n" +
      "\t\tis specified.\n";

    if ("report".equals(cmd)) {
      System.out.println(report);
    } else if ("safemode".equals(cmd)) {
      System.out.println(safemode);
    } else if ("saveNamespace".equals(cmd)) {
      System.out.println(saveNamespace);
    } else if ("refreshNodes".equals(cmd)) {
      System.out.println(refreshNodes);
    } else if ("finalizeUpgrade".equals(cmd)) {
      System.out.println(finalizeUpgrade);
    } else if ("upgradeProgress".equals(cmd)) {
      System.out.println(upgradeProgress);
    } else if ("metasave".equals(cmd)) {
      System.out.println(metaSave);
    } else if ("blockReplication".equals(cmd)) {
      System.out.println(blockReplication);
    } else if (SetQuotaCommand.matches("-"+cmd)) {
      System.out.println(SetQuotaCommand.DESCRIPTION);
    } else if (ClearQuotaCommand.matches("-"+cmd)) {
      System.out.println(ClearQuotaCommand.DESCRIPTION);
    } else if (SetSpaceQuotaCommand.matches("-"+cmd)) {
      System.out.println(SetSpaceQuotaCommand.DESCRIPTION);
    } else if (ClearSpaceQuotaCommand.matches("-"+cmd)) {
      System.out.println(ClearSpaceQuotaCommand.DESCRIPTION);
    } else if ("refreshServiceAcl".equals(cmd)) {
      System.out.println(refreshServiceAcl);
    } else if ("refreshNamenodes".equals(cmd)) {
      System.out.println(refreshNamenodes);
    } else if ("removeNamespace".equals(cmd)) {
      System.out.println(removeNamespace);
    } else if ("refreshDatanodeDataDirs".equals(cmd)){
      System.out.println(refreshDatanodeDataDirs);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else if (RecountCommand.matches(cmd)){
      System.out.println(RecountCommand.DESCRIPTION);
    } else {
      System.out.println(summary);
      System.out.println(report);
      System.out.println(safemode);
      System.out.println(saveNamespace);
      System.out.println(refreshNodes);
      System.out.println(finalizeUpgrade);
      System.out.println(upgradeProgress);
      System.out.println(metaSave);
      System.out.println(SetQuotaCommand.DESCRIPTION);
      System.out.println(ClearQuotaCommand.DESCRIPTION);
      System.out.println(SetSpaceQuotaCommand.DESCRIPTION);
      System.out.println(ClearSpaceQuotaCommand.DESCRIPTION);
      System.out.println(refreshServiceAcl);
      System.out.println(refreshNamenodes);
      System.out.println(removeNamespace);
      System.out.println(refreshDatanodeDataDirs);
      System.out.println(RecountCommand.DESCRIPTION);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }

  }


  /**
   * Command to ask the namenode to finalize previously performed upgrade.
   * Usage: java DFSAdmin -finalizeUpgrade
   * @exception IOException 
   */
  public int finalizeUpgrade() throws IOException {
    int exitCode = -1;

    DistributedFileSystem dfs = getDFS();
    if (dfs == null) {
      System.out.println("FileSystem is " + getFS().getUri());
      return exitCode;
    }

    dfs.finalizeUpgrade();
    exitCode = 0;
   
    return exitCode;
  }

  /**
   * Command to request current distributed upgrade status, 
   * a detailed status, or to force the upgrade to proceed.
   * 
   * Usage: java DFSAdmin -upgradeProgress [status | details | force]
   * @exception IOException 
   */
  public int upgradeProgress(String[] argv, int idx) throws IOException {
    DistributedFileSystem dfs = getDFS();
    if (dfs == null) {
      System.out.println("FileSystem is " + getFS().getUri());
      return -1;
    }
    if (idx != argv.length - 1) {
      printUsage("-upgradeProgress");
      return -1;
    }

    UpgradeAction action;
    if ("status".equalsIgnoreCase(argv[idx])) {
      action = UpgradeAction.GET_STATUS;
    } else if ("details".equalsIgnoreCase(argv[idx])) {
      action = UpgradeAction.DETAILED_STATUS;
    } else if ("force".equalsIgnoreCase(argv[idx])) {
      action = UpgradeAction.FORCE_PROCEED;
    } else {
      printUsage("-upgradeProgress");
      return -1;
    }

    UpgradeStatusReport status = dfs.distributedUpgradeProgress(action);
    String statusText = (status == null ? 
        "There are no upgrades in progress." :
          status.getStatusText(action == UpgradeAction.DETAILED_STATUS));
    System.out.println(statusText);
    return 0;
  }

  /**
   * Enable/Disable Block Replication.
   * Usage: java DFSAdmin -blockReplication enable/disable
   */
  public int blockReplication(String[] argv, int idx) throws IOException {
    DistributedFileSystem dfs = getDFS();
    if (dfs == null) {
      System.out.println("FileSystem is " + getFS().getUri());
      return -1;
    }
    String option = argv[idx];
    boolean isEnable = true;
    if (option.equals("enable")) {
      isEnable = true;
    } else if (option.equals("disable")) {
      isEnable = false;
    } else {
      printUsage("-blockReplication");
      return -1;
    }
    dfs.blockReplication(isEnable);
    System.out.println("Block Replication is "  + (isEnable? "enabled" : "disabled")
        + " on server " + dfs.getUri());
    return 0;
  }

  /**
   * Dumps DFS data structures into specified file.
   * Usage: java DFSAdmin -metasave filename
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @exception IOException if an error accoured wile accessing
   *            the file or path.
   */
  public int metaSave(String[] argv, int idx) throws IOException {
    String pathname = argv[idx];
    DistributedFileSystem dfs = getDFS();
    if (dfs == null) {
      System.out.println("FileSystem is " + getFS().getUri());
      return -1;
    }
    dfs.metaSave(pathname);
    System.out.println("Created file " + pathname + " on server " +
                       dfs.getUri());
    return 0;
  }

  private static UnixUserGroupInformation getUGI(Configuration conf) 
  throws IOException {
    UnixUserGroupInformation ugi = null;
    try {
      ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException(
          "Failed to get the current user's information.").initCause(e));
    }
    return ugi;
  }

  /**
   * Refresh the authorization policy on the {@link NameNode}.
   * @return exitcode 0 on success, non-zero on failure
   * @throws IOException
   */
  public int refreshServiceAcl() throws IOException {
    // Get the current configuration
    Configuration conf = getConf();
    
    // Create the client
    RefreshAuthorizationPolicyProtocol refreshProtocol = 
      (RefreshAuthorizationPolicyProtocol) 
      RPC.getProxy(RefreshAuthorizationPolicyProtocol.class, 
                   RefreshAuthorizationPolicyProtocol.versionID, 
                   NameNode.getClientProtocolAddress(conf), getUGI(conf), conf,
                   NetUtils.getSocketFactory(conf, 
                                             RefreshAuthorizationPolicyProtocol.class));
    
    // Refresh the authorization policy in-effect
    refreshProtocol.refreshServiceAcl();
    
    return 0;
  }

   /**
    * Refresh the namenodes served by the {@link DataNode}.
    * Usage: java DFSAdmin -refreshNamenodes datanodehost:port
    * @param argv List of of command line parameters.
    * @param idx The index of the command that is being processed.
    * @exception IOException if an error accoured wile accessing
    *            the file or path.
    * @return exitcode 0 on success, non-zero on failure
    */
   public int refreshNamenodes(String[] argv, int i) throws IOException {
     ClientDatanodeProtocol datanode = null;
     String dnAddr = (argv.length == 2) ? argv[i] : null;
     try {
       datanode = getClientDatanodeProtocol(dnAddr);
       if (datanode != null) {
         datanode.refreshNamenodes();
         return 0;
       } else {
         return -1;
       }
      } finally {
       if (datanode != null && Proxy.isProxyClass(datanode.getClass())) {
         RPC.stopProxy(datanode);
       }
     }
   }
   
  private int refreshOfferService(String serviceName) throws IOException, InterruptedException {
    ClientDatanodeProtocol datanode = null;
    try {
      datanode = getClientDatanodeProtocol(null);
      if (datanode != null) {
        datanode.refreshOfferService(serviceName);
        return 0;
      } else {
        return -1;
      }
    } finally {
      if (datanode != null && Proxy.isProxyClass(datanode.getClass())) {
        RPC.stopProxy(datanode);
      }
    }
  }
   
   public int refreshDatanodeDataDirs(String [] argv, int i) throws IOException {
     ClientDatanodeProtocol datanode = null;
     String confFilePath = argv[i++];
     String dnAddr = (argv.length == 3) ? argv[i] : null;
     try {
       datanode = getClientDatanodeProtocol(dnAddr);
        if (datanode != null) {
          datanode.refreshDataDirs(confFilePath);
          return 0;
        } else {
          return -1;
        }
      } finally {
        if (datanode != null && Proxy.isProxyClass(datanode.getClass())) {
          RPC.stopProxy(datanode);
       }
     }
   }
   
   /**
    * Gets a new ClientDataNodeProtocol object.
    * @param dnAddr The address of the DataNode or null for the default value
    *        (connecting to localhost).
    * @return the initialized ClientDatanodeProtocol object or null on failure
    */
   private ClientDatanodeProtocol getClientDatanodeProtocol(String dnAddr)
       throws IOException {
     String hostname = null;
     int port;
     int index;
     Configuration conf = getConf();
     
     if (dnAddr == null) {
       // Defaulting the configured address for the port
       dnAddr = conf.get(FSConstants.DFS_DATANODE_IPC_ADDRESS_KEY);
       hostname = "localhost";
     }
     index = dnAddr.indexOf(':');
     if (index < 0) {
       return null;
     }

     port = Integer.parseInt(dnAddr.substring(index+1));
     if (hostname == null) {
       hostname = dnAddr.substring(0, index);
     }

     InetSocketAddress addr = NetUtils.createSocketAddr(hostname + ":" + port);
     if (ClientDatanodeProtocol.LOG.isDebugEnabled()) {
       ClientDatanodeProtocol.LOG.debug("ClientDatanodeProtocol addr=" + addr);
     }
     return (ClientDatanodeProtocol)RPC.getProxy(ClientDatanodeProtocol.class,
         ClientDatanodeProtocol.versionID, addr, conf);
   }
   
   
   /**
    * Removes a namespace from a given {@link DataNode}. It defaults to the
    * datanode on the local machine of no datanode is given.
    * 
    * Usage: java DFSAdmin -removeNamespace nameserviceId [datanodehost:datanodeport]
    * @param argv List of of command line parameters.
    * @param i The index of the command that is being processed.
    * @exception IOException if an error occurred while accessing
    *            the file or path.
    * @return exit code 0 on success, non-zero on failure
    */
   public int removeNamespace(String[] argv, int i) throws IOException {
     String nameserviceId = argv[i++];
     ClientDatanodeProtocol datanode = null;
     String dnAddr = (argv.length == 3) ? argv[i] : null;

     try {
       datanode = getClientDatanodeProtocol(dnAddr);
       if (datanode != null) {
         datanode.removeNamespace(nameserviceId);
         return 0;
       } else {
         return -1;
       }
     } finally {
       if (datanode != null && Proxy.isProxyClass(datanode.getClass())) {
         RPC.stopProxy(datanode);
       }
     }
   }

  /**
   * Print a list of file that have been open longer than N minutes
   * Usage: java DFSAdmin -getOpenFiles path minutes
   * @param argv List of of command line parameters.
   * @param idx The index of the command that is being processed.
   * @throws IOException if an error occured
   * @return exitcode 0 on success, non-zero on failure
   * @see FileSystem#iterativeGetOpenFiles
   */
  private static final int MILLIS_PER_MIN               = 60000;
  public int getOpenFiles(String[] argv, int i) throws IOException {
    String pathStr = argv[i++];
    String minsStr = argv[i++];

    Path f = new Path(pathStr);
    int mins = Integer.parseInt(minsStr);

    try {
      DistributedFileSystem srcFs = getDFS();

      String startAfter = "";
      OpenFileInfo infoList[] = srcFs.iterativeGetOpenFiles(
        f, mins * MILLIS_PER_MIN, startAfter);

      long timeNow = System.currentTimeMillis();

      // make multiple calls, if necessary
      while (infoList.length > 0) {

        for (OpenFileInfo info : infoList) {
          System.out.println(info.filePath + " " + (timeNow - info.millisOpen)
              / MILLIS_PER_MIN);
        }

        // get the next batch, if any
        startAfter = infoList[infoList.length-1].filePath;
        infoList = srcFs.iterativeGetOpenFiles(
          f, mins * MILLIS_PER_MIN, startAfter);
      }
      // success
      return 0;
    } catch (UnsupportedOperationException e) {
      System.err.println("ERROR: " + e.getMessage());
      return -1;
    } catch (IOException e) {
      System.err.println("ERROR: " + e.getMessage());
      return -1;
    }
  }
  
  /**
   * Display the filename the block belongs to and its locations.
   * 
   * @throws IOException
   */
  private int getBlockInfo(String[] argv, int i) throws IOException {
  	long blockId = Long.valueOf(argv[i++]);
  	LocatedBlockWithFileName locatedBlock = 
  			getDFS().getClient().getBlockInfo(blockId);
  
  	if (null == locatedBlock) {
  		System.err.println("Could not find the block with id : " + blockId);
  		return -1;
  	}
  	
  	StringBuilder sb = new StringBuilder();
  	sb.append("block: ")
  				.append(locatedBlock.getBlock()).append("\n")
  				.append("filename: ")
  				.append(locatedBlock.getFileName()).append("\n")
  				.append("locations: ");
  	
  	DatanodeInfo[] locs = locatedBlock.getLocations();
  	for (int k=0; k<locs.length; k++) {
  		if (k > 0) {
  			sb.append(" , ");
  		}
  		sb.append(locs[k].getHostName());
  	}
  				
  	System.out.println(sb.toString());  	
  	return 0;
  }
  
  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-report".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-report] [-service serviceName]");
    } else if ("-safemode".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-safemode enter | leave | get | wait | initqueues] [-service serviceName]");
    } else if ("-saveNamespace".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-saveNamespace [force] [uncompressed] [-service serviceName]");
    } else if ("-refreshNodes".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshNodes] [-service serviceName]");
    } else if ("-finalizeUpgrade".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-finalizeUpgrade] [-service serviceName]");
    } else if ("-upgradeProgress".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-upgradeProgress status | details | force] [-service serviceName]");
    } else if ("-metasave".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-metasave filename] [-service serviceName]");
    } else if ("-blockReplication".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-blockReplication enable | disable]");
    } else if (SetQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [" + SetQuotaCommand.USAGE+"]");
    } else if (ClearQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " ["+ClearQuotaCommand.USAGE+"]");
    } else if (SetSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [" + SetSpaceQuotaCommand.USAGE+"]");
    } else if (ClearSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " ["+ClearSpaceQuotaCommand.USAGE+"]");
    } else if ("-refreshServiceAcl".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshServiceAcl] [-service serviceName]");
    } else if ("-refreshNamenodes".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshNamenodes] [datanodehost:port]");
    } else if ("-removeNamespace".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " -removeNamespace nameserviceId [datanodehost:port]");
    } else if ("-refreshDatanodeDataDirs".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin [-refreshDatanodeDataDirs]"
                         + "[confFilePath] [datanodehost:port]");
    } else if ("-getOpenFiles".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-getOpenFiles] path minutes");
    } else if ("-getBlockInfo".equals(cmd)) {
    	System.err.println("Usage: java DFSAdmin"
          							 + " [-getBlockInfo] block-id");
    } else if (RecountCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin" + "[" +
          RecountCommand.USAGE + "]");
    } else if("-refreshOfferService".equals(cmd)){
      System.err.println("Usage: java DFSAdmin -refreshOfferService -service serviceName");
    } else {
      System.err.println("Usage: java DFSAdmin");
      System.err.println("           [-report] [-service serviceName]");
      System.err.println("           [-safemode enter | leave | get | wait | initqueues] [-service serviceName]");
      System.err.println("           [-saveNamespace [force] [uncompressed] [-service serviceName]");
      System.err.println("           [-rollEditLog [-service serviceName]");
      System.err.println("           [-refreshNodes] [-service serviceName]");
      System.err.println("           [-finalizeUpgrade] [-service serviceName]");
      System.err.println("           [-upgradeProgress status | details | force] [-service serviceName]");
      System.err.println("           [-metasave filename] [-service serviceName]");
      System.err.println("           [-blockReplication enable | disable]");
      System.err.println("           [-refreshServiceAcl] [-service serviceName]");
      System.err.println("           ["+SetQuotaCommand.USAGE+"]");
      System.err.println("           ["+ClearQuotaCommand.USAGE+"]");
      System.err.println("           [-refreshNamenodes] [datanodehost:port]");
      System.err.println("           [removeNamespace nameserviceId [datanodehost:port]]");
      System.err.println("           [-refreshDatanodeDataDirs] [confFilePath] " 
                                           + "[datanodehost:port]");
      System.err.println("           [-getOpenFiles] path minutes");
      System.err.println("           [-getBlockInfo] block-id");
      System.err.println("           ["+SetSpaceQuotaCommand.USAGE+"]");
      System.err.println("           ["+ClearSpaceQuotaCommand.USAGE+"]");
      System.err.println("           ["+RecountCommand.USAGE+"]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * @param argv The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  @Override
  public int run(String[] argv) throws Exception {
    String serviceName = null;
    try {
      String[] tempArrayForServiceName = new String[] {""};
      DFSUtil.getServiceName(argv, tempArrayForServiceName);
      serviceName = tempArrayForServiceName[0];
      argv = DFSUtil.setGenericConf(argv, getConf());
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      printUsage("");
      return -1;
    }
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
    if ("-safemode".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-report".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-saveNamespace".equals(cmd)) {
      if (argv.length < 1 || argv.length > 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshNodes".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-finalizeUpgrade".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-upgradeProgress".equals(cmd)) {
        if (argv.length != 2) {
          printUsage(cmd);
          return exitCode;
        }
    } else if ("-metasave".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshServiceAcl".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshNamenodes".equals(cmd)) {
      if (argv.length < 1 || argv.length > 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-removeNamespace".equals(cmd)) {
      if (argv.length < 2 || argv.length > 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshDatanodeDataDirs".equals(cmd)) {
      if (argv.length < 2 || argv.length > 3 ) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-getOpenFiles".equals(cmd)) {
      if (argv.length != 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-getBlockInfo".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-blockReplication".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if (RecountCommand.matches(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshOfferService".equals(cmd)) {
      if(argv.length != 1) {
        printUsage(cmd);
        return -1;
      }
    }

    // initialize DFSAdmin
    try {
      // Substitute client address with dnprotocol address if it is configured
      InetSocketAddress address = NameNode.getDNProtocolAddress(getConf());
      if (address != null) {
        int dnPort = address.getPort();
        URI fileSystem = FileSystem.getDefaultUri(getConf());
        // Completely rebuilding filesystem URI with a new port
        URI dnProtocolURI = new URI(fileSystem.getScheme(), fileSystem
            .getUserInfo(), fileSystem.getHost(), dnPort, fileSystem.getPath(),
            fileSystem.getQuery(), fileSystem.getFragment());
        FileSystem.setDefaultUri(getConf(), dnProtocolURI);
      }
      init();
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server"
                         + "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      System.err.println(e.getMessage());
      System.err.println("Bad connection to DFS... command aborted.");
      return exitCode;
    }

    exitCode = 0;
    try {
      if ("-report".equals(cmd)) {
        report();
      } else if ("-safemode".equals(cmd)) {
        setSafeMode(argv, i);
      } else if ("-saveNamespace".equals(cmd)) {
        exitCode = saveNamespace(argv, i);
      } else if ("-rollEditLog".equals(cmd)) {
        exitCode = rollEditLog();
      } else if ("-refreshNodes".equals(cmd)) {
        exitCode = refreshNodes();
      } else if ("-finalizeUpgrade".equals(cmd)) {
        exitCode = finalizeUpgrade();
      } else if ("-upgradeProgress".equals(cmd)) {
        exitCode = upgradeProgress(argv, i);
      } else if ("-metasave".equals(cmd)) {
        exitCode = metaSave(argv, i);
      } else if ("-blockReplication".equals(cmd)) {
        exitCode = blockReplication(argv, i);
      } else if (ClearQuotaCommand.matches(cmd)) {
        exitCode = new ClearQuotaCommand(argv, i, getFS()).runAll();
      } else if (SetQuotaCommand.matches(cmd)) {
        exitCode = new SetQuotaCommand(argv, i, getFS()).runAll();
      } else if (ClearSpaceQuotaCommand.matches(cmd)) {
        exitCode = new ClearSpaceQuotaCommand(argv, i, getFS()).runAll();
      } else if (SetSpaceQuotaCommand.matches(cmd)) {
        exitCode = new SetSpaceQuotaCommand(argv, i, getFS()).runAll();
      } else if ("-refreshServiceAcl".equals(cmd)) {
        exitCode = refreshServiceAcl();
      } else if ("-refreshNamenodes".equals(cmd)) {
        exitCode = refreshNamenodes(argv, i);
      } else if ("-refreshOfferService".equals(cmd)) {
        exitCode = refreshOfferService(serviceName);
      } else if ("-removeNamespace".equals(cmd)) {
        exitCode = removeNamespace(argv, i);
      } else if ("-refreshDatanodeDataDirs".equals(cmd)) {
        exitCode = refreshDatanodeDataDirs(argv, i );
      } else if ("-getOpenFiles".equals(cmd)) {
        exitCode = getOpenFiles(argv, i);
      } else if ("-getBlockInfo".equals(cmd)) {
      	exitCode = getBlockInfo(argv, i);
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i]);
        } else {
          printHelp("");
        }
      } else if (RecountCommand.matches(cmd)) {
        exitCode = new RecountCommand(getFS()).runAll();
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": "
                           + content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": "
                           + ex.getLocalizedMessage());
      }
    } catch (Exception e) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": "
                         + e.getLocalizedMessage());
    } 
    return exitCode;
  }

  /**
   * main() has some simple utility methods.
   * @param argv Command line parameters.
   * @exception Exception if the filesystem does not exist.
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new DFSAdmin(), argv);
    System.exit(res);
  }
}
