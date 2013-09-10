package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.hdfs.AvatarShell;
import org.apache.hadoop.hdfs.CachingAvatarZooKeeperClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

public class AvatarDFSck extends DFSck {

  static {
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }

  private final CachingAvatarZooKeeperClient zk;

  public AvatarDFSck(Configuration conf) throws Exception {
    this(conf, System.out);
  }

  public AvatarDFSck(Configuration conf, PrintStream out) throws IOException {
    super(conf, out);
    zk = new CachingAvatarZooKeeperClient(conf, null);
  }

  @Override
  protected String getInfoServer() throws Exception {
    Configuration conf = getConf();
    Stat stat = new Stat();
    String primaryAddr = zk.getPrimaryAvatarAddress(
        new URI(conf.get("fs.default.name")), stat, true, false);
    String uri0 = new URI(conf.get("fs.default.name0", "")).getAuthority();
    if (uri0.equals(primaryAddr)) {
      return conf.get("dfs.http.address0", "");
    } else {
      return conf.get("dfs.http.address1", "");
    }
  }
  
  /**
   * Adjust configuration for nameservice keys.
   */
  public static String[] adjustConf(String[] argv, Configuration conf) {
    String[] serviceId = new String[] { "" };
    String[] filteredArgv = DFSUtil.getServiceName(argv, serviceId);
    if (!serviceId[0].equals("")) {
      NameNode.checkServiceName(conf, serviceId[0]);
      DFSUtil.setGenericConf(conf, serviceId[0],
          AvatarNode.AVATARSERVICE_SPECIFIC_KEYS);
      NameNode.setupDefaultURI(conf);
    }
    return filteredArgv;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    // enforce to specify service if configured
    if (!AvatarShell.isServiceSpecified("AvatarFsck", conf, args)) {
      System.exit(-1);
    }
    
    // make it service aware
    try {
      args = adjustConf(args, conf);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      printUsage();
      System.exit(-1);
    }
    
    // -files option is also used by GenericOptionsParser
    // Make sure that is not the first argument for fsck
    int res = -1;
    if ((args.length == 0) || ("-files".equals(args[0])))
      printUsage();
    else
      res = ToolRunner.run(new AvatarDFSck(conf), args);
    System.exit(res);
  }
}
