package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.hdfs.CachingAvatarZooKeeperClient;
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

  public static void main(String[] args) throws Exception {
    // -files option is also used by GenericOptionsParser
    // Make sure that is not the first argument for fsck
    int res = -1;
    if ((args.length == 0) || ("-files".equals(args[0])))
      printUsage();
    else
      res = ToolRunner.run(new AvatarDFSck(new Configuration()), args);
    System.exit(res);
  }
}
