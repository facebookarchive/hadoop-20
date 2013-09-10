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
package org.apache.hadoop.hdfs;

import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Random;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.ServerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster.ShutdownInterface;
import org.apache.hadoop.hdfs.MiniDFSCluster.ShutDownUtil;
import org.apache.hadoop.hdfs.protocol.AvatarConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Standby;
import org.apache.hadoop.hdfs.server.namenode.NNStorageDirectoryRetentionManager;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.DNSToSwitchMapping;

/**
 * This class manages a Avatar/HDFS cluster with all nodes running
 * locally.
 * To synchronize the AvatarNodes, it uses a local ZooKeeper
 * server.
 */
public class MiniAvatarCluster {

  public static final String NAMESERVICE_ID_PREFIX = "nameserviceId";
  public static int currNSId = 0;
  public static int instantiationRetries = 15;
  public static final String JID = "test-journal";
  
  public static class DataNodeProperties implements ShutdownInterface {
    public AvatarDataNode datanode;
    public Configuration conf;
    public String[] dnArgs;

    DataNodeProperties(AvatarDataNode node, Configuration conf, String[] args) {
      this.datanode = node;
      this.conf = conf;
      this.dnArgs = args;
    }

    @Override
    public void shutdown() throws IOException {
      if (this.datanode != null)
        this.datanode.shutdown();
    }
  }

  public static enum AvatarState {
    ACTIVE,
    STANDBY,
    DEAD
  }
  
  public static class AvatarInfo implements ShutdownInterface {
    public AvatarNode avatar;
    AvatarState state;
    int nnPort;
    int nnDnPort;
    int httpPort;
    int rpcPort;
    String startupOption;
    
    AvatarInfo(AvatarNode avatar, AvatarState state,
               int nnPort, int nnDnPort, int httpPort,
               int rpcPort, String startupOption) {
      this.avatar = avatar;
      this.state = state;
      this.nnPort = nnPort;
      this.nnDnPort = nnDnPort;
      this.httpPort = httpPort;
      this.rpcPort = rpcPort;
      this.startupOption = startupOption;
    }

    @Override
    public void shutdown() throws IOException {
      if (this.avatar != null)
        this.avatar.shutdown(true);
    }
  }
  
  private static final Log LOG = LogFactory.getLog(MiniAvatarCluster.class);

  private static final String DEFAULT_TEST_DIR = 
    "build/contrib/highavailability/test/data";
  public static final String TEST_DIR =
    new File(System.getProperty("test.build.data", DEFAULT_TEST_DIR)).
    getAbsolutePath();

  private static final AtomicInteger ClusterId = new AtomicInteger(1);

  private static final String ZK_DATA_DIR = TEST_DIR + "/zk.data";
  private static final String ZK_CONF_FILE = TEST_DIR + "/zk.conf";

  public static final int zkClientPort = MiniDFSCluster.getFreePort();

  private static String baseAvatarDir;
  private static String dataDir;
  private int numDataNodes;
  private boolean format;
  private String[] racks;
  private String[] hosts;
  private boolean federation;
  private NameNodeInfo[] nameNodes;
  private final boolean enableQJM;
  private StartupOption startOpt;
  private final int numJournalNodes;
  private MiniJournalCluster journalCluster = null; 
  private Configuration conf;
  
  /**
   * Some test cases only work with FileJournalManager, need a way to tell
   * if QJM is enabled here.
   */
  public boolean isUsingJournalCluster() {
  	return journalCluster != null;
  }
  
  public MiniJournalCluster getJournalCluster() {
    if (journalCluster == null) {
      throw new IllegalArgumentException(
          "MiniAvatarCluster not configured to use journal cluster");
    }
    return journalCluster;
  }

  public class NameNodeInfo {
    Configuration conf;
    public ArrayList<AvatarInfo> avatars = null;
    private final String fsimage0Dir;
    private final String fsimage1Dir;
    private final String fsedits0Dir;
    private final String fsedits1Dir;

    private final String fsimagelocalDir;
    private final String fseditslocalDir;

    private final int nnPort;
    private final int nn0Port;
    private final int nn1Port;
    private final int nnDnPort;
    private final int nnDn0Port;
    private final int nnDn1Port;
    private final int httpPort;
    private final int http0Port;
    private final int http1Port;
    private final int rpcPort;
    private final int rpc0Port;
    private final int rpc1Port;

    private Configuration clientConf;
    private Configuration a0Conf;
    private Configuration a1Conf;
    private final String avatarDir;
    String nameserviceId;
    
    NameNodeInfo(int nnIndex) {
      avatarDir = baseAvatarDir;

      fsimagelocalDir = avatarDir + "/fsimagelocal-"
          + FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD;
      fseditslocalDir = avatarDir + "/fseditslocal-"
          + FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD;

      fsimage0Dir = avatarDir + "/fsimage0";
      fsimage1Dir = avatarDir + "/fsimage1";
      fsedits0Dir = avatarDir + "/fsedits0";
      fsedits1Dir = avatarDir + "/fsedits1";

      rpcPort = nnPort = MiniDFSCluster.getFreePort();
      nnDnPort = MiniDFSCluster.getFreePort();
      httpPort = MiniDFSCluster.getFreePort();
      rpc0Port = nn0Port = MiniDFSCluster.getFreePorts(2);
      nnDn0Port = MiniDFSCluster.getFreePort();
      http0Port = MiniDFSCluster.getFreePort();
      rpc1Port = nn1Port = MiniDFSCluster.getFreePorts(2);
      nnDn1Port = MiniDFSCluster.getFreePort();
      http1Port = MiniDFSCluster.getFreePort();
    }
    
    public void setAvatarNodes(ArrayList<AvatarInfo> avatars) {
      this.avatars = avatars;
    }
    
    void unlockStorageDirectory(String instance) {
      if (!instance.equals("zero") && !instance.equals("one")) {
        throw new IllegalArgumentException(
            "Specify one or zero, invalid argument : " + instance);
      }
      new File(fsimagelocalDir.replaceAll(
          FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, instance), "in_use.lock")
          .delete();
      new File(fseditslocalDir.replaceAll(
          FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, instance), "in_use.lock")
          .delete();
    }

    public void initClientConf(Configuration conf) {
      clientConf = new Configuration(conf);
      clientConf.set("fs.default.name", "hdfs://localhost:" + nnPort);
      clientConf.set("fs.default.name0", "hdfs://127.0.0.1:" + nn0Port);
      clientConf.set("fs.default.name1", "hdfs://127.0.0.1:" + nn1Port);
      clientConf.set(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:"
          + nnPort);
      clientConf.set(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY + "0", "127.0.0.1:"
          + nn0Port);
      clientConf.set(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY + "1", "127.0.0.1:"
          + nn1Port);
      clientConf.set("dfs.namenode.dn-address", "localhost:" + nnDnPort);
      clientConf.set("dfs.namenode.dn-address0", "127.0.0.1:" + nnDn0Port);
      clientConf.set("dfs.namenode.dn-address1", "127.0.0.1:" + nnDn1Port);
      clientConf.set("fs.hdfs.impl",
          "org.apache.hadoop.hdfs.DistributedAvatarFileSystem");
      clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
      // Lower the number of retries to close connections quickly.
      clientConf.setInt("ipc.client.connect.max.retries", 3);
    }
    
    public void initGeneralConf(Configuration conf, String nameserviceId) {
      // overwrite relevant settings
      initClientConf(conf);
      this.nameserviceId = nameserviceId;
      // avatar nodes
      if (federation) {
        conf.set("dfs.namenode.rpc-address0", "127.0.0.1:" + rpc0Port);
        conf.set("dfs.namenode.rpc-address1", "127.0.0.1:" + rpc1Port);
      } else {
        conf.set("fs.default.name", "hdfs://localhost:" + nnPort);
        conf.set("fs.default.name0", "hdfs://localhost:" + nn0Port);
        conf.set("fs.default.name1", "hdfs://localhost:" + nn1Port);
        conf.set("dfs.namenode.dn-address", "localhost:" + nnDnPort);
        conf.set(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:" + nnPort);
        conf.set("dfs.http.address", "127.0.0.1:" + httpPort);
      }
      // Enable avatar testing framework for unit tests.
      conf.setFloat("dfs.avatarnode.failover.sample.percent", 1.0f);
      conf.set("dfs.avatarnode.failover.test.data.dir", avatarDir);

      conf.set("dfs.namenode.dn-address0", "127.0.0.1:" + nnDn0Port);
      conf.set("dfs.namenode.dn-address1", "127.0.0.1:" + nnDn1Port);
      conf.set("dfs.http.address0", "127.0.0.1:" + http0Port);
      conf.set("dfs.http.address1", "127.0.0.1:" + http1Port);
      conf.set(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY + "0", "127.0.0.1:"
          + nn0Port);
      conf.set(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY + "1", "127.0.0.1:"
          + nn1Port);

      // set the shared edits and image dirs.
      if (enableQJM) {
        String journalURI = journalCluster.getQuorumJournalURI(JID).toString();
        
        // set the edits dir
        conf.set("dfs.name.edits.dir.shared0", journalURI + "/zero");
        conf.set("dfs.name.edits.dir.shared1", journalURI + "/one");
        
        // set the image dir
        conf.set("dfs.name.dir.shared0", journalURI + "/zero");
        conf.set("dfs.name.dir.shared1", journalURI + "/one");
        
        conf.setBoolean("dfs.force.remote.image", true);
      } else {
        conf.set("dfs.name.edits.dir.shared0", fsedits0Dir);
        conf.set("dfs.name.edits.dir.shared1", fsedits1Dir);
        
        conf.set("dfs.name.dir.shared0", fsimage0Dir);
        conf.set("dfs.name.dir.shared1", fsimage1Dir);
      }
      
      conf.setInt("dfs.safemode.extension", 1000);
      // These two ipc parameters help RPC connections to shut down quickly in
      // unit tests.
      conf.setInt("ipc.client.connect.max.retries", 3);
      conf.setInt("ipc.client.connect.timeout", 2000);
      // We need to disable the filesystem cache so that unit tests and
      // MiniAvatarCluster don't end up sharing FileSystem objects.
      if (federation) {
        for (String key: AvatarNode.AVATARSERVICE_SPECIFIC_KEYS) {
          String value = conf.get(key);
          if (value != null) {
            String newKey = DFSUtil.getNameServiceIdKey(key, nameserviceId);
            conf.set(newKey, value);
            conf.set(key, "");
          }
        }
        String rpcKey = DFSUtil.getNameServiceIdKey(
            AvatarNode.DFS_NAMENODE_RPC_ADDRESS_KEY, nameserviceId);
        conf.set(rpcKey, "localhost:" + rpcPort);
        String dnKey = DFSUtil.getNameServiceIdKey(
            NameNode.DATANODE_PROTOCOL_ADDRESS, nameserviceId);
        conf.set(dnKey, "localhost:" + nnDnPort);
        String httpKey = DFSUtil.getNameServiceIdKey(
            NameNode.DFS_NAMENODE_HTTP_ADDRESS_KEY, nameserviceId);
        conf.set(httpKey, "localhost:" + httpPort);
      }
    }
    
    public void updateAvatarConf(Configuration newConf) {
      conf = new Configuration(newConf);
      if (federation) {
        conf.set(FSConstants.DFS_FEDERATION_NAMESERVICE_ID, nameserviceId);
      }
      
      // server config for avatar nodes
      a0Conf = new Configuration(conf);
      a1Conf = new Configuration(conf);

      a0Conf.set("dfs.name.dir", fsimagelocalDir);
      a0Conf.set("dfs.name.edits.dir", fseditslocalDir);
      a0Conf.set("fs.checkpoint.dir", avatarDir + "/checkpoint0");

      a1Conf.set("dfs.name.dir", fsimagelocalDir);
      a1Conf.set("dfs.name.edits.dir", fseditslocalDir);
      a1Conf.set("fs.checkpoint.dir", avatarDir + "/checkpoint1");
    }
    
    public void createAvatarDirs() {
      new File(fsimagelocalDir.replaceAll(
          FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, "zero")).mkdirs();
      new File(fsimagelocalDir.replaceAll(
          FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, "one")).mkdirs();
      new File(fsimage0Dir).mkdirs();
      new File(fsimage1Dir).mkdirs();
      new File(fseditslocalDir.replaceAll(
          FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, "zero")).mkdirs();
      new File(fseditslocalDir.replaceAll(
          FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, "one")).mkdirs();
      new File(fsedits0Dir).mkdirs();
      new File(fsedits1Dir).mkdirs();
    }
    
    public void cleanupAvatarDirs() throws IOException {
      String[] files = new String[] {
          fsimagelocalDir.replaceAll(
              FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, "zero"),
          fsimagelocalDir.replaceAll(
              FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, "one"),
          fsimage0Dir,
          fsimage1Dir,
          fseditslocalDir.replaceAll(
              FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, "zero"),
          fseditslocalDir.replaceAll(
              FSConstants.DFS_NAMENODE_NAME_DIR_WILDCARD, "one"),
          fsedits0Dir, fsedits1Dir
      };
      for (String filename : files) {
        FileUtil.fullyDelete(new File(filename));
      }
    }

    public String getNameserviceId() {
      return nameserviceId;
    }
  }

  private static ZooKeeperServer zooKeeper;
  private static NIOServerCnxnFactory cnxnFactory;

  private ArrayList<DataNodeProperties> dataNodes = 
    new ArrayList<DataNodeProperties>();
  
  static {
    DataNode.setSecureRandom(new Random());
  }
  
  public static class Builder {
  	private Configuration conf;
  	private int numDataNodes = 1;
  	private boolean format = true;
  	private String[] racks = null;
  	private String[] hosts = null;
  	private int numNameNodes = 1;
  	private boolean federation = false;
  	private long[] simulatedCapacities = null;
  	private int numJournalNodes = 3;
  	private boolean enableQJM = true;
    private MiniJournalCluster journalCluster = null;
    private StartupOption startOpt = null;
    private int instantiationRetries = 15;
  	
  	public Builder(Configuration conf) {
  		this.conf = conf;
  	}
  	
    public Builder startOpt(StartupOption startOpt) {
      this.startOpt = startOpt;
      return this;
    }

    public Builder instantionRetries(int instantionRetries) {
      this.instantiationRetries = instantionRetries;
      return this;
    }

  	public Builder numDataNodes(int numDataNodes) {
  	  this.numDataNodes = numDataNodes;
  	  return this;
  	}
  	
  	public Builder format(boolean format) {
  	  this.format = format;
  	  return this;
  	}
  	
  	public Builder racks(String[] racks) {
  	  this.racks = racks;
  	  return this;
  	}
  	
  	public Builder hosts(String[] hosts) {
  	  this.hosts = hosts;
  	  return this;
  	}
  	
  	public Builder numNameNodes(int numNameNodes) {
  		this.numNameNodes = numNameNodes;
  		return this;
  	}
  	
  	public Builder federation(boolean federation) {
  		this.federation = federation;
  		return this;
  	}
  	
  	public Builder simulatedCapacities(long[] simulatedCapacities) {
  		this.simulatedCapacities = simulatedCapacities;
  		return this;
  	}
  	
  	public Builder numJournalNodes(int numJournalNodes) {
  		this.numJournalNodes = numJournalNodes;
  		return this;
  	}
  	
  	public Builder enableQJM(boolean enableQJM) {
  		this.enableQJM = enableQJM;
  		return this;
  	}
  	
    public Builder setJournalCluster(MiniJournalCluster journalCluster) {
      this.journalCluster = journalCluster;
      this.enableQJM = true;
		return this;
	}

  	public MiniAvatarCluster build() 
  			throws IOException, ConfigException, InterruptedException {
  		return new MiniAvatarCluster(this);
  	}
  }

  public MiniAvatarCluster(Configuration conf,
                           int numDataNodes,
                           boolean format,
                           String[] racks,
                           String[] hosts) 
    throws IOException, ConfigException, InterruptedException {
  	this(new Builder(conf).numDataNodes(numDataNodes).format(format)
  	    .racks(racks).hosts(hosts));
  }
  
  public MiniAvatarCluster(Configuration conf,
                           int numDataNodes,
                           boolean format,
                           String[] racks,
                           String[] hosts,
                           int numNameNodes,
                           boolean federation)
    throws IOException, ConfigException, InterruptedException {
  	this(new Builder(conf).numDataNodes(numDataNodes).format(format)
            .racks(racks)
            .hosts(hosts)
  					.numNameNodes(numNameNodes)
  					.federation(federation));
  }
  
  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostname of each DataNode
   * @param numNameNodes Number of NameNodes to start; 
   * @param federation if true, we start it with federation configure;
   */
  public MiniAvatarCluster(Configuration conf,
                           int numDataNodes,
                           boolean format,
                           String[] racks,
                           String[] hosts,
                           int numNameNodes,
                           boolean federation,
                           long[] simulatedCapacities)
    throws IOException, ConfigException, InterruptedException {
  	this(new Builder(conf).numDataNodes(numDataNodes).format(format)
        .racks(racks)
        .hosts(hosts)
        .numNameNodes(numNameNodes)
        .federation(federation)
        .simulatedCapacities(simulatedCapacities));
  }
  
  public MiniAvatarCluster(Builder b) 
  		throws IOException, ConfigException, InterruptedException {
  	
    Standby.CHECKPOINT_SLEEP_BEFORE_RETRY = 100;
    this.conf = b.conf;
    
    final String testDir = TEST_DIR + "/" + conf.get(MiniDFSCluster.DFS_CLUSTER_ID, "");
    baseAvatarDir = testDir + "/avatar";
    dataDir = testDir + "/data";
    
    this.instantiationRetries = b.instantiationRetries;
    this.numDataNodes = b.numDataNodes;
    this.format = b.format;
    this.racks = b.racks;
    this.hosts = b.hosts;
    this.numJournalNodes = b.numJournalNodes;
    this.enableQJM = b.enableQJM;
    this.startOpt = b.startOpt;
    this.journalCluster = b.journalCluster;

    int clusterId = ClusterId.getAndIncrement();
    conf.setInt(FSConstants.DFS_CLUSTER_ID, clusterId);
    conf.set(FSConstants.DFS_CLUSTER_NAME, "MiniAvatarCluster-" + clusterId);

    conf.setInt("dfs.secondary.info.port", 0);
    conf.set("fs.ha.zookeeper.prefix", "/hdfs");
    conf.set("fs.ha.zookeeper.quorum", "localhost:" + zkClientPort);
    conf.setInt("fs.ha.zookeeper.connect.timeout", 30000);
    conf.setInt("fs.ha.zookeeper.timeout", 30000);
    
    // datanodes
    
    conf.setInt("dfs.datanode.fullblockreport.delay", 1000);
    conf.setInt("dfs.datanode.blockreceived.retry.internval", 1000);
    
    conf.set(FSConstants.DFS_DATANODE_ADDRESS_KEY, "localhost:0");
    conf.set("dfs.datanode.http.address", "localhost:0");
    conf.set("dfs.datanode.ipc.address", "localhost:0");

    String loopBack = getLoopBackInterface();
    LOG.info("LoopBack interface is : " + loopBack);
    conf.set(FSConstants.DFS_DATANODE_DNS_INTERFACE, loopBack);
    conf.set(FSConstants.DFS_NAMENODE_DNS_INTERFACE, loopBack);

    // other settings
    conf.setBoolean("dfs.permissions", false);
    conf.setBoolean("dfs.persist.blocks", true);
    conf.set("fs.hdfs.impl",
             "org.apache.hadoop.hdfs.DistributedAvatarFileSystem");
    conf.setLong("dfs.blockreport.initialDelay", 0);
    conf.setClass("topology.node.switch.mapping.impl", 
                  StaticMapping.class, DNSToSwitchMapping.class);

    if (conf.get("dfs.ingest.retries") == null) {
      conf.setInt("dfs.ingest.retries", 2);
    }
    conf.setLong("rpc.polling.interval", 10);
    conf.setLong("lease.check.interval", 10);
    
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    
    // enable checkpoint by default
    if(conf.get("fs.checkpoint.enabled") == null) {
      conf.setBoolean("fs.checkpoint.enabled", true);
    }
    
    //http image download timeout - 5s
    if(conf.get("dfs.image.transfer.timeout") == null) {
      conf.setInt("dfs.image.transfer.timeout", 5 * 1000);
    }
    
    // make the standby actions (e.g., checkpoint trigger) quicker
    conf.setInt("hdfs.avatarnode.sleep", 1000);
    
    // disable standby backup limits
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_DAYS_TOKEEP, 0);
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_COPIES_TOKEEP, 0);

    // start the JournalCluster.
    if (this.enableQJM) {
    	startJournalCluster();
    }
    
    this.federation = b.federation;
    Collection<String> nameserviceIds = DFSUtil.getNameServiceIds(conf);
    if(nameserviceIds.size() > 1)  
      this.federation = true;
    if (!federation && b.numNameNodes != 1) {
      throw new IOException("Only 1 namenode is allowed in non-federation cluster.");
    }
    nameNodes = new NameNodeInfo[b.numNameNodes];
    for (int nnIndex = 0; nnIndex < b.numNameNodes; nnIndex++) {
      nameNodes[nnIndex] = new NameNodeInfo(nnIndex);
      if (format)
        nameNodes[nnIndex].cleanupAvatarDirs();
      nameNodes[nnIndex].createAvatarDirs();
    }
    if (!federation) {
      nameNodes[0].initGeneralConf(conf, null);
    } else {
      if (nameserviceIds.isEmpty()) {
        for (int i = 0; i < nameNodes.length; i++) {
          nameserviceIds.add(NAMESERVICE_ID_PREFIX + getNSId());
        }
      }
      initFederationConf(conf, nameserviceIds);
    }
    
    if (this.format) {
      File data_dir = new File(dataDir);
      if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
        throw new IOException("Cannot remove data directory: " + data_dir);
      }
    }
  
    // Need to start datanodes before avatarnodes, since the primary starts up
    // in safemode and when the standby starts up, it waits for the primary to
    // exit safemode. So if we start avatarnodes first with non-empty FSImage
    // and FSEdits, the primary avatar would wait for datanode block reports and
    // the standby would wait for the primary to exit safemode and since we
    // wouldn't return from the standby initialization we would never start the
    // datanodes and hence we enter a deadlock.
    registerZooKeeperNodes();
    startDataNodes(b.simulatedCapacities);
    startAvatarNodes();
    waitAvatarNodesActive();

    waitDataNodesActive();

    waitExitSafeMode();
    waitForTheFirstCheckpoint();
  }
  
  /**
   * Retrieves the name of the loopback interface in a platform independent way.
   */
  private static String getLoopBackInterface() throws IOException {
    String loopBack = "lo";
    Enumeration<NetworkInterface> ifaces = NetworkInterface
        .getNetworkInterfaces();
    while (ifaces.hasMoreElements()) {
      NetworkInterface iface = ifaces.nextElement();
      if (iface.isLoopback()) {
        loopBack = iface.getName();
        break;
      }
    }
    return loopBack;
  }

  private void startJournalCluster() throws IOException {
    if (journalCluster == null) {
      this.journalCluster = new MiniJournalCluster.Builder(conf)
          .numJournalNodes(numJournalNodes).build();
    }
  }
  
  private void initFederationConf(Configuration conf,
      Collection<String> nameserviceIds) {
    String nameserviceIdList = "";
    int nnIndex = 0;
    for (String nameserviceId : nameserviceIds) {
      // Create comma separated list of nameserviceIds
      if (nameserviceIdList.length() > 0) {
        nameserviceIdList += ",";
      }
      nameserviceIdList += nameserviceId;
      nameNodes[nnIndex].initGeneralConf(conf, nameserviceId);
      nnIndex++;
    }
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIdList);
  }

  private static ServerConfig createZooKeeperConf() 
    throws IOException, ConfigException {
    
    // create conf file
    File zkConfDir = new File(TEST_DIR);
    zkConfDir.mkdirs();
    File zkConfFile = new File(ZK_CONF_FILE);
    zkConfFile.delete();
    zkConfFile.createNewFile();

    Properties zkConfProps = new Properties();
    zkConfProps.setProperty("tickTime", "2000");
    zkConfProps.setProperty("dataDir", ZK_DATA_DIR);
    zkConfProps.setProperty("clientPort", new Integer(zkClientPort).toString());
    zkConfProps.setProperty("maxClientCnxns", "500");
    zkConfProps.store(new FileOutputStream(zkConfFile), "");

    // create config object
    ServerConfig zkConf = new ServerConfig();
    zkConf.parse(ZK_CONF_FILE);

    return zkConf;
  }

  private static ServerConfig getZooKeeperConf() throws Exception {
    if (new File(ZK_CONF_FILE).exists()) {
      ServerConfig zkConf = new ServerConfig();
      zkConf.parse(ZK_CONF_FILE);

      return zkConf;
    } else {
      return createZooKeeperConf();
    }
  }

  public static boolean clearZooKeeperData() throws Exception {
    ServerConfig zkConf = getZooKeeperConf();
    File dataLogDir = new File(zkConf.getDataLogDir());
    File dataDir = new File(zkConf.getDataDir());
    return (FileUtil.fullyDelete(dataLogDir) && FileUtil.fullyDelete(dataDir));
  }
    
  public static void createAndStartZooKeeper() 
    throws IOException, ConfigException, InterruptedException {
    logStateChange("Creating zookeeper server");
    AvatarShell.retrySleep = 1000;
    ServerConfig zkConf = createZooKeeperConf();

    zooKeeper = new ZooKeeperServer();
    FileTxnSnapLog ftxn = new 
      FileTxnSnapLog(new File(zkConf.getDataLogDir()),
                     new File(zkConf.getDataDir()));
    zooKeeper.setTxnLogFactory(ftxn);
    zooKeeper.setTickTime(zkConf.getTickTime());
    zooKeeper.setMinSessionTimeout(zkConf.getMinSessionTimeout());
    zooKeeper.setMaxSessionTimeout(zkConf.getMaxSessionTimeout());

    cnxnFactory = new NIOServerCnxnFactory();
    cnxnFactory.configure(zkConf.getClientPortAddress(),
        zkConf.getMaxClientCnxns());
    cnxnFactory.startup(zooKeeper);
    logStateChange("Creating zookeeper server - completed");
  }

  private void registerZooKeeperNode(int nnPrimaryPort, int nnDnPrimaryPort,
      int httpPrimaryPort, int rpcPrimaryPort, NameNodeInfo nni) throws IOException {
    int retries = 5;    
    for(int i =0; i<retries; i++) {
      try {
        AvatarZooKeeperClient zkClient =
          new AvatarZooKeeperClient(nni.conf, null, false);
        zkClient.registerPrimary("localhost:" + nni.nnPort, 
        "127.0.0.1:" + nnPrimaryPort, true);
        zkClient.registerPrimary("localhost:" + nni.nnDnPort, 
        "127.0.0.1:" + nnDnPrimaryPort, true);
        zkClient.registerPrimary("localhost:" + nni.httpPort,
        "127.0.0.1:" + httpPrimaryPort, true);
        zkClient.registerPrimary("localhost:" + nni.rpcPort,
        "127.0.0.1:" + rpcPrimaryPort, true);
        try {
          zkClient.shutdown();
        } catch (InterruptedException ie) {
          throw new IOException("zkClient.shutdown() interrupted");
        }
        LOG.info("Closed zk client connection for registerZookeeper");
        return;
      } catch (IOException e) {
        LOG.info("Got exception when registering to zk, retrying", e);
        sleep(1000);
      }
    }
    throw new IOException("Cannot talk to ZK.");
  }

  public void clearZooKeeperNode(int nnIndex) throws IOException {
    int retries = 5;
    for(int i =0; i<retries; i++) {
      try {   
        NameNodeInfo nni = this.nameNodes[nnIndex];
        AvatarZooKeeperClient zkClient =
          new AvatarZooKeeperClient(nni.conf, null, false);
        zkClient.clearPrimary("localhost:" + nni.httpPort);
        zkClient.clearPrimary("localhost:" + nni.nnPort);
        zkClient.clearPrimary("localhost:" + nni.nnDnPort);
        zkClient.clearPrimary("localhost:" + nni.rpcPort);
        try {
          zkClient.shutdown();
        } catch (InterruptedException ie) {
          throw new IOException("zkClient.shutdown() interrupted");
        }
        LOG.info("Closed zk client connection for clearZKNode");
        return;
      } catch (IOException e) {
        LOG.info("Got exception when clearing zk, retrying", e);
        sleep(1000);
      }
    }
    throw new IOException("Cannot talk to ZK.");
  }

  static Configuration getServerConf(String startupOption,
      NameNodeInfo nni) {
    // namenode should use DFS, not DAFS

    if (startupOption.
               equals(AvatarConstants.StartupOption.NODEZERO.getName())) {
      return new Configuration(nni.a0Conf);
    } else if (startupOption.
               equals(AvatarConstants.StartupOption.NODEONE.getName())) {
      return new Configuration(nni.a1Conf);
    } else {
      throw new IllegalArgumentException("invalid avatar");
    }
  }
  
  public void registerZooKeeperNodes() throws IOException {
    for (NameNodeInfo nni : this.nameNodes) {
      nni.updateAvatarConf(this.conf);
      registerZooKeeperNode(nni.nn0Port, nni.nnDn0Port, nni.http0Port,
          nni.rpc0Port, nni);
    }
  }

  private void startAvatarNodes() throws IOException {
    for (NameNodeInfo nni: this.nameNodes) {
      nni.updateAvatarConf(this.conf);
      startAvatarNode(nni, startOpt);
    }
  }

  private void startAvatarNode(NameNodeInfo nni, StartupOption operation) throws IOException {
    registerZooKeeperNode(nni.nn0Port, nni.nnDn0Port, nni.http0Port,
        nni.rpc0Port, nni);

    if (format) {
      LOG.info("formatting");
      // Start the NameNode
      String[] a0FormatArgs; 
      ArrayList<String> argList = new ArrayList<String>();
      argList.add(AvatarConstants.StartupOption.
          NODEZERO.getName());
      argList.add(AvatarConstants.StartupOption.
          FORMATFORCE.getName());
      if (federation) {
        argList.add(StartupOption.SERVICE.getName());
        argList.add(nni.nameserviceId);
      }
      a0FormatArgs = new String[argList.size()];
      argList.toArray(a0FormatArgs);
      instantiateAvatarNode(a0FormatArgs, 
                            getServerConf(AvatarConstants.StartupOption.
                                                NODEZERO.getName(), nni));
    }
    ArrayList<AvatarInfo> avatars = new ArrayList<AvatarInfo>(2);
    {
      LOG.info("starting avatar 0");
      String[] a0Args; 
      ArrayList<String> argList = new ArrayList<String>();
      if (operation != null) {
        argList.add(operation.getName());
      }
      argList.add(AvatarConstants.StartupOption.NODEZERO.getName());
      if (federation) {
        argList.add(StartupOption.SERVICE.getName());
        argList.add(nni.nameserviceId);
      }
      a0Args = new String[argList.size()];
      argList.toArray(a0Args);
      AvatarNode a0 = instantiateAvatarNode(a0Args, 
                         getServerConf(AvatarConstants.
                                       StartupOption.
                                       NODEZERO.
                                       getName(), nni));

      avatars.add(new AvatarInfo(a0,
                                 AvatarState.ACTIVE,
                                 nni.nn0Port, nni.nnDn0Port, nni.http0Port, nni.rpc0Port,
                                 AvatarConstants.StartupOption.NODEZERO.
                                 getName()));
      
      // wait for up to 10 seconds until the ACTIVE is initialized
      for (int i = 0; i < 10; i++) {
        if (a0.isInitDone())
          break;
        LOG.info("Waiting for the ACTIVE to be initialized...");
        sleep(1000);
      }
      if (!a0.isInitDone()) {
        throw new IOException("The ACTIVE cannot be initialized");
      }
    }

    {
      LOG.info("starting avatar 1");
      String[] a1Args; 
      ArrayList<String> argList = new ArrayList<String>();
      argList.add(AvatarConstants.StartupOption.NODEONE.getName());
      argList.add(AvatarConstants.StartupOption.STANDBY.getName());
      argList.add(AvatarConstants.StartupOption.REGULAR.getName());
      if (federation) {
        argList.add(StartupOption.SERVICE.getName());
        argList.add(nni.nameserviceId);
      }
      a1Args = new String[argList.size()];
      argList.toArray(a1Args);
      avatars.add(new AvatarInfo(
          instantiateAvatarNode(a1Args, 
                                getServerConf(AvatarConstants.
                                                                StartupOption.
                                                                NODEONE.
                                                                getName(), nni)),
                                 AvatarState.STANDBY,
                                 nni.nn1Port, nni.nnDn1Port, nni.http1Port, nni.rpc1Port,
                                 AvatarConstants.StartupOption.NODEONE.
                                 getName()));
    }

    for (AvatarInfo avatar: avatars) {
      if (avatar.avatar == null) {
        throw new IOException("Cannot create avatar nodes");
      }
      Assert.assertTrue(
          avatar.avatar.getConf().getBoolean("dfs.persist.blocks", false));
    }
    nni.setAvatarNodes(avatars);
    DFSUtil.setGenericConf(nni.conf, nni.nameserviceId, 
        AvatarNode.AVATARSERVICE_SPECIFIC_KEYS);
    nni.updateAvatarConf(nni.conf);
  }

  public void restartAvatarNodes() throws Exception {
    logStateChange("Restarting avatar nodes");
    shutDownAvatarNodes();
    for (NameNodeInfo nni : this.nameNodes) {
      nni.avatars.clear();
    }
    this.format = false;
    startAvatarNodes();
    waitAvatarNodesActive();

    waitDataNodesActive();

    waitExitSafeMode();
    logStateChange("Restarting avatar nodes - completed");
  }
  
  /*
   * Adds all datanodes to shutdown list
   */
  private void processDatanodesForShutdown(Collection<Thread> threads) {
    for (int i = 0; i < dataNodes.size(); i++) {
      LOG.info("Shutting down data node " + i);
      Thread st = new Thread(new ShutDownUtil(dataNodes.get(i)));
      st.start();
      threads.add(st);
    }
  }
  
  /*
   * Adds all namenodes to shutdown list
   */
  private void processNamenodesForShutdown(Collection<Thread> threads) {
    for (NameNodeInfo nni : this.nameNodes) {
      for (AvatarInfo avatar: nni.avatars) {
        if (avatar.state == AvatarState.ACTIVE || 
            avatar.state == AvatarState.STANDBY) {
          LOG.info("Shutting down Avatar " + avatar.state);
          Thread st = new Thread(new ShutDownUtil(avatar));
          st.start();
          threads.add(st);
        }
      }
    }
  }

  public void shutDownDataNode(int i) throws IOException, InterruptedException {
    logStateChange("Shutting down datanode: " + i);
    dataNodes.get(i).datanode.shutdown();
    logStateChange("Shutting down datanode: " + i + " - completed");
  }

  public void shutDownDataNodes() throws IOException, InterruptedException {
    logStateChange("Shutting down avatar datanodes");
    List<Thread> threads = new ArrayList<Thread>();
    processDatanodesForShutdown(threads);
    MiniDFSCluster.joinThreads(threads);
    logStateChange("Shutting down avatar datanodes - completed");
  }

  private void shutDownJournalCluster() throws IOException {
  	if (journalCluster != null) {
  		journalCluster.shutdown();
  	}
  }
  
  public void shutDownAvatarNodes() throws IOException, InterruptedException {
    logStateChange("Shutting down avatar nodes");
    List<Thread> threads = new ArrayList<Thread>();
    processNamenodesForShutdown(threads);   
    MiniDFSCluster.joinThreads(threads);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignore) {
      // do nothing
    }
    logStateChange("Shutting down avatar nodes - completed");
  }

  public static void shutDownZooKeeper() throws IOException, InterruptedException {
    logStateChange("Shutting down zookeeper server");
    cnxnFactory.shutdown();
    cnxnFactory.join();
    LOG.info("Zookeeper Connection Factory shutdown");
    if (zooKeeper.isRunning()) {
      zooKeeper.shutdown();
    }
    logStateChange("Shutting down zookeeper server - completed");
  }
  
  /**
   * Shut down the cluster
   */
  public void shutDown() throws IOException, InterruptedException {
    logStateChange("Shutting down Mini Avatar Cluster");
    List<Thread> threads = new ArrayList<Thread>();   
    // add all datanodes to be shutdown
    processDatanodesForShutdown(threads);    
    // add all namenodes to be shutdown
    processNamenodesForShutdown(threads);   
    MiniDFSCluster.joinThreads(threads);
    shutDownJournalCluster();
    logStateChange("Shutting down Mini Avatar Cluster - completed");
  }

  private void startDataNodes(long[] simulatedCapacities) throws IOException {
    startDataNodes(simulatedCapacities, numDataNodes, hosts, racks, conf);
  }

  private void startDataNodes() throws IOException {
    startDataNodes(numDataNodes, racks, hosts, conf);
  }

  public void startDataNodes(int numDataNodes, String[] racks, String[] hosts,
      Configuration conf) throws IOException {
    startDataNodes(null, numDataNodes, racks, hosts, conf);
  }

  public void startDataNodes(long[] simulatedCapacities, int numDataNodes,
      String[] racks, String[] hosts, Configuration conf) throws IOException {
    int curDn = dataNodes.size();
    if (racks != null && numDataNodes > racks.length ) {
      throw new IllegalArgumentException( "The length of racks [" + 
                                          racks.length +
                                          "] is less than the number " +
                                          "of datanodes [" +
                                          numDataNodes + "].");
    }
    if (hosts != null && numDataNodes > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + 
                                          hosts.length +
                                          "] is less than the number " +
                                          "of datanodes [" +
                                          numDataNodes + "].");
    }

    //Generate some hostnames if required
    if (racks != null && hosts == null) {
      LOG.info("Generating host names for datanodes");
      hosts = new String[numDataNodes];
      for (int i = 0; i < numDataNodes; i++) {
        hosts[i] = "host" + (curDn + i) + ".foo.com";
      }
    }

    ArrayList<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < numDataNodes; i++) {
      Thread st = new Thread(new StartDatanodeUtil(i, curDn, simulatedCapacities));
      st.start();
      threads.add(st);
    }
    if(!MiniDFSCluster.joinThreads(threads)){
      throw new IOException("Failed to startup the nodes");
    }
    this.numDataNodes = dataNodes.size();
  }
  
  class StartDatanodeUtil implements Runnable {
    private int i;
    private int curDn;
    private long[] simulatedCapacities;
    
    StartDatanodeUtil(int node, int curDn, long[] simulatedCapacities) {
      this.i = node;
      this.curDn = curDn;
      this.simulatedCapacities = simulatedCapacities;
    }
    
    @Override
    public void run() {
      try {
        String dnArg = StartupOption.REGULAR.getName();
        if (startOpt != null && startOpt == StartupOption.ROLLBACK) {
          dnArg = startOpt.getName();
        }
        String[] dnArgs = { dnArg };
        int iN = curDn + i;
        Configuration dnConf = new Configuration(conf);

        if (simulatedCapacities != null) {
          dnConf.setBoolean("dfs.datanode.simulateddatastorage", true);
          dnConf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY,
              simulatedCapacities[i]);
        }

        File dir1 = new File(dataDir, "data" + (2 * iN + 1));
        File dir2 = new File(dataDir, "data" + (2 * iN + 2));
        dir1.mkdirs();
        dir2.mkdirs();
        if (!dir1.isDirectory() || !dir2.isDirectory()) {
          throw new IOException(
              "Mkdirs failed to create directory for DataNode " + iN + ": "
              + dir1 + " or " + dir2);
        }
        dnConf.set("dfs.data.dir", dir1.getPath() + "," + dir2.getPath());

        LOG.info("Starting DataNode " + iN + " with dfs.data.dir: "
            + dnConf.get("dfs.data.dir"));


        if (hosts != null) {
          dnConf.set(FSConstants.SLAVE_HOST_NAME, hosts[i]);
          LOG.info("Starting DataNode " + iN + " with hostname set to: "
              + dnConf.get(FSConstants.SLAVE_HOST_NAME));
        }

        if (racks != null) {
          String name = hosts[i];
          LOG.info("Adding node with hostname : " + name + " to rack "
              + racks[i]);
          StaticMapping.addNodeToRack(name, racks[i]);
        }
        Configuration newconf = new Configuration(dnConf); // save config
        AvatarDataNode dn = instantiateDataNode(dnArgs, dnConf);
        // since the HDFS does things based on IP:port, we need to add the
        // mapping
        // for IP:port to rackId

        String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
        if (racks != null) {
          int port = dn.getSelfAddr().getPort();
          System.out.println("Adding node with IP:port : " + ipAddr + ":"
              + port + " to rack " + racks[i]);
          StaticMapping.addNodeToRack(ipAddr + ":" + port, racks[i]);
        }
        dn.runDatanodeDaemon();
        synchronized (dataNodes) {
          dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));
        }
      } catch (IOException e) {
        LOG.error("Exception when creating datanode", e);
      }
    }
  }

  public void waitAvatarNodesActive() {
    for (int nnIndex = 0; nnIndex < this.nameNodes.length; nnIndex++) {
      waitAvatarNodesActive(nnIndex);
    }
  }

  public void waitAvatarNodesActive(int nnIndex) {
    NameNodeInfo nni = this.nameNodes[nnIndex];
    for (AvatarInfo avatar: nni.avatars) {
      while (avatar.avatar.getNameNodeDNAddress() == null) {
        try {
          logStateChange("Waiting for avatar");
          Thread.sleep(200);
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }
  }

  /* wait Datanodes active for all namespaces */
  public void waitDataNodesActive() throws IOException {
    if(conf.getBoolean("fs.datanodes.wait", true)) {
      for (int nnIndex = 0; nnIndex < this.nameNodes.length; nnIndex++) {
        waitDataNodesActive(nnIndex);
      }
    } else {
      LOG.info("Will not wait for datanodes");
    }
  }
  
  /* wait Datanodes active for specific namespaces */
  public void waitDataNodesActive(int nnIndex) throws IOException {
    DistributedAvatarFileSystem dafs = null;
    logStateChange("Waiting for data nodes");
    int liveDataNodes = 0;
    // make sure all datanodes are alive
    while(liveDataNodes != numDataNodes) {
      try {
        dafs = getFileSystem(nnIndex);
        Thread.sleep(200);
        liveDataNodes = dafs.getLiveDataNodeStats(false).length;
        logStateChange("Waiting for data nodes : live=" + liveDataNodes + ", total=" + numDataNodes);
      } catch (Exception e) {
        LOG.warn("Exception waiting for datanodes : ", e);
      } finally {
        if (dafs != null) {
          dafs.close();
        }
      }
    }
    logStateChange("Waiting for data nodes - completed");
  }
  
  private void checkSingleNameNode() {
    if (nameNodes.length != 1) {
      throw new IllegalArgumentException("It's not a single namenode cluster, use index instead.");
    }
  }

  public AvatarInfo getPrimaryAvatar(int nnIndex) {
    return getAvatarByState(nnIndex, AvatarState.ACTIVE);
  }
  
  public AvatarInfo getStandbyAvatar(int nnIndex) {
    return getAvatarByState(nnIndex, AvatarState.STANDBY);
  }
  
  private AvatarInfo getDeadAvatar(int nnIndex) {
    return getAvatarByState(nnIndex, AvatarState.DEAD);
  }

  private AvatarInfo getAvatarByState(int nnIndex, AvatarState state) {
    for (AvatarInfo avatar: this.nameNodes[nnIndex].avatars) {
      if (avatar.state == state) {
        return avatar;
      }
    }
    return null;
  }


  /**
   * Wait until the primary avatars have been checkpointed
   */
  private void waitForTheFirstCheckpoint() {
    if((!conf.getBoolean("fs.checkpoint.wait", true)) ||
        (!conf.getBoolean("fs.checkpoint.enabled", true))) {
      logStateChange("Waiting for checkpoint is disabled");
      return;
    }
    logStateChange("Waiting for first checkpoint");
    // wait for the first checkpoint to happen, as we
    // assert txids which depend on the checkpoints
    for (int nnIndex=0; nnIndex < this.nameNodes.length; nnIndex++) {
      while(!isCheckpointed(nnIndex)) {
        try {
          logStateChange("Waiting until avatar0 has been checkpointed");
          Thread.sleep(50);
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }
    logStateChange("Waiting for first checkpoint - completed");
  }
  
  /**
   * Return if the primary avatar has been checkpointed.
   */
  private boolean isCheckpointed(int nnIndex) {
    AvatarInfo primary = getPrimaryAvatar(nnIndex);
    return (primary != null && primary.avatar.getFSImage().getLastCheckpointTxId() > -1);
  }
  
  /**
   * Return true if primary avatar has left safe mode
   */
  private boolean hasLeftSafeMode(int nnIndex) throws IOException {
    AvatarInfo primary = getPrimaryAvatar(nnIndex);

    return (primary != null && !primary.avatar.isInSafeMode() && 
            (this.numDataNodes==0 || primary.avatar.getStats()[0] != 0)) ;
  }

  private void waitExitSafeMode() throws IOException {
    for (int nnIndex=0; nnIndex < this.nameNodes.length; nnIndex++) {
      // make sure all datanodes are alive
      while(!hasLeftSafeMode(nnIndex)) {
        try {
          logStateChange("Waiting until avatar0 has left safe mode");
          Thread.sleep(50);
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }
  }

  public DistributedAvatarFileSystem getFileSystem()
      throws IOException {
    checkSingleNameNode();
    return getFileSystem(0);
  }

  /**
   * Get DAFS.
   */
  public DistributedAvatarFileSystem getFileSystem(int nnIndex)
      throws IOException {
    FileSystem fs = FileSystem
        .get(this.nameNodes[nnIndex].clientConf);

    if (!(fs instanceof DistributedAvatarFileSystem)) {
      throw new IOException("fs is not avatar fs");
    }

    return (DistributedAvatarFileSystem) fs;
  }

  /**
   * Kill the primary avatar node.
   * @param updateZK clear zookeeper?
   */
  public void killPrimary() throws IOException {
    checkSingleNameNode();
    killPrimary(0, true);
  }
  
  public void killPrimary(int nnIndex) throws IOException {
    killPrimary(nnIndex, true);
  }
  
  public void killPrimary(boolean clearZK) throws IOException {
    checkSingleNameNode();
    killPrimary(0, clearZK);
  }

  /**
   * Kill the primary avatar node.
   * @param clearZK clear zookeeper?
   */
  public void killPrimary(int nnIndex, boolean clearZK) throws IOException {
    logStateChange("Killing primary avatar: " + nnIndex);
    AvatarInfo primary = getPrimaryAvatar(nnIndex);
    if (primary != null) {
      if (clearZK) {
        clearZooKeeperNode(nnIndex);
      }

      primary.avatar.shutdown(true);
      
      primary.avatar = null;
      primary.state = AvatarState.DEAD;

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
        // do nothing
      }
      logStateChange("Killing primary avatar: " + nnIndex + " - completed");
    } else {
      throw new IOException("can't kill primary avatar, already dead");
    }
  }

  public void killStandby() throws IOException {
    checkSingleNameNode();
    killStandby(0);
  }

  /**
   * Kill the standby avatar node.
   */
  public void killStandby(int nnIndex) throws IOException {
    logStateChange("Killing standby avatar: " + nnIndex);
    AvatarInfo standby = getStandbyAvatar(nnIndex);
    if (standby != null) {
      standby.avatar.shutdown(true);
      
      standby.avatar = null;
      standby.state = AvatarState.DEAD;

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
        // do nothing
      }
      logStateChange("Killing standby avatar: " + nnIndex + " - completed");
    } else {
      logStateChange("Can't kill standby avatar, already dead");
    }
  }

  public void failOver() throws IOException {
    failOver(false);
  }

  public void failOver(boolean force) throws IOException {
    checkSingleNameNode();
    failOver(0, force);
  }

  /**
   * Make standby avatar the new primary avatar. Kill the old
   * primary avatar first if necessary.
   */
  public void failOver(int nnIndex) throws IOException {
    failOver(nnIndex, false);
  }

  public void failOver(int nnIndex, boolean force) throws IOException {
    logStateChange("Failover avatar: " + nnIndex);
    if (getPrimaryAvatar(nnIndex) != null) {
      LOG.info("killing primary avatar before failover");
      killPrimary(nnIndex);
    }

    AvatarInfo standby = getStandbyAvatar(nnIndex);
    if (standby == null) {
      throw new IOException("no standby avatar running");
    }

    standby.avatar.quiesceForFailover(force);
    // Introduce a synthetic delay since this is what will happen in practice.
    // There will be some delay between both calls and this is to make sure
    // there are no locking issues since this was earlier one RPC under a single
    // lock and now its two RPCs which take the lock twice.
    DFSTestUtil.waitNSecond(5);
    standby.avatar.performFailover();
    standby.state = AvatarState.ACTIVE;
    registerZooKeeperNode(standby.nnPort, standby.nnDnPort, standby.httpPort,
        standby.rpcPort, this.nameNodes[nnIndex]);
    logStateChange("Failover avatar: " + nnIndex + " : completed");
  }
  
  public void restartStandby() throws IOException {
    checkSingleNameNode();
    restartStandby(0);
  }
  /**
   * Restart a dead avatar node as a standby avatar.
   */
  public void restartStandby(int nnIndex) throws IOException {
    AvatarInfo dead = getDeadAvatar(nnIndex);
    if (getPrimaryAvatar(nnIndex) == null || dead == null) {
      throw new IOException("cannot start standby avatar: " +
                            "primary or dead avatar not found");
      
    }
    logStateChange("Restarting " + dead.startupOption + " as standby");
    NameNodeInfo nni = this.nameNodes[nnIndex];
    String[] args; 
    ArrayList<String> argList = new ArrayList<String>();
    argList.add(dead.startupOption);
    argList.add(AvatarConstants.StartupOption.STANDBY.getName());
    argList.add(AvatarConstants.StartupOption.REGULAR.getName());
    if (federation) {
      argList.add(StartupOption.SERVICE.getName());
      argList.add(nni.nameserviceId);
    }
    args = new String[argList.size()];
    argList.toArray(args);
    dead.avatar = instantiateAvatarNode(args, getServerConf(dead.startupOption, nni));
    dead.state = AvatarState.STANDBY;

    if (dead.avatar == null) {
      throw new IOException("cannot start avatar node");
    }
    logStateChange("Restarting " + dead.startupOption + " as standby - completed");
  }
  
  /**
   * return NameNodeInfo 
   */
  public NameNodeInfo getNameNode(int nnIndex) {
    return this.nameNodes[nnIndex];
  }
  
  public ArrayList<DataNodeProperties> getDataNodeProperties() {
    return dataNodes;
  }
  
  /**
   * Gets a list of the started DataNodes.  May be empty.
   */
  public ArrayList<AvatarDataNode> getDataNodes() {
    ArrayList<AvatarDataNode> list = new ArrayList<AvatarDataNode>();
    for (int i = 0; i < dataNodes.size(); i++) {
      AvatarDataNode node = dataNodes.get(i).datanode;
      list.add(node);
    }
    return list;
  }
  
  /*
   * return number of namenodes
   */
  public int getNumNameNodes() {
    return this.nameNodes.length;
  }
  
  /**
   * Add a namenode to cluster and start it. Configuration of datanodes
   * in the cluster is refreshed to register with the new namenode.
   * @return newly started namenode
   */
  public NameNodeInfo addNameNode(Configuration conf)
      throws IOException {
    if(!federation) {
      throw new IOException("cannot add namenode to non-federated cluster");
    }
    int nnIndex = nameNodes.length;
    int numNameNodes = nameNodes.length + 1;
    NameNodeInfo[] newlist = new NameNodeInfo[numNameNodes];
    System.arraycopy(nameNodes, 0, newlist, 0, nameNodes.length);
    nameNodes = newlist;
    nameNodes[nnIndex] = new NameNodeInfo(nnIndex);
    
    NameNodeInfo nni = nameNodes[nnIndex];
    nni.createAvatarDirs();
    String nameserviceId = NAMESERVICE_ID_PREFIX + getNSId();
    String nameserviceIds = conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES);
    nameserviceIds += "," + nameserviceId;
    nni.initGeneralConf(conf, nameserviceId);
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);
    
    nni.updateAvatarConf(conf);
    startAvatarNode(nni, null);

    // Refresh datanodes with the newly started namenode
    for (DataNodeProperties dn : dataNodes) {
      DataNode datanode = dn.datanode;
      datanode.refreshNamenodes(conf);
    }
    // Wait for new namenode to get registrations from all the datanodes
    waitDataNodesActive(nnIndex);
    return nni;
  }
  
  private void updateAvatarConfWithServiceId(Configuration dstConf, Configuration srcConf,
      String nameserviceId) {
    for (String key: AvatarNode.AVATARSERVICE_SPECIFIC_KEYS) {
      String federationKey = DFSUtil.getNameServiceIdKey(
          key, nameserviceId);
      String value = srcConf.get(federationKey);
      if (value != null) {
        dstConf.set(federationKey, value);
      }
    }
    for (String key: NameNode.NAMESERVICE_SPECIFIC_KEYS) {
      String federationKey = DFSUtil.getNameServiceIdKey(
          key, nameserviceId);
      String value = srcConf.get(federationKey);
      if (value != null) {
        dstConf.set(federationKey, value);
      }
    }
  }
  
  /**
   * Add another cluster to current cluster and start it. Configuration of datanodes
   * in the cluster is refreshed to register with the new namenodes;
   */
  public void addCluster(MiniAvatarCluster cluster, boolean format)
      throws IOException, InterruptedException{
    if(!federation || !cluster.federation) {
      throw new IOException("Cannot handle non-federated cluster");
    }
    if (cluster.dataNodes.size() > this.dataNodes.size()) {
      throw new IOException("Cannot merge: new cluster has more datanodes the old one.");
    }
    this.shutDown();
    cluster.shutDown();
    
    int nnIndex = nameNodes.length;
    int numNameNodes = nameNodes.length + cluster.nameNodes.length;
    NameNodeInfo[] newlist = new NameNodeInfo[numNameNodes];
    System.arraycopy(nameNodes, 0, newlist, 0, nameNodes.length);
    System.arraycopy(cluster.nameNodes, 0, newlist, nameNodes.length, 
        cluster.nameNodes.length);
    nameNodes = newlist;
    String newNameserviceIds = cluster.conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES);
    String nameserviceIds = conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES);
    nameserviceIds += "," + newNameserviceIds;
    this.format = format;
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);

    int i;
    for (i = 0; i < nameNodes.length; i++) {
      NameNodeInfo nni = nameNodes[i];
      String nameserviceId = nni.nameserviceId;
      nni.initGeneralConf(nni.conf, nni.nameserviceId);
      nni.updateAvatarConf(nni.conf);
      for (int dnIndex = 0; dnIndex < dataNodes.size(); dnIndex++) {
        Configuration dstConf = dataNodes.get(dnIndex).conf;
        if (i >= nnIndex) {
          String dataStr = cluster.dataNodes.get(dnIndex).conf.get("dfs.data.dir");
          dstConf.set("dfs.merge.data.dir." + nameserviceId, dataStr);
        }
        updateAvatarConfWithServiceId(dstConf, nni.conf, nameserviceId);
      }
    }

    for (DataNodeProperties dn : dataNodes) {
      dn.conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);
      dn.datanode = instantiateDataNode(dn.dnArgs, dn.conf);
      dn.datanode.runDatanodeDaemon();
    }

    for (i = 0; i < nameNodes.length; i++) {
      NameNodeInfo nni = nameNodes[i];
      Thread.sleep(2000);
      if (i < nnIndex) {
        startAvatarNode(nni, StartupOption.UPGRADE);
      } else {
        startAvatarNode(nni, null);
      }
    }
    waitAvatarNodesActive();
    waitDataNodesActive();
    waitExitSafeMode();
  }

  public synchronized boolean restartDataNodes() throws IOException,
      InterruptedException {
    return restartDataNodes(true);
  }
  
  public synchronized void restartDataNode(boolean waitActive, int index)
      throws IOException, InterruptedException {
    this.shutDownDataNode(index);
    DataNodeProperties dn = dataNodes.get(index);
    LOG.info("Restart Datanode " + index);
    // Use the same port since dn is identified by host:port.
    int port = dn.datanode.getSelfAddr().getPort();
    dn.conf.set(FSConstants.DFS_DATANODE_ADDRESS_KEY, "localhost:" + port);
    dn.datanode = instantiateDataNode(dn.dnArgs, dn.conf);
    dn.datanode.runDatanodeDaemon();
    if (waitActive) {
      waitDataNodeInitialized(dn.datanode);
    }
  }

  /*
   * Restart all datanodes
   */
  public synchronized boolean restartDataNodes(boolean waitActive)
      throws IOException, InterruptedException {
    logStateChange("Restarting avatar datanodes");
    shutDownDataNodes();
    for (int i = 0; i < dataNodes.size(); i++) {
      restartDataNode(waitActive, i);
    }
    if (waitActive) {
      waitDataNodesActive();
    }
    logStateChange("Restarting avatar datanodes - completed");
    return true;
  }
  
  /**
   * Wait until the Datanode is initialized, or it throws an IOException
   * @param AvatarDataNode dn;
   * @throws IOException when some ServicePair threads are dead. 
   */
  public synchronized void waitDataNodeInitialized(AvatarDataNode dn) throws IOException {
    if (dn == null) {
      return ;
    }
    boolean initialized = false;
    while (!initialized) {
      initialized = true;
      for (int i = 0; i<nameNodes.length; i++) { 
        InetSocketAddress nameNodeAddr = new InetSocketAddress("localhost",
            getNameNode(i).avatars.get(0).nnDnPort);
        if (!dn.initialized(nameNodeAddr)) {
          initialized = false;
          break;
        }
      }
      try {
        Thread.sleep(100);
      } catch (Exception e) {
      }
    }
  }
  
  public int getNamespaceId(int index) {
    return this.nameNodes[index].avatars.get(0).avatar.getNamespaceID();
  }
  
  static public int getNSId() {
    return MiniAvatarCluster.currNSId++;
  }
  
  public static AvatarDataNode instantiateDataNode(String[] dnArgs,
      Configuration conf) throws IOException {
    IOException e = null;
    for (int i = 0; i < instantiationRetries; i++) {
      try {
        return AvatarDataNode.instantiateDataNode(dnArgs, new Configuration(
            conf));
      } catch (IOException ioe) {
        e = ioe;
        LOG.info("Trying to instantiate datanode... ", e);
      }
      sleep(1000);
    }
    LOG.fatal("Exception when instantiating avatardatanode", e);
    throw e;
  }
  
  public static AvatarNode instantiateAvatarNode(String argv[],
      Configuration conf) throws IOException {
    IOException e = null;
    for (int i = 0; i < instantiationRetries; i++) {
      try {
        return AvatarNode.createAvatarNode(argv, conf);
      } catch (IOException ioe) {
        e = ioe;
        LOG.info("Trying to instantiate avatarnode... ", e);
      }
      sleep(1000);
    }
    LOG.fatal("Exception when instantiating avatarnode", e);
    throw e;
  }
  
  public static void clearAvatarDir() {
    try {
      FileUtil.fullyDelete(new File(baseAvatarDir));
    } catch (Exception e) {
      LOG.warn("Exception when deleting directory " + baseAvatarDir, e);
    }
  }
  
  private static void sleep(long time) throws IOException {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      LOG.fatal("Thread interrupted");
      throw new IOException(e.toString());
    }
  }
  
  private static void logStateChange(String msg) {
    LOG.info("----- " + msg + " -----");
  }
}
