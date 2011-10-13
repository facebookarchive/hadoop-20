package org.apache.hadoop.hdfs.server.datanode;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 
 * 
 * 
 * This program starts a mini cluster of data nodes (ie a mini cluster without
 * the name node), all within one address space. It is assumed that the name
 * node has been started separately prior to running this program.
 * 
 * A use case of this is to run a real name node with a large number of
 * simulated data nodes for say a NN benchmark.
 * 
 * Synopisis: DataNodeCluster -n numDatNodes [-racks numRacks] -simulated
 * [-inject startingBlockId numBlocksPerDN] [ -r replicationForInjectedBlocks ]
 * [-d editsLogDirectory]
 * 
 * if -simulated is specified then simulated data nodes are started. if -inject
 * is specified then blocks are injected in each datanode; -inject option is
 * valid only for simulated data nodes.
 * 
 * See Also @link #CreateEditsLog for creating a edits log file to inject a
 * matching set of blocks into into a name node. Typical use of -inject is to
 * inject blocks into a set of datanodes using this DataNodeCLuster command and
 * then to inject the same blocks into a name node using the CreateEditsLog
 * command.
 * 
 */

public class SimulatedDataNode {
	private static String nameData = "SIMULATED_CLUSTER";
	public static final Log LOG = LogFactory.getLog(SimulatedDataNode.class);
	static int numDataNodes = 0;
	private static final String USAGE = "Usage: SimulatedDataNode "
			+ " -n <numDataNodes> "
			+ " [-start startingDataNode]"
			+ " [-format]"
			+ " [-r replicationFactorForInjectedBlocks]"
			+ " [-importCheckpoint sourceOfFsimageInXMLtype]"
			+ "\n"
			+ "		 Default directories are in /mnt/d[0-12]/SIMULATED_CLUSTER/DataNode[0-50]\n";
	private static String[] baseDirs;
	private static int limit = Integer.MAX_VALUE;

	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	static void printUsageExit() {
		System.out.println(USAGE);
		System.exit(-1);
	}

	static void printUsageExit(String err) {
		System.out.println(err);
		printUsageExit();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, LoginException {
		int replication = 1;
		boolean format = false, checkpoint = false;
		int start = 0;
		String srcFSimage = null;

		Configuration conf = new Configuration();
		baseDirs = conf.getStrings("dfs.data.dir");
		int disks = baseDirs.length;

		for (int i = 0; i < args.length; i++) { // parse command line
			if (args[i].equals("-n")) {
				if (++i >= args.length || args[i].startsWith("-")) {
					printUsageExit("missing number of nodes");
				}
				numDataNodes = Integer.parseInt(args[i]);
			} else if (args[i].equals("-nameData")) { 
				if (++i >= args.length || args[i].startsWith("-"))
					printUsageExit(); 
				nameData = args[i]; 
			} else if (args[i].equals("-r")) {
				if (++i >= args.length || args[i].startsWith("-")) {
					printUsageExit("Missing replicaiton factor");
				}
				replication = Integer.parseInt(args[i]);
			} else if (args[i].equals("-format")) {
				format = true;
			} else if (args[i].equals("-start")) {
				if (++i >= args.length || args[i].startsWith("-")) {
					printUsageExit("Missing starting datanode parameter");
				}
				start = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-s")) {
				if (++i >= args.length || args[i].startsWith("-"))
					printUsageExit("Missing checkpoint parameter");
				checkpoint = true;
				srcFSimage = args[i];
			} else if (args[i].equals("-limit")) { 
				if (++i >= args.length || args[i].startsWith("-")) 
					printUsageExit("Mssing limit arguement");
				limit  = Integer.parseInt(args[i]);
			} else {
				printUsageExit();
			}
		}

		if (numDataNodes <= 0 || replication <= 0) {
			printUsageExit("numDataNodes and replication have to be greater than zero");
		}
		if (replication > numDataNodes) {
			printUsageExit("Replication must be less than or equal to numDataNodes");
		}

		String nameNodeAdr = FileSystem.getDefaultUri(conf).getAuthority();
		InetSocketAddress nameNodeAddr = DataNode.getNameNodeAddress(conf);
		SimulatedDataNode.LOG.info("Address of NameNode is "
				+ nameNodeAddr.toString());

		if (nameNodeAdr == null) {
			SimulatedDataNode.LOG
					.info("No name node address and port in config");
			System.exit(-1);
		}

		conf.setBoolean("dfs.simulated.datanode", true);

		SimulatedDataNode.LOG.info("Starting up " + numDataNodes
				+ " Simulated Data Nodes that will connect to Name Node at "
				+ nameNodeAdr);

		final DataNodeThread[] dn = new DataNodeThread[numDataNodes];
		// running multiple DataNodes at the same time
		for (int i = start; i < start + numDataNodes; i++) {
			dn[i - start] = new DataNodeThread(conf, true, format, i,
					replication, disks);
			dn[i - start].start();
		}

		for (int i = 0; i < numDataNodes; i++) {
			dn[i].join();
		}
		
		if (checkpoint) {
			waitActive(conf, numDataNodes, dn);
			LOG.info("Waiting Active [DONE]");
			for (int i = 0; i < dn.length; i++) {
				dn[i].stopDataNode();
			}

			int threads = Math.min(50, numDataNodes);
			String arg = "-n " + numDataNodes + " -t "
					+ threads + " -s " + srcFSimage + " -limit " + limit;
			LoadBlocks loadBlock = new LoadBlocks(conf);
			loadBlock.run(arg.split(" "));
			
			for (int i = start; i < start + numDataNodes; i++) {
				dn[i - start] = new DataNodeThread(conf, true, false, i, replication, disks);
				dn[i - start].start();
			}
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() { 
			public void run() {
				LOG.info("Shutting down Persited Simulated DataNodes"); 
				for (int i = 0; i < numDataNodes; i++) 
					dn[i].stopDataNode();
			}
		}); 
	}

	public static void waitActive(Configuration conf, int numDataNodes,
			DataNodeThread[] dn) throws IOException, LoginException {

		InetSocketAddress addr = NameNode.getDNProtocolAddress(conf);
		if (addr != null) {
			SimulatedDataNode.LOG.info("Connecting to server "
					+ addr.toString());
		}
		
		UnixUserGroupInformation ugi = UnixUserGroupInformation.login(conf);
	    UserGroupInformation.setCurrentUser(ugi);
	    
		addr = new InetSocketAddress(NameNode.getDNProtocolAddress(conf).getHostName(),
				NameNode.getDNProtocolAddress(conf).getPort());
		SimulatedDataNode.LOG.info("[DFSClient connected to] : "
				+ NameNode.getDNProtocolAddress(conf).getHostName());
		DFSClient client = new DFSClient(addr, conf);

		int countedDataNodes = 0;
		while (countedDataNodes < numDataNodes) {
			DatanodeInfo[] dnInfo = client
					.datanodeReport(DatanodeReportType.LIVE);
			countedDataNodes = 0;
			for (int i = 0; i < dnInfo.length; i++) {
				String fullDNInfoAddress = dnInfo[i].getHostName();
				if (fullDNInfoAddress.endsWith("."))
					fullDNInfoAddress = fullDNInfoAddress.substring(0,
							fullDNInfoAddress.length() - 1)
							+ ":"
							+ dnInfo[i].getPort();
				else
					fullDNInfoAddress += ":" + dnInfo[i].getPort();

				for (int j = 0; j < dn.length; j++) {
					InetSocketAddress address = dn[j].getDataNode().datanode
							.getSelfAddr();
					String fullAddress = DNS.getDefaultHost("default",
							"default") + ":" + address.getPort();
					if (fullAddress.equals(fullDNInfoAddress)) {
						countedDataNodes++;
					}
				}
			}
			try {
				SimulatedDataNode.LOG
						.info("Waiting for all Data Nodes to be alive.");
				Thread.sleep(500);
			} catch (Exception e) {
			}
		}
		client.close();
	}

	static class DataNodeThread extends Thread {
		Configuration conf;
		boolean manageDfsDirs, format;
		String[] rack4DataNode;
		int start, replication, disks;
		private StartingSimulatedDataNode startDFS;

		public DataNodeThread(Configuration conf, boolean manageDfsDirs,
				boolean format, int start, int replicate, int disks) {
			this.conf = new Configuration(conf); // save the previous config.
			this.manageDfsDirs = manageDfsDirs;
			this.format = format;
			this.start = start;
			this.replication = replicate;
			this.disks = disks;
		}

		public void stopDataNode() {
			// TODO Auto-generated method stub
			startDFS.stopAllDataNodes();
		}

		public void run() {
			try {
				startDFS = new StartingSimulatedDataNode();
				// change
				
				startDFS.startDataNodes(conf, manageDfsDirs,
						StartupOption.REGULAR, start, disks, format);
			} catch (IOException e) {
				System.out.println("Error creating data node:" + e);
			}
		}

		public void restartDataNode() throws IOException {
			startDFS.restartDataNode();
		}

		public DataNodeProperties getDataNode() {
			return startDFS.getDatanodeProperties();
		}
	}

	static class DataNodeProperties {
		DataNode datanode;
		Configuration conf;
		String[] dnArgs;

		DataNodeProperties(DataNode node, Configuration conf, String[] args) {
			this.datanode = node;
			this.conf = conf;
			this.dnArgs = args;
		}
	}

	static class StartingSimulatedDataNode {
		private DataNodeProperties dataNode = null; 

		public StartingSimulatedDataNode() {
		}

		public DataNodeProperties getDatanodeProperties() {
			return dataNode;
		}

		public boolean stopAllDataNodes() {
			dataNode.datanode.shutdown();
			return true; 
		}

		public boolean restartDataNode() throws IOException {
			Configuration conf = new Configuration(dataNode.conf); 
			String[] args = dataNode.dnArgs;
			
			dataNode = new DataNodeProperties(DataNode.createDataNode(args, conf), conf, args);
			return true; 
		}

		public void formatDataNodeDirs(int numDirsPerData, int start)
				throws IOException {
			for (String str : baseDirs) { 
				File data_dir = new File(str + "/" + nameData + start);
				LOG.info("Deleting " + data_dir.getPath() + " for formatting.");
				if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) { 
					throw new IOException("Cannot remove data directory: " + data_dir);
				}
			}
		}

		public synchronized void startDataNodes(Configuration conf,
				boolean manageDfsDirs, StartupOption operation, int start,
				int disks, boolean format) throws IOException {

			try {
				if (format)
					formatDataNodeDirs(disks, start);
			} catch (IOException e) {
				System.out.println("Error formating data node dirs:" + e);
			}


			// set the initialDelay = 1/3 initialDelay of DFSTEST
			if (conf.get("dfs.blockreport.initialDelay") == null) {
				conf.setLong("dfs.blockreport.initialDelay", 10);
			}
			// set up the port for DataNodes
			conf.set("dfs.datanode.address", "0.0.0.0:0");
			conf.set("dfs.datanode.http.address", "0.0.0.0:0");
			conf.set("dfs.datanode.ipc.address", "0.0.0.0:0");

			String[] dnArgs = (operation == null || operation != StartupOption.ROLLBACK) ? null
					: new String[] { operation.getName() };

			Configuration dnConf = new Configuration(conf);

			if (manageDfsDirs) {
				String dirs = "";
				for (String str : baseDirs) {
					File currentDir = new File(str + "/" + nameData + start);
					if (!currentDir.exists())
						currentDir.mkdir();
					dirs += currentDir.getPath() + ",";
				}
				dnConf.set("dfs.data.dir", dirs);
			}
			System.out.println("Starting DataNode " + start
					+ " with dfs.data.dir: " + dnConf.get("dfs.data.dir"));
			Configuration newconf = new Configuration(dnConf);
			// set up for start up
			DataNode dn = DataNode.instantiateDataNode(dnArgs, dnConf);
			DataNode.runDatanodeDaemon(dn);

			dataNode = new DataNodeProperties(dn, newconf, dnArgs);
		}
	}
}
