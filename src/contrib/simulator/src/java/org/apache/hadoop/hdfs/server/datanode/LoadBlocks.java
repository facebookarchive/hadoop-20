package org.apache.hadoop.hdfs.server.datanode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


public class LoadBlocks {
	static final String CURRENT = "/current";
	static final String META_EXTENSION = ".meta";

	public static final Log LOG = LogFactory.getLog(LoadBlocks.class);
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	/*
	 * this nulCrcFileData is helping us pass by the checksum of HDFS
	 */
	public static byte[] nullCrcFileData;
	private Configuration conf;
	private static int maxFilesPerData;
	private static int numDataNodes;
	private static int numDirsPerData;
	private static String baseDir = null;
	public static final String MYTEST = "/SIMULATED_CLUSTER";
	private static String srcFile = "/Users/dotannguyen/work/fsimage/test.fsimage";

	static final String USAGE = "USAGE: loadblocks "
			+ " -n <numDataNodes> "
			+ " -s sourceOfFsimage"
			+ " [-name nameOfDataNode]"
			+ " [-t numThreadsForEachDir]"
			+ " [-limit maxNumOfBlocks]"
			+ "\n"
			+ "		 Default number of thread is 1.\n"
			+ "		 Default nameOfDataNode is DataNode.\n"
			+ "	     Default limit is Integer.MAX_VALUE\n" 
			+ "You can configure the dirs in conf/hdfs-site.xml with property dfs.data.dir.\n";
	private static final int LIMIT = Integer.MAX_VALUE;

	public LoadBlocks(Configuration conf) {
		this.conf = conf;
	}

	private DataNodeContainer[] dnContainer;
	private int numThreads = 1;
	private int limit = LIMIT;
	private String nameData = "DataNode";

	class FileGenerator {
		private int start, amount;
		private List<Block> blocks;
		private FileNameGenerator generator;

		public FileGenerator(FileNameGenerator generator, List<Block> blocks,
				int amount) {
			this.generator = generator;
			this.amount = amount;
			this.blocks = blocks;
		}

		public void createFiles() {
			for (int i = start; i < start + amount; i++) {
				Block block = blocks.get(i);
				String name = generator.getNextFileName(block.getId());
				try {
					File blk_file = new File(name);
					File meta_file = new File(name + "_" + block.getStamp()
							+ META_EXTENSION);
					blk_file.getParentFile().mkdirs(); 
					meta_file.createNewFile();
					BufferedWriter out = new BufferedWriter(new FileWriter(blk_file));
					out.write(String.valueOf(block.getBytes()));
					out.close();

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	class DataNodeDirThread implements Callable<Object> {
		private int dnNum, disk;
		private String baseDir;
		private FileNameGenerator generator;

		public DataNodeDirThread(String baseDir, int dnNum, int disk) {
			this.dnNum = dnNum;
			this.disk = disk;
			this.baseDir = baseDir;
			this.generator = new FileNameGenerator(baseDir, maxFilesPerData);
		}

		public Object call() {
			List<Block> blocks = dnContainer[dnNum].getBlock(disk);
			int amount = blocks.size();
			LOG.info("Generating files for " + baseDir + " with " + amount
					+ " blocks.");
			new FileGenerator(generator, blocks, amount).createFiles();
			return null; 
		}
	}

	class DiskThread extends Thread {

		String baseDir;
		int disk;
		ExecutorService datanodeExecutor;

		public DiskThread(String baseDir, int disk) {
			this.baseDir = baseDir;
			this.disk = disk;
			this.datanodeExecutor = Executors.newFixedThreadPool(numThreads);
		}

		public void run() {
			DataNodeDirThread[] datanodeThreads = new DataNodeDirThread[numDataNodes];
			List<Future<?>> futures = new ArrayList<Future<?>>();
			for (int i = 0; i < numDataNodes; i += 1) {
				String dir = baseDir + "/"+ nameData + i + "/current";
				File currentDir = new File(dir); 
				currentDir.mkdirs();
				futures.add(datanodeExecutor.submit((new DataNodeDirThread(dir, i, disk))));
			}
			
			for (Future<?> future : futures) {
				try {
					future.get();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				} catch (ExecutionException e) {
					throw new RuntimeException(e);
				}
			}
			// shut down when everything is done
			datanodeExecutor.shutdown();
		}
	}
	
	public void printUsageExit(String s) {
		System.out.println("[ERROR] : " + s);
		System.out.println(USAGE); 
		System.exit(1); 
	}

	public void run(String[] args) throws IOException, InterruptedException {

		// Initialize our arguments
		maxFilesPerData = 100;
		numDataNodes = 0;
		numDirsPerData = 0;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-n"))
				numDataNodes = Integer.parseInt(args[++i]);
			else if (args[i].equals("-name"))
				nameData = args[++i]; 
			else if (args[i].equals("-d"))
				baseDir = args[++i];
			else if (args[i].equals("-s"))
				srcFile = args[++i];
			else if (args[i].equals("-t"))
				numThreads = Integer.parseInt(args[++i]);
			else if (args[i].equals("-limit"))
				limit = Integer.parseInt(args[++i]);
			else {
				printUsageExit("Unknow arguments");
			}
		}
		

		long start = System.currentTimeMillis();
		String[] dataDirsRoot = conf.getStrings("dfs.data.dir");
		LOG.info(Arrays.toString(dataDirsRoot));
		if (dataDirsRoot.length == 0) {
			if (baseDir == null) 
				printUsageExit("You forgot to verify the dfs.data.dir in configuration file or pass value to -d.");
			else 
			dataDirsRoot = new String[] { baseDir };
		}
		numDirsPerData = dataDirsRoot.length;
		
		if (numDataNodes == 0 || numDirsPerData == 0) {
			printUsageExit("You didn't verify correctly for numDataNodes and numDirsPerData."); 
		}
		
		// load FSimage into here
		distributeBlocks(numDataNodes);
		LOG.info("Creating blocks file and metafile [READY]");

		// see the distribution of blocks on data nodes
		for (int i = 0; i < numDataNodes; i++) {
			long total = 0;
			for (int j = 0; j < numDirsPerData; j++) {
				total += dnContainer[i].getBlock(j).size();
			}
			LOG.info("Data Node " + i + " contains " + total + " blocks.");
		}
		DiskThread[] creates = new DiskThread[numDirsPerData];

		for (int i = 0; i < dataDirsRoot.length; i++) {
			(creates[i] = new DiskThread(dataDirsRoot[i], i)).start();
		}

		// wait for all threads to stop
		for (int i = 0; i < creates.length; i++) {
			try { 
			creates[i].join();
			} catch (InterruptedException e) { 
				throw new RuntimeException(e); 
			}
		}
		long exec = (System.currentTimeMillis() - start) / 1000;
		String result = "";
		if (exec > 60)
			result = (exec / 60) + " mins";
		else
			result = exec + " seconds";

		LOG.info("Generating files runs in " + result);
	}

	public void distributeBlocks(int numDataNodes) throws IOException {

		dnContainer = new DataNodeContainer[numDataNodes];
		for (int i = 0; i < numDataNodes; i++) {
			dnContainer[i] = new DataNodeContainer();
		}

		String src = srcFile;
		int replicates = conf.getInt("dfs.replication", 3);
		File input = new File(src);
		BufferedReader read = new BufferedReader(new FileReader(input));
		String str = "";
		Block block = null;
		int count = 0;
		int index = 0; 
		while ((str = read.readLine()) != null) {
			if ((block = getNextBlock(read, str)) != null) {
				count++;
				for (int i = 0; i < replicates; i++) { 
					dnContainer[(index++) % numDataNodes].addBlock(block); 
				}
			}
			if (count == limit)
				break;
		}
		read.close();
	}

	public Block getNextBlock(BufferedReader read, String str)
			throws IOException {
		String[] splits = str.split(" "); 
		if (splits.length != 3) {
			throw new IOException("File is corrupted.");
		}
		return new Block(splits[0], Long.parseLong(splits[1]), splits[2]); 
	}

	class Block {
		private String blk_id, genStamp;
		private long length;

		public Block(String blk_id, long length, String genStamp) {
			this.blk_id = blk_id;
			this.length = length;
			this.genStamp = genStamp;
		}

		public String getId() {
			return "blk_" + blk_id;
		}

		public String getStamp() {
			return genStamp;
		}

		public long getBytes() {
			return length;
		}
	}

	class Dir {
		private List<Block> containers = new LinkedList<Block>();

		public void addBlock(Block block) {
			containers.add(block);
		}

		public List<Block> getBlocks() {
			return containers;
		}
	}

	class DataNodeContainer {
		private Dir[] dirs = new Dir[numDirsPerData];

		public DataNodeContainer() {
			for (int i = 0; i < numDirsPerData; i++) {
				dirs[i] = new Dir();
			}
		}

		public void addBlock(Block block) {
			Random rand = new Random();
			dirs[rand.nextInt(numDirsPerData)].addBlock(block);
		}

		public List<Block> getBlock(int dir) {
			return dirs[dir].getBlocks();
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		new LoadBlocks(new Configuration()).run(args);
	}
}
