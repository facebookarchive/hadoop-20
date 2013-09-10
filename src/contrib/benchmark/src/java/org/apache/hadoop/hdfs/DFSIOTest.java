package org.apache.hadoop.hdfs;

import java.io.IOException;
import org.apache.hadoop.mapred.ReadMapper;
import org.apache.hadoop.mapred.Reduce;
import org.apache.hadoop.mapred.WriteMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class DFSIOTest extends Configured implements Constant, Tool{


	private static long bufferLimitR, bufferLimitW;
	private static long fileSize;
	private static long nmaps;
	private static Configuration fsConfig;
	private static long blocksize;
	private static long ntasks;
	private static boolean ifPrintProfile = false;
	public static long replications;

	public static void printUsage() {
		System.out.println("USAGE: bin/hadoop hadoop-*-benchmark.jar dfstest nMaps fileSize " +
				"[-bufferRead] [-bufferWrite] [-blocksize] [-filesPerTask]" +
				"[-replications] [-printProfile]");
		System.out.println("Default bufferRead = 1000000\n" +
				"Default bufferWrite = 1000000");
		System.out.println("Default blocksize = " + BLOCKSIZE);
		System.out.println("Default filesPerTask = " + NSUBTASKS);
		System.out.println("Default replication = " + 1);
		System.exit(0);
	}

	public void control(Configuration fsConfig, String fileName)
			throws IOException {
		String name = fileName;
		FileSystem fs = FileSystem.get(fsConfig);
		fs.delete(new Path(DFS_INPUT, name), true);

		SequenceFile.Writer write = null;
		for (int i = 0; i < nmaps; i++) {
			try {
				Path controlFile = new Path(DFS_INPUT, name + i);
				write = SequenceFile.createWriter(fs, fsConfig, controlFile,
						Text.class, LongWritable.class, CompressionType.NONE);
				write.append(new Text(name + i), new LongWritable(fileSize));
			} finally {
				if (write != null)
					write.close();
				write = null;
			}
		}
	}


	@Override
	public int run(String[] args) throws IOException {

		long startTime = System.currentTimeMillis();
		if (args.length < 2) {
			printUsage();
		}
		nmaps = Long.parseLong(args[0]);
		fileSize = Long.parseLong(args[1]);

		bufferLimitR = BUFFERLIMIT;
		bufferLimitW = BUFFERLIMIT;
		blocksize = BLOCKSIZE;
		ntasks = NSUBTASKS;
		replications=1;

		for (int i = 2; i < args.length; i++) {
			if (args[i].equals("-bufferRead")) bufferLimitR = Long.parseLong(args[++i]); else
			if (args[i].equals("-bufferWrite")) bufferLimitW = Long.parseLong(args[++i]); else
			if (args[i].equals("-blocksize")) blocksize = Long.parseLong(args[++i]); else
			if (args[i].equals("-filesPerTask")) ntasks = Long.parseLong(args[++i]); else
			if (args[i].equals("-printProfile")) ifPrintProfile = true; else
			if (args[i].equals("-replications")) replications = Long.parseLong(args[++i]); else
				printUsage();
		}

    System.out.println("Auto print Write Profile? " + ifPrintProfile);

		// running the Writting
		fsConfig = new Configuration(getConf());
		fsConfig.setLong("dfs.block.size", blocksize * MEGA);
		fsConfig.set("dfs.buffer.size.read", String.valueOf(bufferLimitR));
		fsConfig.set("dfs.buffer.size.write", String.valueOf(bufferLimitW));
		fsConfig.set("dfs.nmaps", String.valueOf(nmaps));
		fsConfig.set("dfs.nTasks", String.valueOf(ntasks));
		fsConfig.setInt("dfs.replication", (int)replications);
		FileSystem fs = FileSystem.get(fsConfig);

		if (fs.exists(new Path(DFS_OUTPUT)))
			fs.delete(new Path(DFS_OUTPUT), true);
		if (fs.exists(new Path(DFS_INPUT)))
			fs.delete(new Path(DFS_INPUT), true);
		if (fs.exists(new Path(INPUT)))
			fs.delete(new Path(INPUT), true);
		if (fs.exists(new Path(OUTPUT)))
			fs.delete(new Path(OUTPUT), true);
		fs.delete(new Path(TRASH), true);

		// run the control() to set up for the FileSystem
		control(fsConfig, "testing");

		// prepare the for map reduce job
		JobConf conf = new JobConf(fsConfig, DFSIOTest.class);
		conf.setJobName("dfstest-writing");

		// set the output and input for the map reduce
		FileOutputFormat.setOutputPath(conf, new Path(DFS_OUTPUT + "writing"));
		FileInputFormat.setInputPaths(conf, new Path(DFS_INPUT));

		conf.setInputFormat(SequenceFileInputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(WriteMapper.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(1);
		conf.setSpeculativeExecution(false);
		conf.setBoolean("dfs.write.profile.auto.print", ifPrintProfile);
		JobClient.runJob(conf);

		// Reading test
		conf = new JobConf(fsConfig, DFSIOTest.class);
		conf.setJobName("dfstest-reading");
		FileOutputFormat.setOutputPath(conf, new Path(DFS_OUTPUT + "reading"));
		FileInputFormat.setInputPaths(conf, new Path(DFS_INPUT));

		conf.setInputFormat(SequenceFileInputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(ReadMapper.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(1);
		JobClient.runJob(conf);

		// printout the result
		System.out.println("-------------------");
		System.out.println("RESULT FOR WRITING");
		System.out.println("-------------------");
		FSDataInputStream out = fs.open(new Path(OUTPUT,"result-writing"));
		while (true) {
			String temp = out.readLine();
			if (temp == null)
				break;
			System.out.println(temp);
		}
		out.close();

		System.out.println("------------------");
		System.out.println("RESULT FOR READING");
		System.out.println("------------------");
		out = fs.open(new Path(OUTPUT, "result-reading"));
		while (true) {
			String temp = out.readLine();
			if (temp == null)
				break;
			System.out.println(temp);
		}
		out.close();
		long endTime = System.currentTimeMillis();

		System.out.println("------------------");
		double execTime = (endTime - startTime) / 1000.0;
		String unit = "seconds";
		if (execTime > 60) {
			execTime /= 60.0;
			unit = "mins";
		}
		if (execTime > 60) {
			execTime /= 60.0;
			unit = "hours";
		}
		System.out.println("Time executed :\t" + execTime + " " + unit);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new DFSIOTest(), args));
	}

}
