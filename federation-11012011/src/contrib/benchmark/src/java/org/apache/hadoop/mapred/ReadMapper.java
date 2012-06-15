package org.apache.hadoop.mapred;
import org.apache.hadoop.hdfs.Constant;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
	public class ReadMapper extends MapReduceBase implements
			Mapper<Text, LongWritable, Text, Text>, Constant {

		private byte[] buffer;
		private int bufferSize;
		private long totalSize;
		private FileSystem fs;
		private Configuration conf;

		public void configure(JobConf configuration) {
			conf = new Configuration(configuration);
		}

		@Override
		public void map(Text key, LongWritable value,
				OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			// TODO Auto-generated method stub

			String name = key.toString();
			fs = FileSystem.get(conf);
			totalSize = 0;
			FSDataInputStream in;
			bufferSize = (int) BUFFERLIMIT;

			buffer = new byte[bufferSize];
			long tsize;
			long ntasks = Long.parseLong(conf.get("dfs.nTasks"));
			for (int task = 0; task < ntasks; task++) {
				in = fs.open(new Path(INPUT, name + task));
				long startTime = System.currentTimeMillis();
				try {
					for (tsize = bufferSize; tsize == bufferSize;) {
						tsize = in.read(buffer, 0, bufferSize);
						totalSize += tsize;
					}
				} finally {
					in.close();
				}

				long endTime = System.currentTimeMillis();
				long execTime = endTime - startTime;
				float ioRate = (float) (totalSize * 1000.0 / (execTime * MEGA));
				output.collect(
						new Text("1"),
						new Text(String.valueOf(ioRate)));
			}
		}

	}
