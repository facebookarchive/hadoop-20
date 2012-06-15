package org.apache.hadoop.mapred;
import org.apache.hadoop.hdfs.Constant;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
	public class WriteMapper extends MapReduceBase implements
			Mapper<Text, LongWritable, Text, Text>, Constant {

		private byte[] buffer;
		private int bufferSize;
		private long totalSize; // in mb
		public FileSystem fs;
		private Configuration conf;

		public void configure(JobConf configuration) {
			conf = new Configuration(configuration);
		}

		@Override
		public void map(Text key, LongWritable value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// for testing
			// TODO Auto-generated method stub
			// TODO Auto-generated method stub
			int percent = 10;
			bufferSize = (int) BUFFERLIMIT;
			buffer = new byte[bufferSize];

			Random rand = new Random(255);
			String taskID = conf.get("mapred.task.id");
			String name = key.toString() + taskID;

			Path pathSequence = new Path(DFS_INPUT, name);

			fs = FileSystem.get(conf);
			totalSize = value.get();
			fs.delete(new Path(DFS_INPUT, key.toString()), true);

			SequenceFile.Writer write = null;
			try {
				write = SequenceFile.createWriter(fs, conf, pathSequence,
						Text.class, LongWritable.class, CompressionType.NONE);
				write.append(new Text(name), new LongWritable(totalSize));
			} finally {
				if (write != null)
					write.close();
				write = null;
			}

			totalSize *= MEGA;
			long ntasks = Long.parseLong(conf.get("dfs.nTasks"));
			for (int task = 0; task < ntasks; task++) {
				Path pathInput = new Path(INPUT, name + task);
				OutputStream out = fs.create(pathInput, true, bufferSize
						+ percent / 100 * bufferSize);
				rand.nextBytes(buffer);
				long startTime = System.currentTimeMillis();
				try {
					long remain;
					long per = 100;
					for (remain = totalSize; remain > 0; remain -= bufferSize) {
						int temp = (remain > bufferSize) ? bufferSize
								: (int) remain;
						out.write(buffer, 0, temp);
						long t = remain * 100 / totalSize;
						if (t < per) {
							per = temp;
							reporter.setStatus(String.valueOf(per));
						}
					}
				} finally {
					out.close();
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
