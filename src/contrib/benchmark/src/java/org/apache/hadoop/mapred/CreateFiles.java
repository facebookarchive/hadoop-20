package org.apache.hadoop.mapred;

import org.apache.hadoop.hdfs.Constant;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
public class CreateFiles extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text>, Constant {


	private FileSystem fs = null;
	private Configuration conf;
	private byte[] bufferSize = null;

	public void configure(JobConf configuration) {
		conf = new Configuration(configuration);
	}
	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		// intialize
		fs = FileSystem.get(conf);
		String root = conf.get("dfs.rootdir");
		StringTokenizer token = new StringTokenizer(arg1.toString());
		Path filePath = new Path(root + "/" + token.nextToken());
		long fileSize = (long) (BLOCK_SIZE * Double.parseDouble(token.nextToken()));

		FSDataOutputStream out = fs.create(filePath, true);

		Random rand = new Random();
		bufferSize = new byte[(int) BUFFERLIMIT];
		rand.nextBytes(bufferSize);
		try {
			for (int size = 0; size <= fileSize; size += BUFFERLIMIT) {
				out.write(bufferSize);
				arg3.setStatus(size + " / " + fileSize);
			}
		} finally {
			if (out != null)
				out.close();
			out = null;
		}
	}

}
