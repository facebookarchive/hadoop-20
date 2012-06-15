package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Random;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;


import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.ZombieCluster;
import org.apache.hadoop.tools.rumen.ZombieJob;
import org.apache.hadoop.tools.rumen.ZombieJobProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class SortedZombieJobProducer implements JobStoryProducer {

	private ZombieJobProducer producer;
	private boolean isProducerEmpty = false;
	
  private final PriorityQueue<ZombieJob> jobBuffer =
  	new PriorityQueue<ZombieJob>(1,
      new Comparator<ZombieJob>() {
        @Override
        public int compare(ZombieJob o1, ZombieJob o2) {
        	if (o1.getSubmissionTime() < o2.getSubmissionTime())
        		return -1;
        	else
        	if (o1.getSubmissionTime() > o2.getSubmissionTime())
        		return 1;
        	return 0;
        }
      });
  
  private int jobBufferSize = 100;
  
  private void initBuffer() throws IOException {
  	for (int i = 0; i < jobBufferSize; ++i) {
  		ZombieJob job = producer.getNextJob();
  		if (job == null) {
  			isProducerEmpty = true;
  			break;
  		}
  		jobBuffer.add(job);
  	}
  }
  
  public SortedZombieJobProducer(Path path, ZombieCluster cluster,
  		Configuration conf, int bufferSize)
      throws IOException {
    producer = new ZombieJobProducer(path, cluster, conf);
    jobBufferSize = bufferSize;
    initBuffer();
  }
  
  public SortedZombieJobProducer(Path path, ZombieCluster cluster,
      Configuration conf, long randomSeed, int bufferSize) throws IOException {
    producer = new ZombieJobProducer(path, cluster, conf);
    jobBufferSize = bufferSize;
    initBuffer();
  }

  @Override
	public ZombieJob getNextJob() throws IOException {
  	ZombieJob job;
  	if (!isProducerEmpty) {
  		job = producer.getNextJob();
  		if (job != null) 
  			jobBuffer.add(job);
  		else
  			isProducerEmpty = true;
  	}
  	job = jobBuffer.poll();
  	return job;
  }

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		producer.close();
	}
}
