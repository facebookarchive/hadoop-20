/**
 * 
 */
package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import junit.framework.TestCase;


public class TestHarWithCombineFileInputFormat extends TestCase {
  private Path hdfsInputPath;
  private Path archiveInputPath;
  private MiniDFSCluster dfscluster;
  private MiniMRCluster mapred;
  private FileSystem fs;
  private Path filea, fileb;
  private Path archiveDir;
  private JobConf conf;
  
  private class DummyInputFormat extends CombineFileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text,Text> getRecordReader(InputSplit split, JobConf job
        , Reporter reporter) throws IOException {
      return null;
    }
  }
  
  protected void setUp() throws Exception {
    super.setUp();
    conf = new JobConf();
    dfscluster = new MiniDFSCluster(conf, 1, true, null);
    fs = dfscluster.getFileSystem();
    mapred = new MiniMRCluster(1, fs.getUri().toString(), 1);
    
    hdfsInputPath = new Path(fs.getHomeDirectory(), "test"); 
    archiveDir = new Path(fs.getHomeDirectory(), "test-archive");
    
    filea = new Path(hdfsInputPath, "a");
    fileb = new Path(hdfsInputPath, "b");
    
    // Create the following directory structure
    // ~/test/a
    // ~/test/b/
    // ~/test-archive/foo.har/a (in HAR)
    // ~/test-archive/foo.har/b (in HAR)
    
    fs.mkdirs(hdfsInputPath);
    FSDataOutputStream out = fs.create(filea); 
    out.write("a".getBytes());
    out.close();
    out = fs.create(fileb);
    out.write("b".getBytes());
    out.close();
    
    HadoopArchives har = new HadoopArchives(conf);

    String archiveName = "foo.har";
    String[] args = new String[5];
    args[0] = "-archiveName";
    args[1] = "foo.har";
    args[2] = "-p";
    args[3] = hdfsInputPath.toString();
    args[4] = archiveDir.toString();
    int ret = ToolRunner.run(har, args);
    assertTrue("Failed to create HAR", ret == 0);

    archiveInputPath = 
      new Path("har://" + archiveDir.toUri().getPath(), archiveName);
  }  
  
  protected void tearDown() throws Exception {
    try {
      if (mapred != null) {
        mapred.shutdown();
      }
      if (dfscluster != null) {
        dfscluster.shutdown();
      }
    } catch(Exception e) {
      System.err.println(e);
    }
    super.tearDown();
  }
  
  @SuppressWarnings("deprecation")
  public void testGetSplits() throws IOException {
    hdfsInputPath.getFileSystem(conf);
    DummyInputFormat inFormat = new DummyInputFormat();
    DummyInputFormat.setInputPaths(conf, hdfsInputPath, archiveInputPath);
    InputSplit[] splits = inFormat.getSplits(conf, 1);

    assertTrue("Number of splits is incorrect", splits.length == 1);
  }
}
