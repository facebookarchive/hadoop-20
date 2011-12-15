package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class TestNoSort extends HadoopTestCase {
   public TestNoSort() throws IOException {
     super(CLUSTER_MR, LOCAL_FS, 2, 1);
   }

   private static final Path ROOT_DIR = new Path("testing/nosort");
   private static final Path IN_DIR = new Path(ROOT_DIR, "input");
   private static final Path OUT_DIR = new Path(ROOT_DIR, "output");

   private Path getDir(Path dir) {
     // Hack for local FS that does not have the concept of a 'mounting point'
     if (isLocalFS()) {
       String localPathRoot = System.getProperty("test.build.data", "/tmp")
         .replace(' ', '+');
       dir = new Path(localPathRoot, dir);
     }
     return dir;
   }
   
   private JobConf conf;
   private FileSystem fs;

   @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = createJobConf();
    fs = FileSystem.get(conf);
  }
   
  @Override
  protected void tearDown() throws Exception {
    fs.delete(getDir(ROOT_DIR), true);
    super.tearDown();
  }
  
  void prepareInput(int size, FileSystem fs) throws IOException {
    FSDataOutputStream out = fs.create(getDir(new Path(IN_DIR, "part-00000")));
    for (int i=0;i<size;i++) {
      out.write(String.format("%09d\t%09d\n", i/4, i).getBytes());
    }
    out.close();
    out = fs.create(getDir(new Path(IN_DIR, "part-00000")));
    for (int i=0;i<size;i++) {
      out.write(String.format("%09d\t%09d\n", i/4, i).getBytes());
    }
    out.close();
  }

  public void testNoSort() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    prepareInput(10000, fs);
    conf.setInt("io.sort.mb", 1);
    conf.setBoolean("mapred.map.output.sort", false);
    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    KeyValueTextInputFormat.setInputPaths(conf, getDir(IN_DIR));
    TextOutputFormat.setOutputPath(conf, getDir(OUT_DIR));
    conf.setNumReduceTasks(1);
    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    JobClient.runJob(conf);
  }

}
