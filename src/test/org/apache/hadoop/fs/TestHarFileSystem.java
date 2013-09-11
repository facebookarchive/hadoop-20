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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * test the har file system
 * create a har filesystem
 * run fs commands
 * and then run a map reduce job
 */
public class TestHarFileSystem {
  static private Path inputPath, inputrelPath;
  static private MiniDFSCluster dfscluster;
  static private MiniMRCluster mapred;
  static private FileSystem fs;
  static private Path filea, fileb, filec, filed;
  static private Path archivePath;
  
  @BeforeClass
  static public void setUp() throws Exception {
    dfscluster = new MiniDFSCluster(new Configuration(), 2, true, null);
    fs = dfscluster.getFileSystem();
    mapred = new MiniMRCluster(2, fs.getUri().toString(), 1);
    inputPath = new Path(fs.getHomeDirectory(), "test"); 
    inputrelPath = new Path(fs.getHomeDirectory().toUri().
        getPath().substring(1), "test");
    filea = new Path(inputPath, "a");
    fileb = new Path(inputPath, "b");
    filec = new Path(inputPath, "c c");
    filed = new Path(inputPath, "d%d");
    // check for har containing escape worthy 
    // characters in there names
    archivePath = new Path(fs.getHomeDirectory(), "tmp");
    fs.mkdirs(inputPath);
    CopyFilesBase.createFileWithContent(fs, filea, "a".getBytes());
    CopyFilesBase.createFileWithContent(fs, fileb, "b".getBytes());
    CopyFilesBase.createFileWithContent(fs, filec, "c".getBytes());
    CopyFilesBase.createFileWithContent(fs, filed, "d".getBytes());
  }
  
  @AfterClass
  static public void tearDown() throws Exception {
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
  }
  
  static class TextMapperReducer implements Mapper<LongWritable, Text, Text, Text>, 
            Reducer<Text, Text, Text, Text> {
    
    public void configure(JobConf conf) {
      //do nothing 
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      output.collect(value, new Text(""));
    }

    public void close() throws IOException {
      // do nothing
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      while(values.hasNext()) { 
        values.next();
        output.collect(key, null);
      }
    }
  }

  /* check bytes in the har output files */
  private void checkBytes(Path harPath, Configuration conf) throws IOException {
    Path harFilea = new Path(harPath, "a");
    Path harFileb = new Path(harPath, "b");
    Path harFilec = new Path(harPath, "c c");
    Path harFiled = new Path(harPath, "d%d");
    FileSystem harFs = harFilea.getFileSystem(conf);
    assertTrue(CopyFilesBase.checkContentOfFile(harFs, harFilea, "a".getBytes()));
    assertTrue(CopyFilesBase.checkContentOfFile(harFs, harFileb, "b".getBytes()));
    assertTrue(CopyFilesBase.checkContentOfFile(harFs, harFilec, "c".getBytes()));
    assertTrue(CopyFilesBase.checkContentOfFile(harFs, harFiled, "d".getBytes()));
  }

  /**
   * check if the block size of the part files is what we had specified
   */
  private void checkBlockSize(FileSystem fs, Path finalPath, long blockSize) throws IOException {
    FileStatus[] statuses = fs.globStatus(new Path(finalPath, "part-*"));
    for (FileStatus status: statuses) {
      assertTrue(status.getBlockSize() == blockSize);
    }
  }
 
  // test archives with a -p option
  @Test
  public void testRelativeArchives() throws Exception {
    fs.delete(archivePath, true);
    Configuration conf = mapred.createJobConf();
    HadoopArchives har = new HadoopArchives(conf);

    {
      String[] args = new String[6];
      args[0] = "-archiveName";
      args[1] = "foo1.har";
      args[2] = "-p";
      args[3] = fs.getHomeDirectory().toString();
      args[4] = "test";
      args[5] = archivePath.toString();
      int ret = ToolRunner.run(har, args);
      assertTrue("failed test", ret == 0);
      Path finalPath = new Path(archivePath, "foo1.har");
      Path fsPath = new Path(inputPath.toUri().getPath());
      Path filePath = new Path(finalPath, "test");
      // make it a har path
      Path harPath = new Path("har://" + filePath.toUri().getPath());
      assertTrue(fs.exists(new Path(finalPath, "_index")));
      assertTrue(fs.exists(new Path(finalPath, "_masterindex")));
      /*check for existence of only 1 part file, since part file size == 2GB */
      assertTrue(fs.exists(new Path(finalPath, "part-0")));
      assertTrue(!fs.exists(new Path(finalPath, "part-1")));
      assertTrue(!fs.exists(new Path(finalPath, "part-2")));
      assertTrue(!fs.exists(new Path(finalPath, "_logs")));
      FileStatus[] statuses = fs.listStatus(finalPath);
      args = new String[2];
      args[0] = "-ls";
      args[1] = harPath.toString();
      FsShell shell = new FsShell(conf);
      ret = ToolRunner.run(shell, args);
      // fileb and filec
      assertTrue(ret == 0);
      checkBytes(harPath, conf);
      /* check block size for path files */
      checkBlockSize(fs, finalPath, 512 * 1024 * 1024l);
    }
    
    /** now try with different block size and part file size **/
    {
      String[] args = new String[6];
      args[0] = "-archiveName";
      args[1] = "foo.har";
      args[2] = "-p";
      args[3] = fs.getHomeDirectory().toString();
      args[4] = "test";
      args[5] = archivePath.toString();
      conf.setInt("har.block.size", 512);
      conf.setInt("har.partfile.size", 1);
      int ret = ToolRunner.run(har, args);

      assertTrue("failed test", ret == 0);
      Path finalPath = new Path(archivePath, "foo.har");
      Path fsPath = new Path(inputPath.toUri().getPath());
      Path filePath = new Path(finalPath, "test");
      // make it a har path
      Path harPath = new Path("har://" + filePath.toUri().getPath());
      assertTrue(fs.exists(new Path(finalPath, "_index")));
      assertTrue(fs.exists(new Path(finalPath, "_masterindex")));
      /*check for existence of 3 part files, since part file size == 1 */
      assertTrue(fs.exists(new Path(finalPath, "part-0")));
      assertTrue(fs.exists(new Path(finalPath, "part-1")));
      assertTrue(fs.exists(new Path(finalPath, "part-2")));
      assertTrue(!fs.exists(new Path(finalPath, "_logs")));
      FileStatus[] statuses = fs.listStatus(finalPath);
      args = new String[2];
      args[0] = "-ls";
      args[1] = harPath.toString();
      FsShell shell = new FsShell(conf);
      ret = ToolRunner.run(shell, args);
      // fileb and filec
      assertTrue(ret == 0);
      checkBytes(harPath, conf);
      checkBlockSize(fs, finalPath, 512);
    }
  }

  @Test
  public void testArchivesWithMapred() throws Exception {
    //one minor check
    // check to see if fs.har.impl.disable.cache is set 
    Configuration conf = mapred.createJobConf();
    
    boolean archivecaching = conf.getBoolean("fs.har.impl.disable.cache", false);
    assertTrue(archivecaching);
    fs.delete(archivePath, true);
    HadoopArchives har = new HadoopArchives(conf);
    String[] args = new String[4];
 
    //check for destination not specfied
    args[0] = "-archiveName";
    args[1] = "foo.har";
    args[2] = "-p";
    args[3] = "/";
    int ret = ToolRunner.run(har, args);
    assertTrue(ret != 0);
    args = new String[6];
    //check for wrong archiveName
    args[0] = "-archiveName";
    args[1] = "/d/foo.har";
    args[2] = "-p";
    args[3] = "/";
    args[4] = inputrelPath.toString();
    args[5] = archivePath.toString();
    ret = ToolRunner.run(har, args);
    assertTrue(ret != 0);
//  se if dest is a file 
    args[1] = "foo.har";
    args[5] = filec.toString();
    ret = ToolRunner.run(har, args);
    assertTrue(ret != 0);
    //this is a valid run
    args[0] = "-archiveName";
    args[1] = "foo.har";
    args[2] = "-p";
    args[3] = "/";
    args[4] = inputrelPath.toString();
    args[5] = archivePath.toString();
    ret = ToolRunner.run(har, args);
    //check for the existence of the archive
    assertTrue(ret == 0);

    ///try running it again. it should not 
    // override the directory
    ret = ToolRunner.run(har, args);
    assertTrue(ret != 0);
    Path finalPath = new Path(archivePath, "foo.har");
    Path fsPath = new Path(inputPath.toUri().getPath());
    String relative = fsPath.toString().substring(1);
    Path filePath = new Path(finalPath, relative);
    //make it a har path 
    URI uri = fs.getUri();
    Path harPath = new Path("har://" + "hdfs-" + uri.getHost() +":" +
        uri.getPort() + filePath.toUri().getPath());
    assertTrue(fs.exists(new Path(finalPath, "_index")));
    assertTrue(fs.exists(new Path(finalPath, "_masterindex")));
    assertTrue(!fs.exists(new Path(finalPath, "_logs")));
    //creation tested

    //check if the archive is same
    // do ls and cat on all the files
    FsShell shell = new FsShell(conf);
    args = new String[2];
    args[0] = "-ls";
    args[1] = harPath.toString();
    ret = ToolRunner.run(shell, args);
    // ls should work.
    assertTrue((ret == 0));
    
    //now check for contents of all files
    checkBytes(harPath, conf);

    // now check running a map reduce job
    Path outdir = new Path(fs.getHomeDirectory(), "mapout"); 
    JobConf jobconf = mapred.createJobConf();
    FileInputFormat.addInputPath(jobconf, harPath);
    jobconf.setInputFormat(TextInputFormat.class);
    jobconf.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(jobconf, outdir);
    jobconf.setMapperClass(TextMapperReducer.class);
    jobconf.setMapOutputKeyClass(Text.class);
    jobconf.setMapOutputValueClass(Text.class);
    jobconf.setReducerClass(TextMapperReducer.class);
    jobconf.setNumReduceTasks(1);
    JobClient.runJob(jobconf);
    args[1] = outdir.toString();
    ret = ToolRunner.run(shell, args);
    
    // check the result of map reduce job
    FileStatus[] status = fs.globStatus(new Path(outdir, "part*"));
    Path reduceFile = status[0].getPath();
    CopyFilesBase.checkContentOfFile(fs, reduceFile, "a\nb\nc\nd\n".getBytes());
  }

  @Test
  public void testSpaces() throws Exception {
     fs.delete(archivePath, true);
     Configuration conf = mapred.createJobConf();
     HadoopArchives har = new HadoopArchives(conf);
     String[] args = new String[6];
     args[0] = "-archiveName";
     args[1] = "foo bar.har";
     args[2] = "-p";
     args[3] = fs.getHomeDirectory().toString();
     args[4] = "test";
     args[5] = archivePath.toString();
     int ret = ToolRunner.run(har, args);
     assertTrue("failed test", ret == 0);
     Path finalPath = new Path(archivePath, "foo bar.har");
     Path fsPath = new Path(inputPath.toUri().getPath());
     Path filePath = new Path(finalPath, "test");
     // make it a har path
     Path harPath = new Path("har://" + filePath.toUri().getPath());
     FileSystem harFs = harPath.getFileSystem(conf);
     FileStatus[] statuses = harFs.listStatus(finalPath);
  }

  /**
   * Test how block location offsets and lengths are fixed.
   */
  @Test
  public void testFixBlockLocations() {
    // do some tests where start == 0
    {
      // case 1: range starts before current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 20, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 10);
    }
    {
      // case 2: range starts in current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 20, 15);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 3: range starts before current har block and ends in
      // current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 10, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 4: range starts and ends in current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 6, 12);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 6);
    }

    // now try a range where start == 3
    {
      // case 5: range starts before current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 20, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 10);
    }
    {
      // case 6: range starts in current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 20, 15);
      assertEquals(b[0].getOffset(), 3);
      assertEquals(b[0].getLength(), 2);
    }
    {
      // case 7: range starts before current har block and ends in
      // current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 7, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 8: range starts and ends in current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 3, 12);
      assertEquals(b[0].getOffset(), 3);
      assertEquals(b[0].getLength(), 3);
    }

    // test case from JIRA MAPREDUCE-1752
    {
      BlockLocation[] b = { new BlockLocation(null, null, 512, 512),
                            new BlockLocation(null, null, 1024, 512) };
      HarFileSystem.fixBlockLocations(b, 0, 512, 896);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 128);
      assertEquals(b[1].getOffset(), 128);
      assertEquals(b[1].getLength(), 384);
    }
  }
  
  @Test
  public void testLsOnTopArchiveDirectory() throws Exception {
    fs.delete(archivePath, true);
    Configuration conf = mapred.createJobConf();
    HadoopArchives har = new HadoopArchives(conf);
    String[] args = {
        "-archiveName",  
        "foobar.har",
        "-p",
        inputPath.toString(),
        filea.getName(),
        fileb.getName(),
        filec.getName(),
        filed.getName(),
        archivePath.toString()
    };
    int ret = ToolRunner.run(har, args);
    assertTrue("failed test", ret == 0);
    Path finalPath = new Path(archivePath, "foobar.har");
    // make it a har path
    Path harPath = new Path("har://" + finalPath.toUri().getPath());
    FileSystem harFs = harPath.getFileSystem(conf);
    FileStatus[] statuses = harFs.listStatus(finalPath);
    
    String[] fileNames = new String[statuses.length];
    for (int i = 0; i < statuses.length; ++i) {
       fileNames[i] = statuses[i].getPath().getName();
    }
    Arrays.sort(fileNames); // sort array to be sure about order
    String[] expected = {"a", "b", "c c", "d%d"};
    assertArrayEquals(expected, fileNames);
  }

  @Test
  public void testAppend() throws Exception {
    fs.delete(archivePath, true);
    Configuration conf = mapred.createJobConf();
    Path finalPath = new Path(archivePath, "foobar.har");
    {
      HadoopArchives har = new HadoopArchives(conf);
      // create archive
      String[] args = {
          "-archiveName",
          finalPath.getName(),
          "-p",
          inputPath.toString(),
          filea.getName(),
          fileb.getName(),
          archivePath.toString()
      };
      int ret = ToolRunner.run(har, args);
      assertTrue("failed test", ret == 0);
    }
    {
      HadoopArchives har = new HadoopArchives(conf);
      // append to archive
      String[] args = {
          "-append",
          finalPath.getName(),
          "-p",
          inputPath.toString(),
          filec.getName(),
          filed.getName(),
          archivePath.toString()
      };
      int ret = ToolRunner.run(har, args);
      assertTrue("failed test", ret == 0);
    }
    {
      // check results
      Path harPath = new Path("har://" + finalPath.toUri().getPath());
      checkBytes(harPath, conf);
      
      // check the items in _index file
      String[] actual = extractItemNamesFromIndex(new Path(finalPath, "_index")).toArray(new String[0]);
      String[] expected = {"/", "/a", "/b", "/c c", "/d%d"};
      assertArrayEquals(actual, expected);
    }
  }
  
  @Test
  public void testAppendFromArchive() throws Exception {
    fs.delete(archivePath, true);
    Configuration conf = mapred.createJobConf();
    Path archive = new Path(archivePath, "foo.har");
    Path archive2 = new Path(archivePath, "foo2.har");
    
    Path dirPath = new Path(inputPath, "dir");
    Path filee = new Path(dirPath, "e");
    fs.mkdirs(dirPath);
    CopyFilesBase.createFileWithContent(fs, filee, "e".getBytes());
    {
      HadoopArchives har = new HadoopArchives(conf);
      // create full archive
      String[] args = {
          "-archiveName",
          archive2.getName(),
          "-p", inputPath.toString(),
          filea.getName(),
          fileb.getName(),
          filec.getName(),
          filed.getName(),
          "dir/e",
          archivePath.toString()
      };
      int ret = ToolRunner.run(har, args);
      assertTrue("failed test", ret == 0);
    }
    {
      HadoopArchives har = new HadoopArchives(conf);
      // create archive without a, b, e
      String[] args = {
          "-archiveName",
          archive.getName(),
          "-p", inputPath.toString(),
          filec.getName(),
          filed.getName(),
          archivePath.toString()
      };
      int ret = ToolRunner.run(har, args);
      assertTrue("failed test", ret == 0);
    }
    {
      HadoopArchives har = new HadoopArchives(conf);
      // append to archive 
      String[] args = {
          "-appendFromArchive",
          archive2.toString(),
          "/a",
          "/b",
          "/dir/e",
          archive.toString()
      };
      int ret = ToolRunner.run(har, args);
      assertTrue("failed test", ret == 0);
    }
    {
      // check results
      Path harPath = new Path("har://" + archive.toUri().getPath());
      checkBytes(harPath, conf);
      
      // check the items in _index file
      String[] actual = extractItemNamesFromIndex(new Path(archive, "_index")).toArray(new String[0]);
      String[] expected = {"/", "/a", "/b", "/c c", "/d%d", "/dir", "/dir/e"};
      assertArrayEquals(actual, expected);
    }
  }
  
  private List<String> extractItemNamesFromIndex(Path pathToIndex) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(pathToIndex);
    FSDataInputStream inputStream = fs.open(pathToIndex);
    LineReader lineReader = new LineReader(inputStream, new Configuration());
    long totalRead = 0;
    List<String> result = new ArrayList<String>();
    while (totalRead < fileStatus.getLen()) {
      Text line = new Text();
      totalRead += lineReader.readLine(line);
      HarStatus harStauts = new HarStatus(line.toString());
      result.add(harStauts.getName());
    }
    return result;
  }
  
}
