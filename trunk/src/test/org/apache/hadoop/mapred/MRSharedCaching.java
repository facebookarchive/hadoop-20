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

package org.apache.hadoop.mapred;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.io.MD5Hash;
import java.net.URI;

public class MRSharedCaching {
  static String testStr = null;
  static String TEST_ROOT_DIR;
  static Path concatPath;

  static {
    TEST_ROOT_DIR = new
      Path(System.getProperty("test.build.data","/tmp"))
      .toString().replace(' ', '+');

    concatPath = new Path(TEST_ROOT_DIR, "sharedTest.txt");
  }

  /**
   * Using the wordcount example and adding caching to it. The cache
   * archives/files are set and then are checked in the map if they have been
   * localized or not.
   */
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    JobConf conf;

    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();

    public void configure(JobConf jconf) {
      conf = jconf;
      try {
        Path[] localArchives =
          DistributedCache.getLocalSharedCacheArchives(conf);
        Path[] localFiles = DistributedCache.getLocalSharedCacheFiles(conf);
        // read the cached files (unzipped, unjarred and text)
        // and put it into a single file TEST_ROOT_DIR/test.txt
        String TEST_ROOT_DIR = jconf.get("test.build.data","/tmp");
        Path file = new Path("file:///", TEST_ROOT_DIR);
        FileSystem fs = FileSystem.getLocal(conf);
        if (!fs.mkdirs(file)) {
          throw new IOException("Mkdirs failed to create " + file.toString());
        }
        Path fileOut = new Path(file, "sharedTest.txt");
        fs.delete(fileOut, true);
        DataOutputStream out = fs.create(fileOut);

        if (localArchives != null) {
          for (int i = 0; i < localArchives.length; i++) {
            // read out the files from these archives
            File f = new File(localArchives[i].toString());
            File txt = new File(f, "sharedTest.txt");
            FileInputStream fin = new FileInputStream(txt);
            DataInputStream din = new DataInputStream(fin);
            String str = din.readLine();
            din.close();
            out.writeBytes(str);
            out.writeBytes("\n");
          }
        }

        if (localFiles != null) {
          for (int i = 0; i < localFiles.length; i++) {
            // read out the files from these archives
            File txt = new File(localFiles[i].toString());
            FileInputStream fin = new FileInputStream(txt);
            DataInputStream din = new DataInputStream(fin);
            String str = din.readLine();
            out.writeBytes(str);
            out.writeBytes("\n");
          }
        }
        out.close();
      } catch (IOException ie) {
        // If file could not be opened the check at the end of 
        // launchMRCache* will catch the error
        System.out.println(StringUtils.stringifyException(ie));
      }
    }

    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, one);
      }

    }
  }

  /**
   * Using the wordcount example and adding caching to it. The cache
   * archives/files are set and then are checked in the map if they have been
   * symlinked or not.
   */
  public static class MapClass2 extends MapClass {
    
    JobConf conf;

    public void configure(JobConf jconf) {
      conf = jconf;
      try {
        // read the cached files (unzipped, unjarred and text)
        // and put it into a single file TEST_ROOT_DIR/test.txt
        String TEST_ROOT_DIR = jconf.get("test.build.data","/tmp");
        Path file = new Path("file:///", TEST_ROOT_DIR);
        FileSystem fs = FileSystem.getLocal(conf);
        if (!fs.mkdirs(file)) {
          throw new IOException("Mkdirs failed to create " + file.toString());
        }
        Path fileOut = new Path(file, "sharedTest.txt");
        fs.delete(fileOut, true);
        DataOutputStream out = fs.create(fileOut); 
        String[] symlinks = new String[2];
        symlinks[0] = ".";
        symlinks[1] = "sharedTest.zip";

        for (int i = 0; i < symlinks.length; i++) {
          // read out the files from these archives
          File f = new File(symlinks[i]);
          File txt = new File(f, "sharedTest.txt");
          FileInputStream fin = new FileInputStream(txt);
          BufferedReader reader = new BufferedReader(new InputStreamReader(fin));
          String str = reader.readLine();
          reader.close();
          out.writeBytes(str);
          out.writeBytes("\n");
        }
        out.close();
      } catch (IOException ie) {
        // If file could not be opened the check at the end of 
        // launchMRCache* will catch the error
        System.out.println(StringUtils.stringifyException(ie));
      }
    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class ReduceClass extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static class TestResult {
    public RunningJob job;
    public boolean isOutputOk;
    TestResult(RunningJob job, boolean isOutputOk) {
      this.job = job;
      this.isOutputOk = isOutputOk;
    }
  }

   // Boilerplate code
  public static FileSystem setupJob(String indir,
                              String outdir, String cacheDir,
                              JobConf conf, String input)
  throws IOException {
    return setupJob(indir, outdir, cacheDir, conf, input, false);
  }

  // Boilerplate code
  public static FileSystem setupJob(String indir,
                              String outdir, String cacheDir,
                              JobConf conf, String input,
                              boolean withSymlink)
  throws IOException {
    final Path inDir = new Path(indir);
    final Path outDir = new Path(outdir);
    FileSystem fs = FileSystem.get(conf);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
    conf.setJobName("sharedcachetest");

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setCombinerClass(MRSharedCaching.ReduceClass.class);
    conf.setReducerClass(MRSharedCaching.ReduceClass.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setSpeculativeExecution(false);

    if (!withSymlink) {
      conf.setMapperClass(MRSharedCaching.MapClass.class);
    } else {
      conf.setMapperClass(MRSharedCaching.MapClass2.class);
    }
    
    // Turn on sharing
    conf.set("mapred.cache.shared.enabled", "true");

    return fs;
  }

  private static String getCARDir() {
    return "CAR/";
  }

  private static String getSharedFilesDir() {
    return getCARDir() + "files/";
  }

  private static String getSharedArchivesDir() {
    return getCARDir() + "archives/";
  }

  /**
   * Loads a file and an archive. This test checks the basic functionality of
   * the DistribtedCache */
  public static TestResult launchMRCache(String indir,
                                         String outdir, String cacheDir, 
                                         JobConf conf, String input,
                                         boolean withSymlink)
    throws IOException {

    conf.set("test.build.data", TEST_ROOT_DIR);

    FileSystem fs = setupJob(indir, outdir, cacheDir, conf, input,
        withSymlink);
      
    URI localFS = URI.create("file:///");
    Path cachePath = new Path(localFS.toString(),
        System.getProperty("test.cache.data"));
    cachePath = new Path(cachePath, "sharedTest1");

    Path txtPath = new Path(cachePath, "sharedTest.txt");
    Path zipPath = new Path(cachePath, "sharedTest.zip");
    conf.set("tmpfiles", txtPath.toUri().toString());
    conf.set("tmparchives", zipPath.toUri().toString());

    String md5Txt = MD5Hash.digest(new
        FileInputStream(txtPath.toUri().getPath())).toString();
    String md5Zip = MD5Hash.digest(new
        FileInputStream(zipPath.toUri().getPath())).toString();

    // Read string from source file
    if (testStr == null) {
      testStr = new BufferedReader
        (new InputStreamReader(FileSystem.getLocal(conf).
                               open(txtPath))).readLine();
    }

    RunningJob job = JobClient.runJob(conf);
    
    // after the job ran check to see if the input from the localized cache
    // match the real string. check if there are 2 instances or not.
    int count = 0;
    Path result = concatPath; 
    {
      BufferedReader file = new BufferedReader
         (new InputStreamReader(FileSystem.getLocal(conf).open(result)));
      String line = file.readLine();
      while (line != null) {
        if (!testStr.equals(line)) {
          return new TestResult(job, false);
        }
        count++;
        line = file.readLine();

      }
      file.close();
    }
    if (count != 2) {
      return new TestResult(job, false);
    }

    // Also check that the files were loaded correctly into hdfs
    Path basePath = fs.makeQualified(new Path(conf.get("mapred.system.dir")));
    if (!fs.exists(new Path(basePath,
            getSharedFilesDir() + md5Txt + "_sharedTest.txt"))) {
      return new TestResult(job, false);
    }

    if (!fs.exists(new Path(basePath,
            getSharedArchivesDir() + md5Zip + "_sharedTest.zip"))) {
      return new TestResult(job, false);
    }

    return new TestResult(job, true);
  }

  /**
   * Loads 2 different files with the same filename. This test checks that
   * when there are two different files with the same filename,
   * DistributedCache still works well */
  public static TestResult launchMRCache2(String indir,
                                          String outdir, String cacheDir,
                                          JobConf conf, String input)
  throws IOException {

    conf.set("test.build.data", TEST_ROOT_DIR);

    FileSystem fs = setupJob(indir, outdir, cacheDir, conf, input);

    URI localFS = URI.create("file:///");
    Path cachePath = new Path(localFS.toString(),
        System.getProperty("test.cache.data"));
    cachePath = new Path(cachePath, "sharedTest2");

    Path txtPath = new Path(cachePath, "sharedTest.txt");
    conf.set("tmpfiles", txtPath.toUri().toString());

    String md5 = MD5Hash.digest(new
        FileInputStream(txtPath.toUri().getPath())).toString();

    RunningJob job = JobClient.runJob(conf);

    // In this second test, we want to make sure we are not reading the
    // sharedTest.txt file from test one
    Path result = concatPath;
    {
      BufferedReader file = new BufferedReader
         (new InputStreamReader(FileSystem.getLocal(conf).open(result)));
      String line = file.readLine();
      while (line != null) {
        // If the strings are equal, that means we are accessing the wrong
        // sharedTest.txt
        if (testStr.equals(line)) {
          return new TestResult(job, false);
        }
        line = file.readLine();
      }
      file.close();
    }

    // Also check that the file was loaded correctly into hdfs
    Path basePath = fs.makeQualified(new Path(conf.get("mapred.system.dir")));
    if (!fs.exists(new Path(basePath,
            getSharedFilesDir() + md5 + "_sharedTest.txt"))) {
      return new TestResult(job, false);
    }

    return new TestResult(job, true);
  }

  /**
   * Loads 2 files with the same content, but different filenames. This test
   * checks that when there are two identical files with different filenames,
   * DistributedCache still works well */
  public static TestResult launchMRCache3(String indir,
                                          String outdir, String cacheDir,
                                          JobConf conf, String input)
  throws IOException {

    conf.set("test.build.data", TEST_ROOT_DIR);

    FileSystem fs = setupJob(indir, outdir, cacheDir, conf, input);

    URI localFS = URI.create("file:///");
    Path cachePath = new Path(localFS.toString(),
        System.getProperty("test.cache.data"));
    cachePath = new Path(cachePath, "sharedTest1");

    String path1 = new Path(cachePath, "sharedTest.txt").toUri().toString();
    String path2 = new Path(cachePath, "sharedTest2.txt").toUri().toString();

    conf.set("tmpfiles", path1 + "," + path2);

    RunningJob job = JobClient.runJob(conf);

    int count = 0;
    Path result = concatPath;
    {
      BufferedReader file = new BufferedReader
         (new InputStreamReader(FileSystem.getLocal(conf).open(result)));
      String line = file.readLine();
      while (line != null) {
        if (!testStr.equals(line)) {
          return new TestResult(job, false);
        }
        line = file.readLine();
        count++;
      }
      file.close();
    }

    if (count != 2) {
      return new TestResult(job, false);
    }

    return new TestResult(job, true);
  }
}
