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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.Hoopla;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;

public class BenchmarkHar extends Configured implements Tool {

  static final Log LOG = LogFactory.getLog(BenchmarkHar.class);
  protected static final Random RANDOM = new Random();

  private int numDirs = 100;
  private int numFilesInDir = 10;
  private int fileSize = 100000;

  private MiniDFSCluster cluster;
  private FileSystem fs;
  private Path localDir;
  private long startTime;

  private void resetMeasurements() {
    startTime = System.currentTimeMillis();
  }

  private void printMeasurements() {
    LOG.error(" time: " +
                       ((System.currentTimeMillis() - startTime)/1000.));
  }

  
  void createLocalFiles() throws IOException {
    LOG.error("Creating files");
    resetMeasurements();
    localDir = new Path(CopyFilesBase.TEST_ROOT_DIR, "test_files");
    FileSystem localFs = FileSystem.getLocal(getConf());
    for (int i = 0; i < numDirs; ++i) {
      Path dirPath = new Path(localDir, Integer.toString(i));
      for (int j = 0; j < numFilesInDir; ++j) {
        Path filePath = new Path(dirPath, Integer.toString(j));
        byte[] toWrite = new byte[fileSize];
        RANDOM.nextBytes(toWrite);
        CopyFilesBase.createFileWithContent(localFs, filePath, toWrite);
      }
    }
    printMeasurements();
  }
  
  void uploadViaMapReduce() throws Exception {
    Path remotePath = new Path(fs.getHomeDirectory(), "test_files");
    {
      LOG.error("Uploading remote file");
      resetMeasurements();
      String[] args = {
          "-copyFromLocal",
          localDir.toString(),
          remotePath.toString()
      };
      int ret = ToolRunner.run(new FsShell(getConf()), args);
      assert ret == 0;
      printMeasurements();
    }
    
    Path archivePath = new Path(fs.getHomeDirectory(), "foo.har"); 
    {
      LOG.error("Creating har archive");
      String[] args = {
          "-archiveName",
          "foo.har",
          "-p",
          fs.getHomeDirectory().toString(),
          "test_files",
          fs.getHomeDirectory().toString()
      };
      int ret = ToolRunner.run(new HadoopArchives(getConf()), args);
      assert ret == 0;
      printMeasurements();
    }
    fs.delete(remotePath, true);
    fs.delete(archivePath, true);
  }
  
  void uploadAsHar() throws Exception {
    LOG.error("Uploading as har");
    resetMeasurements();
    Path archivePath = new Path(fs.getHomeDirectory(), "foo.har"); 
    String[] args = {
        "-copyFromLocal",
        localDir.toString(),
        archivePath.toString()
    };
    int ret = ToolRunner.run(new HadoopArchives(getConf()), args);
    printMeasurements();
    assert ret == 0;
    fs.delete(archivePath, true);
  }
  
  
  public static void printUsage() {
    System.out.println("USAGE: bin/hadoop hadoop-*test.jar BenchmarkHar" + 
        "[-numdirs] [-numfilesindir] [-filesize]");
    System.exit(0);
  }

  
  @Override
  public int run(String[] args) throws Exception {
    // silence the minidfs cluster
    Log hadoopLog = LogFactory.getLog("org");
    if (hadoopLog instanceof Log4JLogger) {
      ((Log4JLogger) hadoopLog).getLogger().setLevel(Level.ERROR);
    }
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-numdirs")) {
        ++i;
        numDirs = Integer.parseInt(args[i]); 
      } else if (args[i].equals("-numfilesindir")) {
        ++i;
        numFilesInDir = Integer.parseInt(args[i]);
      } else if (args[i].equals("-filesize")) {
        ++i;
        fileSize = Integer.parseInt(args[i]);
      } else {
        printUsage();
      }
    }
    LOG.error("Starting test with " + 
        " numDirs " + numDirs +
        " numFilesInDir " + numFilesInDir +
        " fileSize " + fileSize
     );
    
    Configuration conf = new Configuration();
    conf.setInt("dfs.datanode.scan.period.hours", -1); // disable periodic scanner

    setConf(conf);
    cluster = new MiniDFSCluster(conf, 5, true, null);
    fs = cluster.getFileSystem();
    
    createLocalFiles();
    
    for (int i = 0; i < 5; ++i) {
      LOG.error("Iteration " + i);
      uploadAsHar();
      uploadViaMapReduce();
    }
    
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BenchmarkHar(), args);
    System.exit(res);
  }

  
}
