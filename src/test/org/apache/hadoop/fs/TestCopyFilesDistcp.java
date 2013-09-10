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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CopyFilesBase.MyFile;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.mortbay.log.Log;

import junit.framework.TestCase;

public class TestCopyFilesDistcp extends TestCase {
  {
    ((Log4JLogger)LogFactory.getLog(TestCopyFilesDistcp.class)
        ).getLogger().setLevel(Level.OFF);
    DataNode.LOG.getLogger().setLevel(Level.OFF);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)DistCp.LOG).getLogger().setLevel(Level.ALL);
  }
  
  enum DistcpType {
    NORMAL,
    FASTCOPY,
    COPYBYCHUNK
  }
  
  public void testSkipUnderConstrunctionFile() throws Exception {
    performDistcpWithUnderConstructionFile(DistcpType.NORMAL);
  }
  
  public void testSkipUnderConstrunctionFileWithFastCopy() throws Exception {
    performDistcpWithUnderConstructionFile(DistcpType.FASTCOPY);
  }
  
  public void testSkipUnderConstrunctionFileWithCopyByChunk() throws Exception {
    performDistcpWithUnderConstructionFile(DistcpType.COPYBYCHUNK);
  }
  
  public void performDistcpWithUnderConstructionFile(DistcpType type) 
      throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = dfs.getFileSystem();
      namenode = fs.getUri().toString();
      mr = new MiniMRCluster(3, namenode, 1);
      MyFile[] files = TestCopyFiles.createFiles(fs.getUri(), "/srcdat");
     
      // create a under construction files
      MyFile f = new MyFile();
      Path p = new Path(new Path("/srcdat"), f.getName());
      FSDataOutputStream out = fs.create(p);
      byte[] toWrite = new byte[f.getSize()];
      new Random(f.getSeed()).nextBytes(toWrite);
      out.write(toWrite);
      out.flush();
      Log.info("Created under construction file: " + p);
      
      Path destRoot = new Path("/destdat");
      Path destPath = new Path(destRoot, f.getName());
      
      String option = "";
      if (type.equals(DistcpType.FASTCOPY)) {
        option = "-usefastcopy";
      } else if (type.equals(DistcpType.COPYBYCHUNK)) {
        option = "-copybychunk";
      }
      
      Configuration job = mr.createJobConf();
      List<String> args = new ArrayList<String>();
      args.add("-m");
      args.add("100");
      args.add("-skipunderconstruction");
      if (!option.equals("")) {
        args.add(option);
      }
      args.add("-log");
      args.add(namenode + "/logs");
      args.add(namenode + "/srcdat");
      args.add(namenode + "/destdat");
      
      ToolRunner.run(new DistCp(job),args.toArray(new String[] {}));
      assertTrue("Source and destination directories do not match.",
                 TestCopyFiles.checkFiles(fs, "/destdat", 
                     files));
      // the under construction file should be skipped.
      assertFalse(fs.exists(destPath));
      
      // close the under construction file.
      out.close();
      
      // copy again
      fs.delete(destRoot, true);
      args.clear();
      args.add("-m");
      args.add("100");
      args.add("-skipunderconstruction");
      if (!option.equals("")) {
        args.add(option);
      }
      args.add("-log");
      args.add(namenode + "/logs2");
      args.add(namenode + "/srcdat");
      args.add(namenode + "/destdat");
      ToolRunner.run(new DistCp(job),args.toArray(new String[] {}));
      
      MyFile[] moreFiles = new MyFile[files.length + 1];
      int i = 0;
      for (i = 0; i < files.length; i++) {
        moreFiles[i] = files[i];
      }
      moreFiles[i] = f;
      assertTrue("Source and destination directories do not match.",
                 TestCopyFiles.checkFiles(fs, "/destdat", moreFiles));
      
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }
}
