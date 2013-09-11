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

import java.net.URI;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.FastCopy;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCopyFilesWithHardLink extends CopyFilesBase {
  
  {
    ((Log4JLogger)FastCopy.LOG).getLogger().setLevel(Level.ALL);
  }
  
  @Test
  public void testCopyDfsToDfsWithHardLink() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 3, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        DistributedFileSystem dfs = DFSUtil.convertToDFS(hdfs);
        
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-usefastcopy",
                                         "-p",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                    fs.exists(new Path(namenode+"/logs")));
       
        // assert the hardlinks.
        for (MyFile file : files) {
          String[] hardlinks = dfs.getHardLinkedFiles(new Path("/srcdat/" + file.getName()));
          for (String hardLink : hardlinks) {
            assertEquals("/destdat/" + file.getName(), hardLink);
          }
        }
        
        final int nupdate = NFILES>>2;
        updateFiles(cluster.getFileSystem(), "/srcdat", files, nupdate);
        deldir(hdfs, "/logs");
        
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-usefastcopy",
                                         "-prbugp", // not to avoid preserving mod. times
                                         "-overwrite",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        
        // assert the hardlinks.
        for (MyFile file : files) {
          String[] hardlinks = dfs.getHardLinkedFiles(new Path("/srcdat/" + file.getName()));
          for (String hardLink : hardlinks) {
            assertEquals("/destdat/" + file.getName(), hardLink);
          }
        }
        
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/logs");
        // test the replication
        assertEquals(3, hdfs.getDefaultReplication());
        for (MyFile file : files) {
          hdfs.setReplication(new Path("/srcdat/" + file.getName()), (short) 2);
        }
        
        // copy without -p
        ToolRunner.run(new DistCp(conf), new String[] {
                              "-usefastcopy",
                              "-overwrite",
                              "-log",
                              namenode+"/logs",
                              namenode+"/srcdat",
                              namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));

        // assert the hardlinks.
        for (MyFile file : files) {
          String[] hardlinks = dfs.getHardLinkedFiles(new Path("/srcdat/" + file.getName()));
          for (String hardLink : hardlinks) {
            assertEquals("/destdat/" + file.getName(), hardLink);
            assertEquals(hardLink, 3, hdfs.getFileStatus(new Path("/destdat/" + file.getName())).getReplication());
          }
        }

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

}
