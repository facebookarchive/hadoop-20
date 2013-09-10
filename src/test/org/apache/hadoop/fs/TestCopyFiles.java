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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;


/**
 * A JUnit test for copying files recursively.
 */
public class TestCopyFiles extends CopyFilesBase {

  /** copy files from local file system to local file system */
  @Test
  public void testCopyFromLocalToLocal() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.get(LOCAL_FS, conf);
    MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR+"/srcdat");
    ToolRunner.run(new DistCp(new Configuration()),
                           new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                                         "file:///"+TEST_ROOT_DIR+"/destdat"});
    assertTrue("Source and destination directories do not match.",
               checkFiles(localfs, TEST_ROOT_DIR+"/destdat", files));
    deldir(localfs, TEST_ROOT_DIR+"/destdat");
    deldir(localfs, TEST_ROOT_DIR+"/srcdat");
  }

  /** copy files from dfs file system to dfs file system */ 
  @Test
  public void testCopyByChunkFromDfsToDfs() throws Exception {
    String namenode = null;
    String jobTrackerName = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 2, true, null);
      dfs.waitActive();
      final FileSystem hdfs = dfs.getFileSystem();
      namenode = hdfs.getUri().toString();
      mr = new MiniMRCluster(4, namenode, 3);
      jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      FileSystem.setDefaultUri(conf, namenode);
      conf.set("mapred.job.tracker", jobTrackerName);
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        assertTrue(checkFiles(hdfs, "/srcdat", files));
        ToolRunner.run(new DistCp(conf), new String[] {
          "-m", "5", "-copybychunk",
          namenode+"/srcdat",
          namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
      }

      // test distcp can delete tmp file
      FileSystem.setDefaultUri(conf, "file:///");
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat2");
        assertTrue(checkFiles(hdfs, "/srcdat2", files));
        ToolRunner.run(new DistCp(conf), new String[] {
          "-m", "5", "-copybychunk",
          namenode+"/srcdat2",
          namenode+"/destdat2"});

        // check whether the tmp file for distcp exists (_distcp_tmp_xxxxxx)
        boolean distcpTmpExists = false;
        FileStatus[] fsstatus = hdfs.listStatus(new Path(namenode+"/destdat2"));
        for (int i = 0; i < fsstatus.length; i++) {
          if (fsstatus[i].getPath().toString().indexOf("_distcp_tmp_") >= 0) {
            distcpTmpExists = true;
            break;
          }
        }
        assertFalse("Distcp tmp file should have been deleted.",
            distcpTmpExists);

        deldir(hdfs, "/destdat2");
        deldir(hdfs, "/srcdat2");
      }
    } finally {
      if (dfs != null) { dfs.shutdown(); }
    }
  }

  @Test
  public void testCopyFromDfsToDfs() throws Exception {
    String namenode = null;
    String jobTrackerName = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 2, true, null);
      dfs.waitActive();
      final FileSystem hdfs = dfs.getFileSystem();
      namenode = hdfs.getUri().toString();
      mr = new MiniMRCluster(4, namenode, 3);
      jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      FileSystem.setDefaultUri(conf, namenode);
      conf.set("mapred.job.tracker", jobTrackerName);
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        assertTrue(checkFiles(hdfs, "/srcdat", files));
        ToolRunner.run(new DistCp(conf), new String[] {
          "-log",
          namenode+"/logs",
          namenode+"/srcdat",
          namenode+"/destdat"});

        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
            fs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }

      // test distcp can delete tmp file
      FileSystem.setDefaultUri(conf, "file:///");
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat2");
        assertTrue(checkFiles(hdfs, "/srcdat2", files));
        ToolRunner.run(new DistCp(conf), new String[] {
          "-log",
          namenode+"/logs2",
          namenode+"/srcdat2",
          namenode+"/destdat2"});

        // check whether the tmp file for distcp exists (_distcp_tmp_xxxxxx)
        boolean distcpTmpExists = false;
        FileStatus[] fsstatus = hdfs.listStatus(new Path(namenode+"/destdat2"));
        for (int i = 0; i < fsstatus.length; i++) {
          if (fsstatus[i].getPath().toString().indexOf("_distcp_tmp_") >= 0) {
            distcpTmpExists = true;
            break;
          }
        }
        assertFalse("Distcp tmp file should have been deleted.",
            distcpTmpExists);

        deldir(hdfs, "/destdat2");
        deldir(hdfs, "/srcdat2");
        deldir(hdfs, "/logs2");
      }
    } finally {
      if (dfs != null) { dfs.shutdown(); }
    }
  }

  /** copy files from local file system to dfs file system */
  @Test
  public void testCopyByChunkFromLocalToDfs() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR+"/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-copybychunk",
                                         "-log",
                                         namenode+"/logs",
                                         "file:///"+TEST_ROOT_DIR+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(cluster.getFileSystem(), "/destdat", files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/logs");
        deldir(FileSystem.get(LOCAL_FS, conf), TEST_ROOT_DIR+"/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /** copy files from local file system to dfs file system */
  @Test
  public void testCopyFromLocalToDfs() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR+"/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         namenode+"/logs",
                                         "file:///"+TEST_ROOT_DIR+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(cluster.getFileSystem(), "/destdat", files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/logs");
        deldir(FileSystem.get(LOCAL_FS, conf), TEST_ROOT_DIR+"/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** copy empty directory on dfs file system */
  @Test
  public void testCopyByChunkEmptyDir() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {

        FileSystem fs = FileSystem.get(URI.create(namenode), new Configuration());
        fs.mkdirs(new Path("/empty"));

        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-copybychunk",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/empty",
                                         namenode+"/dest"});
        fs = FileSystem.get(URI.create(namenode+"/destdat"), conf);
        assertTrue("Destination directory does not exist.",
                   fs.exists(new Path(namenode+"/dest")));
        deldir(hdfs, "/dest");
        deldir(hdfs, "/empty");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** copy empty directory on dfs file system */
  @Test
  public void testEmptyDir() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();   
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {

        FileSystem fs = FileSystem.get(URI.create(namenode), new Configuration());
        fs.mkdirs(new Path("/empty"));

        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/empty",
                                         namenode+"/dest"});
        fs = FileSystem.get(URI.create(namenode+"/destdat"), conf);
        assertTrue("Destination directory does not exist.",
                   fs.exists(new Path(namenode+"/dest")));
        deldir(hdfs, "/dest");
        deldir(hdfs, "/empty");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /** copy files from dfs file system to local file system */
  @Test
  public void testCopyFromDfsToLocal() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      final FileSystem localfs = FileSystem.get(LOCAL_FS, conf);
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         "/logs",
                                         namenode+"/srcdat",
                                         "file:///"+TEST_ROOT_DIR+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(localfs, TEST_ROOT_DIR+"/destdat", files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path("/logs")));
        deldir(localfs, TEST_ROOT_DIR+"/destdat");
        deldir(hdfs, "/logs");
        deldir(hdfs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test
  public void testCopyByChunkDfsToDfsOverwrite() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-p",
                                         "-copybychunk",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                    fs.exists(new Path(namenode+"/logs")));

        FileStatus[] dchkpoint = getFileStatus(hdfs, "/destdat", files);
        final int nupdate = NFILES>>2;
        updateFiles(cluster.getFileSystem(), "/srcdat", files, nupdate);
        deldir(hdfs, "/logs");
        
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-prbugp", // no t to avoid preserving mod. times
                                         "-copybychunk",
                                         "-overwrite",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        assertTrue("-overwrite didn't.",
                 checkUpdate(hdfs, dchkpoint, "/destdat", files, NFILES));

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test
  public void testCopyDfsToDfsUpdateOverwrite() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
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

        FileStatus[] dchkpoint = getFileStatus(hdfs, "/destdat", files);
        final int nupdate = NFILES>>2;
        updateFiles(cluster.getFileSystem(), "/srcdat", files, nupdate);
        deldir(hdfs, "/logs");

        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-prbugp", // no t to avoid preserving mod. times
                                         "-update",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        assertTrue("Update failed to replicate all changes in src",
                 checkUpdate(hdfs, dchkpoint, "/destdat", files, nupdate));

        deldir(hdfs, "/logs");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-prbugp", // no t to avoid preserving mod. times
                                         "-overwrite",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        assertTrue("-overwrite didn't.",
                 checkUpdate(hdfs, dchkpoint, "/destdat", files, NFILES));

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
 
  @Test
  public void testCopyDfsToDfsUpdateWithSkipCRC() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      
      FileSystem fs = FileSystem.get(URI.create(namenode), new Configuration());
      // Create two files of the same name, same length but different
      // contents
      final String testfilename = "test";
      final String srcData = "act act act";
      final String destData = "cat cat cat";
      
      if (namenode.startsWith("hdfs://")) {
        deldir(hdfs,"/logs");
        
        Path srcPath = new Path("/srcdat", testfilename);
        Path destPath = new Path("/destdat", testfilename);
        FSDataOutputStream out = fs.create(srcPath, true);
        out.writeUTF(srcData);
        out.close();

        out = fs.create(destPath, true);
        out.writeUTF(destData);
        out.close();
        
        // Run with -skipcrccheck option
        ToolRunner.run(new DistCp(conf), new String[] {
          "-p",
          "-update",
          "-skipcrccheck",
          "-log",
          namenode+"/logs",
          namenode+"/srcdat",
          namenode+"/destdat"});
        
        // File should not be overwritten
        FSDataInputStream in = hdfs.open(destPath);
        String s = in.readUTF();
        System.out.println("Dest had: " + s);
        assertTrue("Dest got over written even with skip crc",
            s.equalsIgnoreCase(destData));
        in.close();
        
        deldir(hdfs, "/logs");

        // Run without the option        
        ToolRunner.run(new DistCp(conf), new String[] {
          "-p",
          "-update",
          "-log",
          namenode+"/logs",
          namenode+"/srcdat",
          namenode+"/destdat"});
        
        // File should be overwritten
        in = hdfs.open(destPath);
        s = in.readUTF();
        System.out.println("Dest had: " + s);

        assertTrue("Dest did not get overwritten without skip crc",
            s.equalsIgnoreCase(srcData));
        in.close();

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
       }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testCopyFromDfsToDfsWithFastCopy() throws Exception {
    String namenode = null;
    String jobTrackerName = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 2, true, null);
      dfs.waitActive();
      final FileSystem hdfs = dfs.getFileSystem();
      namenode = hdfs.getUri().toString();
      mr = new MiniMRCluster(4, namenode, 3);
      jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      FileSystem.setDefaultUri(conf, namenode);
      conf.set("mapred.job.tracker", jobTrackerName);
      conf.setInt(DistCp.BYTES_PER_MAP_LABEL, 1024);
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        assertTrue(checkFiles(hdfs, "/srcdat", files));
        ToolRunner.run(new DistCp(conf), new String[] {
          "-usefastcopy",
          "-log",
          namenode+"/logs",
          namenode+"/srcdat",
          namenode+"/destdat"});

        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
            fs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }

      // test distcp can delete tmp file
      FileSystem.setDefaultUri(conf, "file:///");
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat2");
        assertTrue(checkFiles(hdfs, "/srcdat2", files));
        ToolRunner.run(new DistCp(conf), new String[] {
          "-usefastcopy",
          "-log",
          namenode+"/logs2",
          namenode+"/srcdat2",
          namenode+"/destdat2"});

        // check whether the tmp file for distcp exists (_distcp_tmp_xxxxxx)
        boolean distcpTmpExists = false;
        FileStatus[] fsstatus = hdfs.listStatus(new Path(namenode+"/destdat2"));
        for (int i = 0; i < fsstatus.length; i++) {
          if (fsstatus[i].getPath().toString().indexOf("_distcp_tmp_") >= 0) {
            distcpTmpExists = true;
            break;
          }
        }
        assertFalse("Distcp tmp file should have been deleted.",
            distcpTmpExists);

        deldir(hdfs, "/destdat2");
        deldir(hdfs, "/srcdat2");
        deldir(hdfs, "/logs2");
      }
    } finally {
      if (dfs != null) { dfs.shutdown(); }
    }
  }
  
  @Test
  public void testDistCpUseFastCopy() throws Exception {
    String namenode = null;
    String namenode2 = null;
    MiniDFSCluster dfs = null;
    MiniDFSCluster dfs2 = null;
    try {
      Configuration conf = new Configuration();
      conf.set(MiniDFSCluster.DFS_CLUSTER_ID,
          Long.toString(System.currentTimeMillis() + 1));
      dfs = new MiniDFSCluster(conf, 1, true, null);
      dfs.getNameNode().clusterName = "myCluster1";
      final FileSystem hdfs = dfs.getFileSystem();
      namenode = hdfs.getUri().toString();

      Configuration conf2 = new Configuration(conf);
      conf2.set(MiniDFSCluster.DFS_CLUSTER_ID,
          Long.toString(System.currentTimeMillis() + 2));
      dfs2 = new MiniDFSCluster(conf2, 1, true, null);
      dfs2.getNameNode().clusterName = "myCluster2";
      final FileSystem hdfs2 = dfs2.getFileSystem();
      namenode2 = hdfs2.getUri().toString();

      dfs.waitActive();
      FileSystem.setDefaultUri(conf, namenode);
      

      boolean retValue;
      retValue = DistCp.DistCopier.canUseFastCopy(
          Arrays.asList(new Path[] { new Path("/files") }),
          new Path("/files"), conf);
      assertTrue(retValue);

      retValue = DistCp.DistCopier.canUseFastCopy(
          Arrays.asList(new Path[] { new Path("/files"),
              new Path(namenode2 + "/files2") }), new Path("/files"), conf);
      assertFalse(retValue);

      dfs.getNameNode().clusterName = null;
      retValue = DistCp.DistCopier.canUseFastCopy(
          Arrays.asList(new Path[] { new Path("/files"),
              new Path(namenode2 + "/files2") }), new Path("/files"), conf);
      assertTrue(retValue);
      
      dfs.getNameNode().clusterName = "Cluster1";
      dfs2.getNameNode().clusterName = null;
      retValue = DistCp.DistCopier.canUseFastCopy(
          Arrays.asList(new Path[] { new Path("/files"),
              new Path(namenode2 + "/files2") }), new Path("/files"), conf);
      assertTrue(retValue);      
   } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (dfs2 != null) { dfs2.shutdown(); }
    }
  }
  
  @Test
  public void testCopyDfsToDfsUpdateOverwriteWithFastCopy() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
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

        FileStatus[] dchkpoint = getFileStatus(hdfs, "/destdat", files);
        final int nupdate = NFILES>>2;
        updateFiles(cluster.getFileSystem(), "/srcdat", files, nupdate);
        deldir(hdfs, "/logs");

        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-usefastcopy",
                                         "-prbugp", // not to avoid preserving mod. times
                                         "-update",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        assertTrue("Update failed to replicate all changes in src",
                 checkUpdate(hdfs, dchkpoint, "/destdat", files, nupdate));

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

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testCopyDfsToDfsUpdateWithSkipCRCAndFastCopy() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      
      FileSystem fs = FileSystem.get(URI.create(namenode), new Configuration());
      // Create two files of the same name, same length but different
      // contents
      final String testfilename = "test";
      final String srcData = "act act act";
      final String destData = "cat cat cat";
      
      if (namenode.startsWith("hdfs://")) {
        deldir(hdfs,"/logs");
        
        Path srcPath = new Path("/srcdat", testfilename);
        Path destPath = new Path("/destdat", testfilename);
        FSDataOutputStream out = fs.create(srcPath, true);
        out.writeUTF(srcData);
        out.close();

        out = fs.create(destPath, true);
        out.writeUTF(destData);
        out.close();
        
        // Run with -skipcrccheck option
        ToolRunner.run(new DistCp(conf), new String[] {
          "-usefastcopy",
          "-p",
          "-update",
          "-skipcrccheck",
          "-log",
          namenode+"/logs",
          namenode+"/srcdat",
          namenode+"/destdat"});
        
        // File should not be overwritten
        FSDataInputStream in = hdfs.open(destPath);
        String s = in.readUTF();
        System.out.println("Dest had: " + s);
        assertTrue("Dest got over written even with skip crc",
            s.equalsIgnoreCase(destData));
        in.close();
        
        deldir(hdfs, "/logs");

        // Run without the option        
        ToolRunner.run(new DistCp(conf), new String[] {
          "-usefastcopy",
          "-p",
          "-update",
          "-log",
          namenode+"/logs",
          namenode+"/srcdat",
          namenode+"/destdat"});
        
        // File should be overwritten
        in = hdfs.open(destPath);
        s = in.readUTF();
        System.out.println("Dest had: " + s);

        assertTrue("Dest did not get overwritten without skip crc",
            s.equalsIgnoreCase(srcData));
        in.close();

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
       }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testCopyDuplication() throws Exception {
    final FileSystem localfs = FileSystem.get(LOCAL_FS, new Configuration());
    try {    
      MyFile[] files = createFiles(localfs, TEST_ROOT_DIR+"/srcdat");
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/src2/srcdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(localfs, TEST_ROOT_DIR+"/src2/srcdat", files));
  
      assertEquals(DistCp.DuplicationException.ERROR_CODE,
          ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/src2/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/destdat",}));
    }
    finally {
      deldir(localfs, TEST_ROOT_DIR+"/destdat");
      deldir(localfs, TEST_ROOT_DIR+"/srcdat");
      deldir(localfs, TEST_ROOT_DIR+"/src2");
    }
  }

  @Test
  public void testCopySingleFile() throws Exception {
    FileSystem fs = FileSystem.get(LOCAL_FS, new Configuration());
    Path root = new Path(TEST_ROOT_DIR+"/srcdat");
    try {    
      MyFile[] files = {createFile(root, fs)};
      //copy a dir with a single file
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/destdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(fs, TEST_ROOT_DIR+"/destdat", files));
      
      //copy a single file
      String fname = files[0].getName();
      Path p = new Path(root, fname);
      FileSystem.LOG.info("fname=" + fname + ", exists? " + fs.exists(p));
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat/"+fname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"+fname});
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR+"/dest2", files));     
      //copy single file to existing dir
      deldir(fs, TEST_ROOT_DIR+"/dest2");
      fs.mkdirs(new Path(TEST_ROOT_DIR+"/dest2"));
      MyFile[] files2 = {createFile(root, fs, 0)};
      String sname = files2[0].getName();
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"-update",
                        "file:///"+TEST_ROOT_DIR+"/srcdat/"+sname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"});
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR+"/dest2", files2));     
      updateFiles(fs, TEST_ROOT_DIR+"/srcdat", files2, 1);
      //copy single file to existing dir w/ dst name conflict
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"-update",
                        "file:///"+TEST_ROOT_DIR+"/srcdat/"+sname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"});
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR+"/dest2", files2));     
    }
    finally {
      deldir(fs, TEST_ROOT_DIR+"/destdat");
      deldir(fs, TEST_ROOT_DIR+"/dest2");
      deldir(fs, TEST_ROOT_DIR+"/srcdat");
    }
  }

  /** tests basedir option copying files from dfs file system to dfs file system */
  @Test
  public void testCopyByChunkBasedir() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/basedir/middle/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-copybychunk",
                                         "-basedir",
                                         "/basedir",
                                         namenode+"/basedir/middle/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat/middle/srcdat", files));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/basedir");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** tests basedir option copying files from dfs file system to dfs file system */
  @Test
  public void testBasedir() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/basedir/middle/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-basedir",
                                         "/basedir",
                                         namenode+"/basedir/middle/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat/middle/srcdat", files));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/basedir");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testCopyByChunkPreserveOption() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      {//test preserving user
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), "u" + i, null);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pu", "-copybychunk",
          nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "u" + i, dststat[i].getOwner());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving group
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), null, "g" + i);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pg", "-copybychunk", 
          nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "g" + i, dststat[i].getGroup());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving mode
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        FsPermission[] permissions = new FsPermission[srcstat.length];
        for(int i = 0; i < srcstat.length; i++) {
          permissions[i] = new FsPermission((short)(i & 0666));
          fs.setPermission(srcstat[i].getPath(), permissions[i]);
        }

        ToolRunner.run(new DistCp(conf),
            new String[]{"-pp", "-copybychunk",
          nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
  
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, permissions[i], dststat[i].getPermission());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving times
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        fs.mkdirs(new Path("/srcdat/tmpf1"));
        fs.mkdirs(new Path("/srcdat/tmpf2"));
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        FsPermission[] permissions = new FsPermission[srcstat.length];
        for(int i = 0; i < srcstat.length; i++) {
          fs.setTimes(srcstat[i].getPath(), 40, 50);
        }

        ToolRunner.run(new DistCp(conf),
            new String[]{"-pt", "-copybychunk", 
          nnUri+"/srcdat", nnUri+"/destdat"});

        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("Modif. Time i=" + i, 40, dststat[i].getModificationTime());
          assertEquals("Access Time i=" + i+ srcstat[i].getPath() + "-" + dststat[i].getPath(), 50, dststat[i].getAccessTime());
        }

        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));

        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test
  public void testPreserveOption() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      {//test preserving user
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), "u" + i, null);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pu", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "u" + i, dststat[i].getOwner());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving group
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), null, "g" + i);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pg", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "g" + i, dststat[i].getGroup());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving mode
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        FsPermission[] permissions = new FsPermission[srcstat.length];
        for(int i = 0; i < srcstat.length; i++) {
          permissions[i] = new FsPermission((short)(i & 0666));
          fs.setPermission(srcstat[i].getPath(), permissions[i]);
        }

        ToolRunner.run(new DistCp(conf),
            new String[]{"-pp", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
  
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, permissions[i], dststat[i].getPermission());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving times
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        fs.mkdirs(new Path("/srcdat/tmpf1"));
        fs.mkdirs(new Path("/srcdat/tmpf2"));
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        FsPermission[] permissions = new FsPermission[srcstat.length];
        for(int i = 0; i < srcstat.length; i++) {
          fs.setTimes(srcstat[i].getPath(), 40, 50);
        }

        ToolRunner.run(new DistCp(conf),
            new String[]{"-pt", nnUri+"/srcdat", nnUri+"/destdat"});

        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("Modif. Time i=" + i, 40, dststat[i].getModificationTime());
          assertEquals("Access Time i=" + i+ srcstat[i].getPath() + "-" + dststat[i].getPath(), 50, dststat[i].getAccessTime());
        }

        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));

        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test
  public void testCopyByChunkMapCount() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration(); 
      dfs = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = dfs.getFileSystem();
      final FsShell shell = new FsShell(conf);
      namenode = fs.getUri().toString();
      mr = new MiniMRCluster(3, namenode, 1);
      MyFile[] files = createFiles(fs.getUri(), "/srcdat");
      long totsize = 0;
      for (MyFile f : files) {
        totsize += f.getSize();
      }
      Configuration job = mr.createJobConf();
      job.setLong("distcp.bytes.per.map", totsize / 3);
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "100", "-r", "3",
                        "-copybychunk",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(fs, "/destdat", files));

      String logdir = namenode + "/logs";
      System.out.println(execCmd(shell, "-lsr", logdir));
      FileStatus[] logs = fs.listStatus(new Path(logdir));
      // rare case where splits are exact, logs.length can be 4
      assertTrue("Unexpected map count, logs.length=" + logs.length,
          logs.length == 5 || logs.length == 4);

      deldir(fs, "/destdat");
      deldir(fs, "/logs");
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "1", "-r", "1",
                        "-copybychunk",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});

      System.out.println(execCmd(shell, "-lsr", logdir));
      logs = fs.listStatus(new Path(namenode+"/logs"));
      assertTrue("Unexpected map count, logs.length=" + logs.length,
          logs.length == 2);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }
  
  @Test
  public void testDistCpWithTOSSuccess() throws Exception {
    distcpWithTOS(new int[] {-1, 4, 191});
  }
  
  @Test
  public void testDistCpWithTOSFailure() throws Exception {
    try {
      distcpWithTOS(new int[] {200});
      fail("Expected failure for wrong tos value");
    } catch (Exception e) {
      Log.warn("Expected exception: " + e.getMessage(), e);
    }
  }
  
  private void distcpWithTOS(int[] tosValue) throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = dfs.getFileSystem();
      namenode = fs.getUri().toString();
      mr = new MiniMRCluster(3, namenode, 1);
      MyFile[] files = createFiles(fs.getUri(), "/srcdat");
      
      Configuration job = mr.createJobConf();
      for (int i = 0; i < tosValue.length; i++) {
        ToolRunner.run(new DistCp(job),
            new String[] {"-m", "100", 
                          "-tos", String.valueOf(tosValue[i]),
                          "-log",
                          namenode+"/logs" + String.valueOf(i),
                          namenode+"/srcdat",
                          namenode+"/destdat" + String.valueOf(i)});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat" + String.valueOf(i), files));
      }
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }
  
  @Test
  public void testMapCount() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = dfs.getFileSystem();
      final FsShell shell = new FsShell(conf);
      namenode = fs.getUri().toString();
      mr = new MiniMRCluster(3, namenode, 1);
      MyFile[] files = createFiles(fs.getUri(), "/srcdat");
      long totsize = 0;
      for (MyFile f : files) {
        totsize += f.getSize();
      }
      Configuration job = mr.createJobConf();
      job.setLong("distcp.bytes.per.map", totsize / 3);
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "100",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(fs, "/destdat", files));

      String logdir = namenode + "/logs";
      System.out.println(execCmd(shell, "-lsr", logdir));
      FileStatus[] logs = fs.listStatus(new Path(logdir));
      // rare case where splits are exact, logs.length can be 4
      assertTrue("Unexpected map count, logs.length=" + logs.length,
          logs.length == 2);

      deldir(fs, "/destdat");
      deldir(fs, "/logs");
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "1",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});

      System.out.println(execCmd(shell, "-lsr", logdir));
      logs = fs.listStatus(new Path(namenode+"/logs"));
      assertTrue("Unexpected map count, logs.length=" + logs.length,
          logs.length == 2);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  @Test
  public void testCopyByChunkLimits() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final String nnUri = FileSystem.getDefaultUri(conf).toString();
      final FileSystem fs = FileSystem.get(URI.create(nnUri), conf);
      final DistCp distcp = new DistCp(conf);
      final FsShell shell = new FsShell(conf);  

      final String srcrootdir =  "/src_root";
      final Path srcrootpath = new Path(srcrootdir); 
      final String dstrootdir =  "/dst_root";
      final Path dstrootpath = new Path(dstrootdir); 

      {//test -filelimit
        MyFile[] files = createFiles(URI.create(nnUri), srcrootdir);
        int filelimit = files.length / 2;
        System.out.println("filelimit=" + filelimit);

        ToolRunner.run(distcp,
            new String[]{"-copybychunk", "-filelimit", ""+filelimit, 
            nnUri+srcrootdir, nnUri+dstrootdir});
        String results = execCmd(shell, "-lsr", dstrootdir);
        results = removePrefix(results, dstrootdir);
        System.out.println("results=" +  results);

        FileStatus[] dststat = getFileStatus(fs, dstrootdir, files, true);
        assertEquals(filelimit, dststat.length);
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }

      {//test -sizelimit
        createFiles(URI.create(nnUri), srcrootdir);
        long sizelimit = fs.getContentSummary(srcrootpath).getLength()/2;
        System.out.println("sizelimit=" + sizelimit);

        ToolRunner.run(distcp,
            new String[]{"-copybychunk", "-sizelimit", ""+sizelimit, 
            nnUri+srcrootdir, nnUri+dstrootdir});
        ContentSummary summary = fs.getContentSummary(dstrootpath);
        System.out.println("summary=" + summary);
        assertTrue(summary.getLength() <= sizelimit);
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testLimits() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final String nnUri = FileSystem.getDefaultUri(conf).toString();
      final FileSystem fs = FileSystem.get(URI.create(nnUri), conf);
      final DistCp distcp = new DistCp(conf);
      final FsShell shell = new FsShell(conf);  

      final String srcrootdir =  "/src_root";
      final Path srcrootpath = new Path(srcrootdir); 
      final String dstrootdir =  "/dst_root";
      final Path dstrootpath = new Path(dstrootdir); 

      {//test -filelimit
        MyFile[] files = createFiles(URI.create(nnUri), srcrootdir);
        int filelimit = files.length / 2;
        System.out.println("filelimit=" + filelimit);

        ToolRunner.run(distcp,
            new String[]{"-filelimit", ""+filelimit, nnUri+srcrootdir, nnUri+dstrootdir});
        String results = execCmd(shell, "-lsr", dstrootdir);
        results = removePrefix(results, dstrootdir);
        System.out.println("results=" +  results);

        FileStatus[] dststat = getFileStatus(fs, dstrootdir, files, true);
        assertEquals(filelimit, dststat.length);
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }

      {//test -sizelimit
        createFiles(URI.create(nnUri), srcrootdir);
        long sizelimit = fs.getContentSummary(srcrootpath).getLength()/2;
        System.out.println("sizelimit=" + sizelimit);

        ToolRunner.run(distcp,
            new String[]{"-sizelimit", ""+sizelimit, nnUri+srcrootdir, nnUri+dstrootdir});
        
        ContentSummary summary = fs.getContentSummary(dstrootpath);
        System.out.println("summary=" + summary);
        assertTrue(summary.getLength() <= sizelimit);
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }

      {//test update
        final MyFile[] srcs = createFiles(URI.create(nnUri), srcrootdir);
        final long totalsize = fs.getContentSummary(srcrootpath).getLength();
        System.out.println("src.length=" + srcs.length);
        System.out.println("totalsize =" + totalsize);
        fs.mkdirs(dstrootpath);
        final int parts = RAN.nextInt(NFILES/3 - 1) + 2;
        final int filelimit = srcs.length/parts;
        final long sizelimit = totalsize/parts;
        System.out.println("filelimit=" + filelimit);
        System.out.println("sizelimit=" + sizelimit);
        System.out.println("parts    =" + parts);
        final String[] args = {"-filelimit", ""+filelimit, "-sizelimit", ""+sizelimit,
            "-update", nnUri+srcrootdir, nnUri+dstrootdir};

        int dstfilecount = 0;
        long dstsize = 0;
        for(int i = 0; i <= parts; i++) {
          ToolRunner.run(distcp, args);
        
          FileStatus[] dststat = getFileStatus(fs, dstrootdir, srcs, true);
          System.out.println(i + ") dststat.length=" + dststat.length);
          assertTrue(dststat.length - dstfilecount <= filelimit);
          ContentSummary summary = fs.getContentSummary(dstrootpath);
          System.out.println(i + ") summary.getLength()=" + summary.getLength());
          assertTrue(summary.getLength() - dstsize <= sizelimit);
          assertTrue(checkFiles(fs, dstrootdir, srcs, true));
          dstfilecount = dststat.length;
          dstsize = summary.getLength();
        }

        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test
  public void testHftpAccessControl() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      final UnixUserGroupInformation DFS_UGI = createUGI("dfs", true); 
      final UnixUserGroupInformation USER_UGI = createUGI("user", false); 

      //start cluster by DFS_UGI
      final Configuration dfsConf = new Configuration();
      UnixUserGroupInformation.saveToConf(dfsConf,
          UnixUserGroupInformation.UGI_PROPERTY_NAME, DFS_UGI);
      cluster = new MiniDFSCluster(dfsConf, 2, true, null);
      cluster.waitActive();

      final String httpAdd = dfsConf.get("dfs.http.address");
      final URI nnURI = FileSystem.getDefaultUri(dfsConf);
      final String nnUri = nnURI.toString();
      final Path home = createHomeDirectory(FileSystem.get(nnURI, dfsConf), USER_UGI);
      
      //now, login as USER_UGI
      final Configuration userConf = new Configuration();
      UnixUserGroupInformation.saveToConf(userConf,
          UnixUserGroupInformation.UGI_PROPERTY_NAME, USER_UGI);
      final FileSystem fs = FileSystem.get(nnURI, userConf);

      final Path srcrootpath = new Path(home, "src_root"); 
      final String srcrootdir =  srcrootpath.toString();
      final Path dstrootpath = new Path(home, "dst_root"); 
      final String dstrootdir =  dstrootpath.toString();
      final DistCp distcp = new DistCp(userConf);

      FileSystem.mkdirs(fs, srcrootpath, new FsPermission((short)0700));
      final String[] args = {"hftp://"+httpAdd+srcrootdir, nnUri+dstrootdir};

      { //copy with permission 000, should fail
        fs.setPermission(srcrootpath, new FsPermission((short)0));
        assertEquals(-3, ToolRunner.run(distcp, args));
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /** test -delete */
  @Test
  public void testCopyByChunkDelete() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final URI nnURI = FileSystem.getDefaultUri(conf);
      final String nnUri = nnURI.toString();
      final FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      final DistCp distcp = new DistCp(conf);
      final FsShell shell = new FsShell(conf);  

      final String srcrootdir = "/src_root";
      final String dstrootdir = "/dst_root";

      {
        //create source files
        createFiles(nnURI, srcrootdir);
        String srcresults = execCmd(shell, "-lsr", srcrootdir);
        srcresults = removePrefix(srcresults, srcrootdir);
        System.out.println("srcresults=" +  srcresults);

        //create some files in dst
        createFiles(nnURI, dstrootdir);
        System.out.println("dstrootdir=" +  dstrootdir);
        shell.run(new String[]{"-lsr", dstrootdir});

        //run distcp
        ToolRunner.run(distcp,
            new String[]{"-delete", "-overwrite", "-copybychunk", "-log", "/log",
                         nnUri+srcrootdir, nnUri+dstrootdir});

        //make sure src and dst contains the same files
        String dstresults = execCmd(shell, "-lsr", dstrootdir);
        dstresults = removePrefix(dstresults, dstrootdir);
        System.out.println("first dstresults=" +  dstresults);
        assertEquals(srcresults, dstresults);

        //create additional file in dst
        create(fs, new Path(dstrootdir, "foo"));
        create(fs, new Path(dstrootdir, "foobar"));

        //run distcp again
        ToolRunner.run(distcp,
            new String[]{"-delete", "-overwrite", "-copybychunk", "-log", "/log2",
                         nnUri+srcrootdir, nnUri+dstrootdir});
        
        //make sure src and dst contains the same files
        dstresults = execCmd(shell, "-lsr", dstrootdir);
        dstresults = removePrefix(dstresults, dstrootdir);
        System.out.println("second dstresults=" +  dstresults);
        assertEquals(srcresults, dstresults);

        //cleanup
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
    
  /** test -delete */
  @Test
  public void testDelete() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final URI nnURI = FileSystem.getDefaultUri(conf);
      final String nnUri = nnURI.toString();
      final FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      final DistCp distcp = new DistCp(conf);
      final FsShell shell = new FsShell(conf);  

      final String srcrootdir = "/src_root";
      final String dstrootdir = "/dst_root";

      {
        //create source files
        createFiles(nnURI, srcrootdir);
        String srcresults = execCmd(shell, "-lsr", srcrootdir);
        srcresults = removePrefix(srcresults, srcrootdir);
        System.out.println("srcresults=" +  srcresults);

        //create some files in dst
        createFiles(nnURI, dstrootdir);
        System.out.println("dstrootdir=" +  dstrootdir);
        shell.run(new String[]{"-lsr", dstrootdir});

        //run distcp
        ToolRunner.run(distcp,
            new String[]{"-delete", "-update", "-log", "/log",
                         nnUri+srcrootdir, nnUri+dstrootdir});

        //make sure src and dst contains the same files
        String dstresults = execCmd(shell, "-lsr", dstrootdir);
        dstresults = removePrefix(dstresults, dstrootdir);
        System.out.println("first dstresults=" +  dstresults);
        assertEquals(srcresults, dstresults);

        //create additional file in dst
        create(fs, new Path(dstrootdir, "foo"));
        create(fs, new Path(dstrootdir, "foobar"));

        //run distcp again
        ToolRunner.run(distcp,
            new String[]{"-delete", "-update", "-log", "/log2",
                         nnUri+srcrootdir, nnUri+dstrootdir});
        
        //make sure src and dst contains the same files
        dstresults = execCmd(shell, "-lsr", dstrootdir);
        dstresults = removePrefix(dstresults, dstrootdir);
        System.out.println("second dstresults=" +  dstresults);
        assertEquals(srcresults, dstresults);

        //cleanup
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** test globbing  */
  public void testCopyByChunkGlobbing() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-copybychunk",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat/*",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                   fs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /** test globbing  */
  @Test
  public void testGlobbing() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration(); 
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat/*",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                   fs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** Test copying from a source directory that doesn't exist */
  @Test
  public void testSrcFileNotFound() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.get(LOCAL_FS, conf);
    deldir(localfs, TEST_ROOT_DIR+"/srcdatdoesnotexist");

    int result = ToolRunner.run(new DistCp(new Configuration()),
        new String[] {"file:///"+TEST_ROOT_DIR+"/srcdatdoesnotexist",
            "file:///"+TEST_ROOT_DIR+"/destdat"});

    assertEquals("Should have failed with a -1, indicating invalid arguments",
        -1, result);

    deldir(localfs, TEST_ROOT_DIR+"/destdat");
    deldir(localfs, TEST_ROOT_DIR+"/srcdatdoesnotexist");
  }
}
