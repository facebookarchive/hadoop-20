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

import java.io.IOException;
import java.text.SimpleDateFormat;
import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A Unit-test to test release manager
 */
public class TestReleaseManager extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestReleaseManager.class);
  private static final long CLEAN_INTERVAL = 10000L;
  private static final long CLEAN_THRESHOLD = 20000L;
  private static final String RELEASE_DIR = "/tmp/releaseDir";
  private static final String WORKING_DIR = "/tmp/workingDir";
  private static final String TAG_FILE_NAME = "releaseTag";
  private static final int MAX_RETRY = 10;

  private SimpleDateFormat formatter;
  private Configuration conf;
  private FileSystem fs;
  private Path releasePath;
  private Path workingPath;
  private String tagFileName;
  private Path releaseTagPath;
  private long releaseTimeStamp;
  private CoronaReleaseManager crReleaseManager;

  @Override
  protected void setUp() throws Exception {
    formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    conf = new Configuration();
    conf.set("mapred.release.dir", RELEASE_DIR);
    conf.set("mapred.release.working.dir", WORKING_DIR);
    conf.setLong("mapred.release.dir.cleanInterval", CLEAN_INTERVAL);
    conf.setLong("mapred.release.dir.cleanThreshold", CLEAN_THRESHOLD);

    fs = FileSystem.newInstanceLocal(conf);
    releasePath = new Path(RELEASE_DIR);
    workingPath = new Path(WORKING_DIR);
    tagFileName = TAG_FILE_NAME;
    releaseTagPath = new Path(releasePath, tagFileName);
    touchTag();

    crReleaseManager = new CoronaReleaseManager(conf);
    crReleaseManager.start();
  }

  @Override
  protected void tearDown() throws Exception {
    crReleaseManager.shutdown();
    fs.delete(releasePath, true);
    fs.delete(workingPath, true);
  }

  private void touchTag() throws IOException {
    fs.create(releaseTagPath);
    FileStatus tagStatus = fs.getFileStatus(releaseTagPath);
    releaseTimeStamp = tagStatus.getModificationTime();
  }

  // get working release from release manager's autorun
  private String getAutoRelease(long timestamp, JobID jobid)
    throws IOException {
    boolean created = false;
    Path checkPath = new Path(workingPath, formatter.format(timestamp));
    Path checkTagPath = new Path(checkPath, TAG_FILE_NAME);
    for (int i = 0; i < MAX_RETRY; i++) {
      try {
        if (fs.exists(checkTagPath)) {
          created = true;
          LOG.info(checkPath + " is created successfully");
          break;
        }
      } catch (IOException e) {
      }
      LOG.info("Waiting for the release manager");
      try {
        Thread.sleep(CLEAN_INTERVAL);
      } catch (InterruptedException e) {
      }
    }   
    if (!created) {
      LOG.error(checkTagPath + " is not created");
    }
    assertEquals(created, true);

    String resultStr = crReleaseManager.getRelease(jobid);
    assertEquals(resultStr, checkPath.toString());
    return resultStr;
  }

  // get working release by demand
  private String getRelease(long timestamp, JobID jobid) throws IOException {
    Path checkPath = new Path(workingPath, formatter.format(timestamp));
    String resultStr = crReleaseManager.getRelease(jobid);
    assertEquals(resultStr, checkPath.toString());
    return resultStr;
  }

  // test the release is copied by the release manager automatically
  public void testInitialCopy() throws IOException {
    LOG.info("Start testInitialCopy");
    JobID jobid = new JobID("TestJob", 1);
    getAutoRelease(releaseTimeStamp, jobid);
    LOG.info("Done with the testing for testInitialCopy");
  }

  // test if there is new release, release manager will copy automatically
  public void testIncrementalCopy() throws IOException {
    LOG.info("Start testIncrementalCopy");
    try {
      Thread.sleep(CLEAN_INTERVAL);
    } catch (InterruptedException e) {
    }

    // change the release timestamp
    touchTag();
    JobID jobid = new JobID("TestJob", 1);
    getAutoRelease(releaseTimeStamp, jobid);
    LOG.info("Done with the testing for testIncrementalCopy");
  }

  // test if there is new release, release manager can copy by demand
  public void testCopyByDemand() throws IOException {
    LOG.info("Start testCopyByDemand");
    touchTag();
    JobID jobid = new JobID("TestJob", 1);
    getRelease(releaseTimeStamp, jobid);
    LOG.info("Done with the testing for testCopyByDemand");
  }

  // test if an old release is not used any more if will be removed
  public void testCleanup() throws IOException {
    LOG.info("Start testCleanup");
    JobID jobid = new JobID("TestJob", 1);
    String oldPath = getAutoRelease(releaseTimeStamp, jobid);
    try {
      Thread.sleep(CLEAN_INTERVAL);
    } catch (InterruptedException e) {
    }
    touchTag();
    JobID jobid2 = new JobID("TestJob", 2);
    getRelease(releaseTimeStamp, jobid2);

    crReleaseManager.returnRelease(jobid);
    boolean deleted = false;
    for (int i = 0; i < MAX_RETRY; i++) {
      LOG.info("Waiting for the release manager");
      try {
        Thread.sleep(CLEAN_INTERVAL);
      } catch (InterruptedException e) {
      }
      try {
        if (!fs.exists(new Path(oldPath))) {
          deleted = true;
          LOG.info( oldPath + " is deleted");
          break;
        }
      } catch (IOException e) {
      }
    }   
    if (!deleted) {
      LOG.error( oldPath + " is not deleted");
    }
    assertEquals(deleted, true);
    LOG.info("Done with the testing for testCleanup");
  }

  // test getRelease without return, and the working copy shall stay
  public void testNoCleanup() throws IOException {
    LOG.info("Start testNoCleanup");
    JobID jobid = new JobID("TestJob", 1);
    String oldPath = getAutoRelease(releaseTimeStamp, jobid);
    try {
      Thread.sleep(CLEAN_INTERVAL);
    } catch (InterruptedException e) {
    }

    touchTag();
    JobID jobid2 = new JobID("TestJob", 2);
    getRelease(releaseTimeStamp, jobid2);
    
    boolean deleted = false;
    for (int i = 0; i < MAX_RETRY; i++) {
      LOG.info("Waiting for the release manager");
      try {
        Thread.sleep(CLEAN_INTERVAL);
      } catch (InterruptedException e) {
      }
      try {
        if (!fs.exists(new Path(oldPath))) {
          deleted = true;
          LOG.error( oldPath + " is deleted by mistake");
          break;
        }
      } catch (IOException e) {
      }
    }   
    if (!deleted) {
      LOG.info(oldPath + " is not deleted");
    }
    assertEquals(deleted, false);
    LOG.info("Done with the testing for testNoCleanup");
  }

  // test when a working release is used, the tag file timestamp is changed
  public void testNewTag() throws IOException {
    LOG.info("Start testNewTag");
    JobID jobid = new JobID("TestJob", 1);
    long oldTimeStamp = releaseTimeStamp;
    long currentTimeStamp = System.currentTimeMillis();
    try {
      Thread.sleep(1000);
    } catch(InterruptedException e) {
    }
    String workingPath = getRelease(releaseTimeStamp, jobid);
    String workingTag = workingPath + "/RELEASE_COPY_DONE";
    FileStatus tagStatus = fs.getFileStatus(new Path(workingTag));
    long newTimeStamp = tagStatus.getModificationTime();
    LOG.info("Before getRelease, " + workingTag + " timestamp is " + oldTimeStamp);
    LOG.info("After getRelease, the timestamp is " + newTimeStamp);
    assertEquals(newTimeStamp > currentTimeStamp, true);
    assertEquals(newTimeStamp > oldTimeStamp, true);
    LOG.info("Done with the testing for testNewTag");
  }
}
