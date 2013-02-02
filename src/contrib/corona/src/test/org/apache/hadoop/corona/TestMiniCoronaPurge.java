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

package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.regex.Pattern;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ExpireUnusedJobFiles;
import org.apache.hadoop.mapred.Clock;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

/**
 * A Unit-test to test job execution of Mini Corona Map-Reduce Cluster.
 */
public class TestMiniCoronaPurge extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestMiniCoronaPurge.class);
  private static final long THRESHOLD = 60000L;

  public void testPurge() throws Exception {
    LOG.info("Starting testPurge");
    JobConf conf = new JobConf();
    conf.setLong("mapred.job.file.expirethreshold", THRESHOLD);
    conf.setLong("mapred.job.file.checkinterval",10000);
    conf.setLong("mapred.job.history.expirethreshold", THRESHOLD);
    conf.setLong("mapred.job.history.checkinterval",10000);
    conf.setBoolean("mapred.job.temp.cleanup", true);
    String systemDir = "/tmp/hadoop/mapred/system";
    FileSystem fs = new Path(systemDir).getFileSystem(conf);
    Path tagDir;
    // long oldtime = System.currentTimeMillis() - THRESHOLD;
    for (int i = 0; i < 10; i++) {
      tagDir= new Path(systemDir + "/job_00000.00000_00" + i);
      if (!fs.exists(tagDir)) { 
        fs.mkdirs(tagDir); 
      }
      // fs.setTimes(tagDir, oldtime, -1);
    }
  
    // Those dirs shall not be removed
    tagDir= new Path(systemDir + "/nojob_00000.00000_01");
    if (!fs.exists(tagDir)) { 
      fs.mkdirs(tagDir); 
    }
    // fs.setTimes(tagDir, oldtime, -1);

    Thread.sleep(2*THRESHOLD);
    Pattern p = Pattern.compile("^(.+)\\/job_(\\d+)\\.(\\d+)_(\\d+)$");
    ExpireUnusedJobFiles expire = new ExpireUnusedJobFiles(new Clock(), conf,
      new Path(systemDir), p, THRESHOLD);
    expire.run();

    LOG.info("Check dirs under " + systemDir);
    // check if the dirs have been removed 
    boolean result;
    for (int i = 0; i < 10; i++) {
      tagDir= new Path(systemDir + "/job_00000.00000_00" + i);
      result = fs.exists(tagDir);
      assertTrue(tagDir + " is not deleted", result == false); 
    }

    // The dirs shall not be removed
    tagDir= new Path(systemDir + "/nojob_00000.00000_01" );
    result = fs.exists(tagDir);
    assertTrue(tagDir + " is deleted", result == true); 
  }

}
