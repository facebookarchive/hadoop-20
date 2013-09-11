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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.namenode.ValidateNamespaceDirPolicy.NNStorageLocation;
import org.junit.Before;
import org.junit.Test;

public class TestValidateNamespaceDirPolicy {

  private static final Log LOG = LogFactory.getLog(FSEditLog.class);
  private Configuration conf;

  private File base_dir = new File(System.getProperty("test.build.data",
      "build/test/data"));
  private File dir1 = new File(base_dir, "dir1");
  private File dir2 = new File(base_dir, "dir2");
  private URI uri1, uri2;
  private List<URI> dirs = new ArrayList<URI>();

  @Before
  public void setUp() throws IOException, URISyntaxException {
    conf = new Configuration();
    dir1.delete();
    dir2.delete();
    if (dir1.exists())
      FileUtil.fullyDelete(dir1);
    if (dir2.exists())
      FileUtil.fullyDelete(dir2);
    uri1 = new URI("file:" + dir1.getAbsolutePath());
    uri2 = new URI("file:" + dir2.getAbsolutePath());
    dirs.clear();
    dirs.add(uri1);
    dirs.add(uri2);
  }

  @Test
  public void testNonExistentAny() {
    // the specified directories do not exist
    dir1.mkdirs();
    testFailure(0, "One of the locations does not exist", dirs, conf);
    testFailure(1, "One of the locations does not exist", dirs, conf);
    testFailure(2, "One of the locations does not exist", dirs, conf);
  }

  @Test
  public void testExistentFile() throws IOException {
    // the specified directory is a file
    dir1.mkdirs();
    dir2.createNewFile();
    testFailure(0, "One of the locations is a file", dirs, conf);
    testFailure(1, "One of the locations is a file", dirs, conf);
    testFailure(2, "One of the locations is a file", dirs, conf);
  }

  @Test
  public void testExistentPolicy1() throws IOException {
    // the specified directories exist but are on the same device
    dir1.mkdirs();
    dir2.mkdirs();
    testFailure(1, "Device is the same", dirs, conf);
    testFailure(2, "Device is the same", dirs, conf);
  }

  private void testFailure(int policy, String message, Collection<URI> dirs,
      Configuration conf) {
    try {
      ValidateNamespaceDirPolicy.validatePolicy(conf, policy, dirs,
          "test.param", new HashMap<URI, NNStorageLocation>());
      fail("Should throw IOException." + message);
    } catch (IOException e) {
      LOG.info("Expected exception", e);
    }
  }
}
