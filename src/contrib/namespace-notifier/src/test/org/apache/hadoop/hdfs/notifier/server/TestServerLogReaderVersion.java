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
package org.apache.hadoop.hdfs.notifier.server;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.notifier.NotifierUtils;
import org.apache.hadoop.hdfs.notifier.server.TestPreTransactionalServerLogReader.DummyServerCore;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.junit.Before;
import org.junit.Test;

public class TestServerLogReaderVersion {

  public static final Log LOG = LogFactory
      .getLog(TestServerLogReaderVersion.class.getName());

  private File base_dir = new File(System.getProperty("test.build.data",
      "build/test/data"));

  private File editsDir;

  @Before
  public void setUp() throws IOException {
    LOG.info("----------------- START ----------------- ");
    FileUtil.fullyDelete(base_dir);
    editsDir = new File(base_dir, "edits");
    editsDir.mkdirs();
  }

  @Test
  public void testVersion() throws IOException {
    // -37 is pre-transactional layout
    int lv = 12345;
    StorageInfo si = new StorageInfo(lv, 10, 0);
    StorageDirectory sd = new NNStorage(si).new StorageDirectory(editsDir);
    format(sd);

    URI editsURI = Util.stringAsURI(sd.getRoot().getAbsolutePath());
    int version = NotifierUtils.getVersion(editsURI);
    LOG.info("Read version: " + lv);
    assertEquals(lv, version);
  }

  @Test
  public void testVersionNonFile() throws IOException {
    URI editsURI = Util.stringAsURI("foo:/bar");
    try {
      NotifierUtils.getVersion(editsURI);
      fail("Non-files are not supported for now");
    } catch (Exception e) {
      LOG.info("Expected exception " + e.getMessage());
    }
  }

  @Test
  public void testMissingVersion() throws IOException {
    // -37 is pre-transactional layout
    int lv = 12345;
    StorageInfo si = new StorageInfo(lv, 10, 0);
    StorageDirectory sd = new NNStorage(si).new StorageDirectory(editsDir);
    format(sd);

    URI editsURI = Util.stringAsURI(sd.getRoot().getAbsolutePath());

    // remove verision file
    sd.getVersionFile().delete();
    try {
      NotifierUtils.getVersion(editsURI);
      fail("Should fail");
    } catch (Exception e) {
      LOG.info("expected exception: " + e.getMessage());
    }
  }

  public static EditLogFileOutputStream initEdits(File editsDir)
      throws IOException {
    File edits = TestPreTransactionalServerLogReader.getFileWithCurrent(
        editsDir, "edits_inprogress_0000000000000000000");

    if (!edits.createNewFile())
      throw new IOException("Failed to create edits file");
    EditLogFileOutputStream out = new EditLogFileOutputStream(edits, null);
    out.create();
    return out;
  }

  private void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir
    sd.write();
    LOG.info("Storage directory " + sd.getRoot()
        + " has been successfully formatted.");
  }
}
