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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Before;
import org.junit.Test;

public class TestNNStorageDirectoryRetentionManager {

  public static final Log LOG = LogFactory
      .getLog(TestNNStorageDirectoryRetentionManager.class.getName());

  private File base_dir = new File(System.getProperty("test.build.data",
      "build/test/data"));
  private Configuration conf;
  private FileSystem fs;
  private File dir = new File(base_dir, "test");

  @Before
  public void setUp() throws IOException {
    LOG.info("----------------- START ----------------- ");
    conf = new Configuration();
    fs = FileSystem.getLocal(conf).getRaw();
    FileUtil.fullyDelete(base_dir);
  }

  @Test
  public void testBackupNoLimit() throws IOException {
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_DAYS_TOKEEP, 0);
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_COPIES_TOKEEP, 0);

    // we can create backups indefinitely
    createBackups(50, conf, dir, fs, -1);
  }

  @Test
  public void testBackupWithLimit() throws IOException {
    int limit = 5;
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_DAYS_TOKEEP, 1);
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_COPIES_TOKEEP,
        limit);

    // we can create backups up to limit-1, the limit-th should fail
    createBackups(10, conf, dir, fs, limit);
  }

  @Test
  public void testDeleteOldBackups1() throws IOException {
    // 3-1 two copies should be retained
    int daysToKeep = 0;
    int copiesToKeep = 3;
    int initialCopies = 10;
    testDeleteOldBackupsInternal(initialCopies, copiesToKeep, daysToKeep);
  }

  @Test
  public void testDeleteOldBackups2() throws IOException {
    // no copies should be deleted
    int daysToKeep = 0;
    int copiesToKeep = 5;
    int initialCopies = 3;
    testDeleteOldBackupsInternal(initialCopies, copiesToKeep, daysToKeep);
  }

  @Test
  public void testSortedListBackups() throws Exception {
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_DAYS_TOKEEP, 0);
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_COPIES_TOKEEP, 0);

    String[] backups = createBackups(10, conf, dir, fs, -1);
    assertEquals(10, backups.length);
    Date[] backupDates = new Date[backups.length];
    for (int i = 0; i < backups.length; i++) {
      String b = backups[i];
      backupDates[i] = NNStorageDirectoryRetentionManager.dateForm.get().parse(b
          .substring(b.indexOf(File.pathSeparator) + 1));
    }

    // aseert that the copies are sorted from oldest to newest
    for (int i = 1; i < backupDates.length; i++) {
      backupDates[i].after(backupDates[i - 1]);
    }
  }

  // /////////////////////////////////////////////////////////////////////////

  private void testDeleteOldBackupsInternal(int initialCopies,
      int copiesToKeep, int daysToKeep) throws IOException {
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_DAYS_TOKEEP, 0);
    conf.setInt(NNStorageDirectoryRetentionManager.NN_IMAGE_COPIES_TOKEEP, 0);

    // create backups
    String[] backups = createBackups(initialCopies, conf, dir, fs, -1);

    NNStorageDirectoryRetentionManager.deleteOldBackups(dir.getParentFile(),
        backups, daysToKeep, copiesToKeep);

    String[] backupsAfter = NNStorageDirectoryRetentionManager.getBackups(dir);

    int shift = Math.max(0, initialCopies - copiesToKeep + 1);
    for (int i = 0; i < backupsAfter.length; i++) {
      assertTrue(backupsAfter[i].equals(backups[i + shift]));
    }

    // days to keep is set to 0, so we should keep 3-1 backups
    // or all
    int expectedCount = shift == 0 ? initialCopies : copiesToKeep - 1;
    assertEquals(expectedCount,
        NNStorageDirectoryRetentionManager.getBackups(dir).length);
  }

  private static String[] createBackups(int count, Configuration conf,
      File dir, FileSystem fs, int failAtBackup) throws IOException {
    // create count backups
    for (int i = 0; i < count; i++) {
      // create directory
      dir.mkdirs();

      try {
        // create backup for the directory
        NNStorageDirectoryRetentionManager.backupFiles(fs, dir, conf);
        if (i == failAtBackup) {
          fail("Backup should not be created");
        }
      } catch (IOException e) {
        if (i == failAtBackup) {
          LOG.info("Expected exception", e);
          break;
        }
      }

      // original directory does not exist
      assertFalse(dir.exists());

      String[] backups = NNStorageDirectoryRetentionManager.getBackups(dir);
      assertEquals(i + 1, backups.length);
      sleep();
    }
    return NNStorageDirectoryRetentionManager.getBackups(dir);
  }

  private static void sleep() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      // Ignore
    }
  }

}
