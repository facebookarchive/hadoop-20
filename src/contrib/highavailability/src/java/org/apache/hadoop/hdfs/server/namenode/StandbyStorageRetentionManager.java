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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;

/**
 * StandbyStorageRetentionManager manages the standby backups created on startup.
 * Whenever the standby starts, it copies all its own storage directories aside,
 * marking them with a time stamp (e.g., clustername:2012-10-11-06:42:03.149).
 * 
 * The policy is governed by two parameters:
 * 
 * A) standby.image.days.tokeep - minimum days to keep backups
 * B) standby.image.copies.tokeep - minimum copies of backup to keep.
 * 
 * -------
 * If A > 0, at startup, the standby will prune all backups older than A,
 * providing that it retains at least B copies (together with the current one).
 * If more copies than B are left, the standby will abort startup.
 * 
 * If A == 0, the standby will keep B copies (together with the current one.
 * In this case, the startup would never fail, as the standby will remove the
 * oldest copies.
 * 
 * Special case: If both A and B are 0, standby does not delete any backups.
 */
public class StandbyStorageRetentionManager {

  public static final Log LOG = LogFactory.getLog(StandbyStorageRetentionManager.class.getName());
  public static final String STANDBY_IMAGE_COPIES_TOKEEP = "standby.image.copies.tokeep";
  public static final int STANDBY_IMAGE_COPIES_TOKEEP_DEFAULT = 5;
  
  public static final String STANDBY_IMAGE_DAYS_TOKEEP = "standby.image.days.tokeep";
  public static final int STANDBY_IMAGE_DAYS_TOKEEP_DEFAULT = 7;
  
  public static final SimpleDateFormat dateForm =
      new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS");
  
  /**
   * Backup given directory.
   * Enforce that max number of backups has not been reached.
   */
  static void backupFiles(FileSystem fs, File dest, 
      Configuration conf) throws IOException {
    
    // check if we can still backup
    cleanUpAndCheckBackup(conf, dest);
    
    int MAX_ATTEMPT = 3;
    for (int i = 0; i < MAX_ATTEMPT; i++) {
      try {
        String mdate = dateForm.format(new Date(AvatarNode.now()));
        if (dest.exists()) {
          File tmp = new File (dest + File.pathSeparator + mdate);
          if (!dest.renameTo(tmp)) {
            throw new IOException("Unable to rename " + dest +
                                  " to " +  tmp);
          }
          LOG.info("Moved aside " + dest + " as " + tmp);
        }
        return;
      } catch (IOException e) {
        LOG.error("Creating backup exception. Will retry ", e);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException iex) {
          throw new IOException(iex);
        }
      }
    }
    throw new IOException("Cannot create backup for: " + dest);
  }

  
  /** 
   * Check if we have not exceeded the maximum number of backups.
   */
  static void cleanUpAndCheckBackup(Configuration conf, File origin) throws IOException {
    // get all backups
    String[] backups = getBackups(origin);
    File root = origin.getParentFile();
    
    // maximum total number of backups
    int copiesToKeep = conf.getInt(STANDBY_IMAGE_COPIES_TOKEEP,
        STANDBY_IMAGE_COPIES_TOKEEP_DEFAULT);

    // days to keep, if set to 0 than keep only last backup
    int daysToKeep = conf.getInt(STANDBY_IMAGE_DAYS_TOKEEP,
        STANDBY_IMAGE_DAYS_TOKEEP_DEFAULT);
    
    if (copiesToKeep == 0 && daysToKeep == 0) {
      // Do not delete anything in this case
      // every startup will create extra checkpoint
      return;
    }
    
    // cleanup copies older than daysToKeep
    deleteOldBackups(root, backups, daysToKeep, copiesToKeep);
    
    // check remaining backups
    backups = getBackups(origin);
    if (backups.length >= copiesToKeep) {
      throw new IOException("Exceeded maximum number of standby backups of "
          + origin + " under " + origin.getParentFile() + " max: " + copiesToKeep);
    }
  }
  
  /**
   * Delete backups according to the retention policy.
   * 
   * @param root root directory
   * @param backups backups SORTED on the timestamp from oldest to newest
   * @param daysToKeep
   * @param copiesToKeep
   */
  static void deleteOldBackups(File root, String[] backups, int daysToKeep,
      int copiesToKeep) {
    Date now = new Date(AvatarNode.now());
    
    // leave the copiesToKeep-1 at least (+1 will be the current backup)
    int maxIndex = Math.max(0, backups.length - copiesToKeep + 1);
    
    for (int i = 0; i < maxIndex; i++) {
      String backup = backups[i];
      Date backupDate = null;
      try {
        backupDate = dateForm.parse(backup.substring(backup
            .indexOf(File.pathSeparator) + 1));
      } catch (ParseException pex) {
        // This should not happen because of the 
        // way we construct the list
      }
      long backupAge = now.getTime() - backupDate.getTime();
      
      // if daysToKeep is set delete everything older providing that
      // we retain at least copiesToKeep copies
      boolean deleteOldBackup = (daysToKeep > 0
          && backupAge > daysToKeep * 24 * 60 * 60 * 1000);

      // if daysToKeep is set to zero retain most recent copies
      boolean deleteExtraBackup = (daysToKeep == 0);
      
      if (deleteOldBackup || deleteExtraBackup) {
        // This backup is older than daysToKeep, delete it
        try {
          FileUtil.fullyDelete(new File(root, backup));
          LOG.info("Deleted backup " + new File(root, backup));
        } catch (IOException iex) {
          LOG.error("Error deleting backup " + new File(root, backup), iex);
        }
      } else {
        // done with deleting old backups
        break;
      }
    }
  }
  
  /**
   * List all directories that match the backup pattern.
   * Sort from oldest to newest.
   */
  static String[] getBackups(File origin) {
    File root = origin.getParentFile();
    final String originName = origin.getName();
    
    String[] backups = root.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (!name.startsWith(originName + File.pathSeparator)
            || name.equals(originName))
          return false;
        try {
          dateForm.parse(name.substring(name.indexOf(File.pathSeparator) + 1));
        } catch (ParseException pex) {
          return false;
        }
        return true;
      }
    });
    if (backups == null)
      return new String[0];

    Arrays.sort(backups, new Comparator<String>() {

      @Override
      public int compare(String back1, String back2) {
        try {
          Date date1 = dateForm.parse(back1.substring(back1
              .indexOf(File.pathSeparator) + 1));
          Date date2 = dateForm.parse(back2.substring(back2
              .indexOf(File.pathSeparator) + 1));
          // Sorting in reverse order, from later dates to earlier
          return -1 * date2.compareTo(date1);
        } catch (ParseException pex) {
          return 0;
        }
      }
    });
    return backups;
  } 
}
