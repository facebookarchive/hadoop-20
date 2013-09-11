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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/** 
 * Provides a trash facility which supports pluggable Trash policies. 
 *
 * See the implementation of the configured TrashPolicy for more
 * details.
 */
public class Trash extends Configured {
  private TrashPolicy trashPolicy; // configured trash policy instance

  /** 
   * Construct a trash can accessor.
   * @param conf a Configuration
   */
  public Trash(Configuration conf) throws IOException {
    this(FileSystem.get(conf), conf);
  }

  /**
   * Construct a trash can accessor for the FileSystem provided.
   * @param fs the FileSystem
   * @param conf a Configuration
   */
  public Trash(FileSystem fs, Configuration conf) throws IOException {
    this(fs, conf, null);
  }

  /**
   * Construct a trash can accessor for the FileSystem provided.
   * @param fs the FileSystem
   * @param conf a Configuration
   * @param userName name of the user whose home directory will be used
   */
  public Trash(FileSystem fs, Configuration conf,
               String userName) throws IOException {
    super(conf);
    trashPolicy = TrashPolicy.getInstance(
      conf, fs, fs.getHomeDirectory(userName));
  }

  /**
   * Returns whether the trash is enabled for this filesystem
   */
  public boolean isEnabled() {
    return trashPolicy.isEnabled();
  }
  
  public void setDeleteInterval(long deleteInterval) {
    trashPolicy.deletionInterval = deleteInterval;
  }
  
  public long getDeletionInterval(){
    return trashPolicy.deletionInterval;
  }

  /** Move a file or directory to the current trash directory.
   * @return false if the item is already in the trash or trash is disabled
   */ 
  public boolean moveToTrash(Path path) throws IOException {
    return trashPolicy.moveToTrash(path);
  }

  /**
   * Move a file or directory from the current trash directory.
   * @param path path to restore. Should not contain the ".Trash" portion
   * @return false if the item does not exist in the trash or trash is disabled
   * @throws IOExcetion
   */ 
  public boolean moveFromTrash(Path path) throws IOException {
    return trashPolicy.moveFromTrash(path);
  }

  /** Create a trash checkpoint. */
  public void checkpoint() throws IOException {
    trashPolicy.createCheckpoint();
  }

  /** Delete old checkpoint(s). */
  public void expunge() throws IOException {
    trashPolicy.deleteCheckpoint();
  }

  /** get the current working directory */
  Path getCurrentTrashDir() throws IOException {
    return trashPolicy.getCurrentTrashDir();
  }

  /** get the configured trash policy */
  TrashPolicy getTrashPolicy() {
    return trashPolicy;
  }

  /** Return a {@link Runnable} that periodically empties the trash of all
   * users, intended to be run by the superuser.
   */
  public Runnable getEmptier() throws IOException {
    return trashPolicy.getEmptier();
  }

  /** Run an emptier.*/
  public static void main(String[] args) throws Exception {
    new Trash(new Configuration()).getEmptier().run();
  }
}
