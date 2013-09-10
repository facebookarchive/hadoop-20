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


/** Provides a <i>trash</i> feature.  Files are moved to a user's trash
 * directory, a subdirectory of their home directory named ".Trash".  Files are
 * initially moved to a <i>current</i> sub-directory of the trash directory.
 * Within that sub-directory their original path is preserved.  Periodically
 * one may checkpoint the current trash and remove older checkpoints.  (This
 * design permits trash management without enumeration of the full trash
 * content, without date support in the filesystem, and without clock
 * synchronization.)
 */
public class TrashPolicyDefault extends TrashPolicyBase {
  protected Path homesParent;

  public TrashPolicyDefault() { }

  protected TrashPolicyDefault(Path home, Configuration conf) throws IOException {
    initialize(conf, home.getFileSystem(conf), home);
  }

  @Override
  public void initialize(Configuration conf, FileSystem fs, Path home) {
    super.initialize(conf, fs, home);
    this.trash = getTrashDirFromBase(home);
    this.homesParent = home.getParent();
  }

  @Override
  public Path getTrashDir(Path rmPath) {
    return trash;
  }

  @Override
  protected FileStatus[] getTrashBases() throws IOException {
    return fs.listStatus(homesParent);
  }

  @Override
  protected Path getExtraTrashPath() throws IOException {
    return null;
  }
  
  @Override
  protected TrashPolicyBase getTrashPolicy(Path trashBasePath, Configuration conf) throws IOException {
    return new TrashPolicyDefault(trashBasePath, conf);
  }
}
