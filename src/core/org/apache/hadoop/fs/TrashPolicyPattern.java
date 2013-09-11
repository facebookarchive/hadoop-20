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

/** Provides a <i>trash</i> feature.  Files are moved to one of the trash
 * base directories, which is specified by as path pattern. Files are
 * initially moved to a <i>current</i> sub-directory of the trash directory.
 * Within that sub-directory their original path is preserved.  Periodically
 * one may checkpoint the current trash and remove older checkpoints.  (This
 * design permits trash management without enumeration of the full trash
 * content, without date support in the filesystem, and without clock
 * synchronization.)
 * 
 * User needs to config two parameters:
 * fs.trash.base.paths specifies the pattern of base paths.
 * fs.trash.unmatched.paths specifies a default trash location if file is
 * not under any of the base pathes.
 * 
 * For example, if
 * fs.trash.base.paths = /namespace/*
 * fs.trash.unmatched.paths = /
 * 
 * Then file /namespace/ns1/x/y/z will be moved to
 * /namespace/ns1/.Trash/Current/namespace/ns1/x/y/z
 * but /x/y/z will be moved to /.Trash/x/y/z
 * 
 * fs.trash.base.paths follows the pattern of globStatus(),
 * so it can be something like "{/namespace/*,/user/*}"
 */
public class TrashPolicyPattern extends TrashPolicyBase {
  private Path basePathPattern;
  private Path unmatchedTrashPath;

  public TrashPolicyPattern() { }

  protected TrashPolicyPattern(FileSystem fs, Path trashBasePath, Configuration conf)
      throws IOException {
    initialize(conf, fs, null);
    this.trash = getTrashDirFromBase(fs.makeQualified(trashBasePath));
  }

  @Override
  public void initialize(Configuration conf, FileSystem fs, Path home) {
    super.initialize(conf, fs, home);
    basePathPattern = new Path(conf.get("fs.trash.base.paths",
        "{/namespace/*/,/user/*/}"));
    unmatchedTrashPath = new Path(conf.get("fs.trash.unmatched.paths",
        "/"));
  }
  
  @Override
  public Path getTrashDir(Path rmPath) throws IOException {
    if (rmPath != null) {
      rmPath = fs.makeQualified(rmPath);
    }
    if (rmPath == null && trash == null) {
      throw new IOException("Per user trash is disabled");
    } else if (rmPath == null) {
      return trash;
    } else if (trash == null) {
      FileStatus[] trashBasePaths = getTrashBases();
      for (FileStatus trashBase : trashBasePaths) {
        if (trashBase.isDir()
            && rmPath.toString().startsWith(trashBase.getPath().toString())
            && !trashBase.getPath().toString().equals(rmPath.toString())) {
          return getTrashDirFromBase(trashBase.getPath());
        }
      }
      return getTrashDirFromBase(unmatchedTrashPath);
    } else {
      if (rmPath.toString().startsWith(trash.toString())) {
        return trash;
      } else {
        throw new IOException("File is not under trash base directory");
      }
    }
  }

  @Override
  protected FileStatus[] getTrashBases() throws IOException {
    return fs.globStatus(basePathPattern);
  }

  @Override
  protected Path getExtraTrashPath() throws IOException {
    return unmatchedTrashPath;
  }

  
  @Override
  protected TrashPolicyBase getTrashPolicy(Path trashBasePath,
      Configuration conf) throws IOException {
    return new TrashPolicyPattern(this.fs, trashBasePath, conf);
  }

}
