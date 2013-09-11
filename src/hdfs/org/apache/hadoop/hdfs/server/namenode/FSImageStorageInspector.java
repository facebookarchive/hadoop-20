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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NNStorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;

/**
 * Interface responsible for inspecting a set of storage directories and devising
 * a plan to load the namespace from them.
 */
public abstract class FSImageStorageInspector {
  /**
   * Inspect the contents of the given storage directory.
   */
  public abstract void inspectDirectory(StorageDirectory sd) throws IOException;

  /**
   * @return false if any of the storage directories have an unfinalized upgrade 
   */
  abstract boolean isUpgradeFinalized();
  
  /**
   * Get the image files which should be loaded into the filesystem.
   * @throws IOException if not enough files are available (eg no image found in any directory)
   */
  abstract FSImageFile getLatestImage() throws IOException;

  /** 
   * Get the minimum tx id which should be loaded with this set of images.
   */
  abstract long getMaxSeenTxId();

  /**
   * @return true if the directories are in such a state that the image should be re-saved
   * following the load
   */
  abstract boolean needToSave();
  
  /**
   * @return true if the edits properties necessitate re-saving 
   * following the load of edits
   */
  abstract boolean forceSave();

  /**
   * Record of an image that has been located and had its filename parsed.
   */
  public static class FSImageFile {
    final StorageDirectory sd;    
    final long txId;
    private final File file;
    private ImageManager im;
    
    public FSImageFile(StorageDirectory sd, File file, long txId,
        ImageManager im) {
      assert txId >= 0 || txId == HdfsConstants.INVALID_TXID : "Invalid txid on "
          + file + ": " + txId;
      
      this.sd = sd;
      this.txId = txId;
      this.file = file;
      this.im = im;
    } 
    
    public File getFile() {
      return file;
    }
    
    public ImageManager getImageManager() {
      return im;
    }
    
    public boolean isLocal() {
      if (sd == null)
        return false;
      return NNStorage.isPreferred(StorageLocationType.LOCAL, sd);
    }

    public long getCheckpointTxId() {
      return txId;
    }
    
    @Override
    public String toString() {
      return String.format("FSImageFile(file=%s, cpktTxId=%019d)",
          (file != null ? file.toString() : " null "), txId);
    }
  }

}
