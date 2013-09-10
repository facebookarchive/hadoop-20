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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;

/**
 * FileImageManager provides abstraction for image location in file storage.
 */
public class FileImageManager implements ImageManager {
  
  static final Log LOG = LogFactory.getLog(FileImageManager.class);
  
  private final StorageDirectory sd;
  private final Storage storage;
  private volatile boolean imageDisabled = false;
  
  public FileImageManager(StorageDirectory sd, Storage storage) {
    this.sd = sd;
    this.storage = storage;
  }

  /**
   * Get file output stream
   */
  @Override
  public OutputStream getCheckpointOutputStream(long imageTxId) throws IOException {
    String fileName = NNStorage.getCheckpointImageFileName(imageTxId);
    return new FileOutputStream(new File(sd.getCurrentDir(), fileName));
  }

  @Override
  public boolean saveDigestAndRenameCheckpointImage(long txid, MD5Hash digest) {
    try {
      renameCheckpointInDir(sd, txid);
      // when saving image directly, we save the digest on-the-fly
      File imageFile = NNStorage.getImageFile(sd, txid);
      MD5FileUtils.saveMD5File(imageFile, digest);
      return true;
    } catch (IOException ioe) {
      LOG.warn("Unable to rename checkpoint in " + sd, ioe);
      reportError(sd);
      return false;
    }
  }
  
  /**
   * Rolls checkpointed image.
   */
  private static void renameCheckpointInDir(StorageDirectory sd, long txid)
      throws IOException {
    File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
    File curFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, txid);
    // renameTo fails on Windows if the destination file 
    // already exists.
    LOG.info("Renaming  " + ckpt.getAbsolutePath() + " to "
        + curFile.getAbsolutePath());
    if (!ckpt.renameTo(curFile)) {
      if (!curFile.delete() || !ckpt.renameTo(curFile)) {
        throw new IOException("renaming  " + ckpt.getAbsolutePath() + " to "  + 
            curFile.getAbsolutePath() + " FAILED");
      }
    }    
  }
  
  /**
   * Return true if the last image
   */
  @Override
  public boolean isImageDisabled() {
    return imageDisabled;
  }
  
  /**
   * Set image status.
   */
  @Override
  public void setImageDisabled(boolean isDisabled) {
    this.imageDisabled = isDisabled;
  }
  
  /**
   * Reports error to underlying storage.
   */
  private void reportError(StorageDirectory sd) {
    if (storage instanceof NNStorage) {
      // pass null, since we handle the disable here
      ((NNStorage)storage).reportErrorsOnDirectory(sd, null);
    } else {
      LOG.error("Failed direcory: " + sd.getCurrentDir());
    }
  }
  
  public String toString() {
    return sd.getCurrentDir().toString();
  }

  @Override
  public ImageInputStream getImageInputStream(long txid) throws IOException {
    File imageFile = NNStorage.getImageFile(sd, txid);
    return new ImageInputStream(txid, new FileInputStream(imageFile),
        MD5FileUtils.readStoredMd5ForFile(imageFile), imageFile.toString(),
        imageFile.length());
  }
  
  @Override
  public boolean hasSomeJournalData() throws IOException {
    return sd.hasSomeJournalData();
  }

  @Override
  public boolean hasSomeImageData() throws IOException {
    return sd.hasSomeImageData();
  }

  @Override
  public RemoteImageManifest getImageManifest(long fromTxId) throws IOException {
    throw new UnsupportedOperationException("This operation is not supported.");
  }

  @Override
  public FSImageFile getLatestImage() throws IOException {
    throw new UnsupportedOperationException("This operation is not supported.");
  }
  
  @Override
  public void transitionImage(StorageInfo si, Transition transition,
      StartupOption startOpt) throws IOException {
    throw new UnsupportedOperationException("This operation is not supported.");
  }

  @Override
  public RemoteStorageState analyzeImageStorage() throws IOException {
    throw new UnsupportedOperationException("This operation is not supported.");
  }

  @Override
  public String toHTMLString() {
    return String.format("file:/%s", sd.getRoot());
  }

  @Override
  public URI getURI() throws IOException {
    return Util.fileAsURI(sd.getRoot());
  }
  
  public StorageDirectory getStorageDirectory() {
    return sd;
  }
}
