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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.io.MD5Hash;

/**
 * ImageSet provides an abstraction for all image locations on the namenode. The
 * image managers can be local (FileImageManager), or some other locations
 * (e.g., QJM, which is implemented by QuorumJournalManager). This set provides
 * basic functionality for checkpointing (i.e., establishing write streams,
 * saving md5 hash).
 */
public class ImageSet {

  static final Log LOG = LogFactory.getLog(ImageSet.class);

  private final List<ImageManager> imageManagers;
  
  private final NameNodeMetrics metrics;

  public ImageSet(FSImage fsImage, Collection<URI> fsDirs,
      Collection<URI> fsEditsDirs, NameNodeMetrics metrics) throws IOException {
    this.imageManagers = new ArrayList<ImageManager>();
    this.metrics = metrics;

    // get all IMAGE directories
    Iterator<StorageDirectory> it = fsImage.storage
        .dirIterator(NameNodeDirType.IMAGE);
    while (it.hasNext()) {
      StorageDirectory sd = it.next();
      validate(sd.getRoot(), fsDirs);
      imageManagers.add(new FileImageManager(sd, fsImage.storage));
    }
    
    // add all journal managers that store images
    List<JournalManager> nonFileJournalManagers = fsImage.editLog.getNonFileJournalManagers();
    for (JournalManager jm : nonFileJournalManagers) {
      if (jm instanceof ImageManager && jm.hasImageStorage()) {
        ImageManager im = (ImageManager) jm;
        validate(im.getURI(), fsDirs);
        imageManagers.add(im);
      }
    }
    
    // initialize metrics
    updateImageMetrics();
  }
  
  /**
   * For sanity checking that the given storage directory was configured.
   */
  private void validate(File root, Collection<URI> dirs) throws IOException {
    if (dirs == null)
      return;
    for (URI dir : dirs) {
      if (new File(dir.getPath()).getAbsolutePath().equals(
          root.getAbsolutePath())) {
        // we found the corresponding entry
        return;
      }
    }
    throwIOException("Error. Storage directory: " + root
        + " is not in the configured list of storage directories: " + dirs);
  }

  /**
   * For sanity checking that the given location was configured.
   */
  private void validate(URI location, Collection<URI> dirs) throws IOException {
    if (dirs != null && !dirs.contains(location)) {
      throwIOException("Error. Location: " + location
          + " is not in the configured list of storage directories: " + dirs);
    }
  }

  /**
   * Get the list of output streams for all underlying image managers, given the
   * checkpoint transaction id.
   */
  public synchronized List<OutputStream> getCheckpointImageOutputStreams(
      long imageTxId) throws IOException {
    List<OutputStream> list = new ArrayList<OutputStream>();
    for (ImageManager im : imageManagers) {
      list.add(im.getCheckpointOutputStream(imageTxId));
    }
    return list;
  }

  /**
   * Save digest in all underlying image managers, and rename the checkpoint
   * file. This will throw an exception if all image managers fail.
   */
  public synchronized void saveDigestAndRenameCheckpointImage(long txid,
      MD5Hash digest) throws IOException {
    for (ImageManager im : imageManagers) {
      if (im.saveDigestAndRenameCheckpointImage(txid, digest)) {
        // restore enabled state
        im.setImageDisabled(false);
      } else {
        // failed image
        im.setImageDisabled(true);
      }
    }
    checkImageManagers();
  }
  
  /**
   * Check if any image managers are available.
   */
  void checkImageManagers() throws IOException {
    updateImageMetrics();
    int numAvailable = 0;
    for (ImageManager im : imageManagers) {
      if (!im.isImageDisabled()) {
        numAvailable++;
      }
    }
    if (numAvailable == 0) {
      throwIOException("No image locations are available");
    }
  }
  
  void restoreImageManagers() {
    for (ImageManager im : imageManagers) {
      im.setImageDisabled(false);
    }
    updateImageMetrics();
  }
  
  /**
   * Count available image managers and update namenode metrics.
   */
  void updateImageMetrics() {
    if (metrics == null) {
      return;
    }
    int failedImageDirs = 0;
    for (ImageManager im : imageManagers) {
      if(im.isImageDisabled()) {
        failedImageDirs++;
      }
    }
    // update only images, journals are handled in JournalSet
    metrics.imagesFailed.set(failedImageDirs);
  }
  
  /**
   * Get the latest image from all non-file image managers.
   */
  public FSImageFile getLatestImageFromNonFileImageManagers()
      throws IOException {
    FSImageFile latestImage = null;
    for (ImageManager im : imageManagers) {
      if (!(im instanceof FileImageManager)) {
        FSImageFile current = im.getLatestImage();
        if (latestImage == null
            || latestImage.getCheckpointTxId() < current.getCheckpointTxId()) {
          latestImage = current;
        }
      }
    }
    return latestImage;
  }

  /**
   * Format the non-file images.
   */
  public void transitionNonFileImages(StorageInfo nsInfo, boolean checkEmpty,
      Transition transition, StartupOption startOpt)
      throws IOException {
    for (ImageManager im : imageManagers) {
      if (!(im instanceof FileImageManager)) {
        if (checkEmpty && im.hasSomeImageData()) {
          LOG.warn("Image " + im + " is not empty.");
          continue;
        }
        LOG.info(transition + " : " + im);
        im.transitionImage(nsInfo, transition, startOpt);
      }
    }
  }

  /**
   * Get the list of image managers
   */
  public List<ImageManager> getImageManagers() {
    return imageManagers;
  }
  
  /**
   * Get the list of non-file image managers.
   */
  List<ImageManager> getNonFileImageManagers() {
    List<ImageManager> nonFile = new ArrayList<ImageManager>();
    for (ImageManager im : imageManagers) {
      if (!(im instanceof FileImageManager)) {
        nonFile.add(im);
      }
    }
    return nonFile;
  }
  
  /**
   * Return the number of image managers in this set
   */
  public int getNumImageManagers() {
    return imageManagers.size();
  }

  /**
   * Convert given list of files to a list of output streams.
   */
  public static List<OutputStream> convertFilesToStreams(File[] localPaths,
      Storage dstStorage, String str) throws IOException {
    List<OutputStream> outputStreams = new ArrayList<OutputStream>();
    if (localPaths != null) {
      for (File f : localPaths) {
        try {
          if (f.exists()) {
            LOG.warn("Overwriting existing file " + f
                + " with file downloaded form " + str);
          }
          outputStreams.add(new FileOutputStream(f));
        } catch (IOException ioe) {
          LOG.warn("Unable to download file " + f, ioe);
          if (dstStorage != null
              && (dstStorage instanceof StorageErrorReporter)) {
            ((StorageErrorReporter) dstStorage).reportErrorOnFile(f);
          }
        }
      }

      if (outputStreams.isEmpty()) {
        throw new IOException("Unable to download to any storage directory");
      }
    }
    return outputStreams;
  }
  
  private void throwIOException(String msg) throws IOException {
    LOG.error(msg);
    throw new IOException(msg);
  }
  
  void reportErrorsOnImageManager(StorageDirectory badSD) {
    try {
      for (ImageManager im : imageManagers) {
        if (Util.fileAsURI(badSD.getRoot()).equals(im.getURI())) {
          im.setImageDisabled(true);
        }
      }
    } catch (IOException e) {
      LOG.info("Error when reporting problems with : " + badSD.getRoot());
    }
  }
}
