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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.io.MD5Hash;

/**
 * ImageManager provides an abstraction for a single image location. The
 * location can be local storage, as well as custom remote implementations for
 * storing images. For now, this class provides only utilities for manipulating
 * with checkpoint (i.e., obtaining checkpoint streams, saving digests).
 */
public interface ImageManager extends FormatConfirmable {

  /**
   * Transition the underlying storage
   */
  void transitionImage(StorageInfo si, Transition transition,
      StartupOption startOpt)
      throws IOException;

  /**
   * Get an output stream for this location, for image with given txid.
   */
  public OutputStream getCheckpointOutputStream(long txid) throws IOException;

  /**
   * Finalize checkpointed image, by renaming it, and saving the image digest.
   */
  public boolean saveDigestAndRenameCheckpointImage(long txid, MD5Hash digest);

  /**
   * Is the image manager disabled
   */
  public boolean isImageDisabled();
  
  /**
   * Set status for the image.
   */
  public void setImageDisabled(boolean isDisabled);
  
  /**
   * Return a manifest of what images are available. 
   * 
   * @param fromTxId Starting transaction id for the requested images.
   * @return RemoteEditLogManifest object.
   */
  public RemoteImageManifest getImageManifest(long fromTxId) throws IOException; 
  
  /**
   * Get latest image for this manager
   */
  public FSImageFile getLatestImage() throws IOException;
  
  /**
   * Get input stream for the image of given txid.
   */
  public ImageInputStream getImageInputStream(long txid) throws IOException;

  /**
   * Analyze image storage.
   */
  RemoteStorageState analyzeImageStorage() throws IOException;

  /**
   * Pretty string representation.
   */
  public String toHTMLString();
  
  /**
   * Get URI of this location.
   */
  public URI getURI() throws IOException;
}
