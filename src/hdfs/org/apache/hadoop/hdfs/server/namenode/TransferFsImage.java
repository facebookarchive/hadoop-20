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

import java.io.*;
import java.net.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.lang.Math;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.DataTransferThrottler;
import org.apache.hadoop.util.FlushableLogger;
import org.apache.hadoop.util.InjectionHandler;


/**
 * This class provides fetching a specified file from the NameNode.
 */
public class TransferFsImage implements FSConstants{
  
  // Image transfer timeout
  public static final String DFS_IMAGE_TRANSFER_TIMEOUT_KEY = "dfs.image.transfer.timeout";
  public static final int DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT = 60 * 1000;
  
  public final static String CONTENT_LENGTH = "Content-Length";
  public final static String MD5_HEADER = "X-MD5-Digest";

  private static final Log LOG = LogFactory.getLog(TransferFsImage.class);
  // immediate flush logger
  private static final Log FLOG = FlushableLogger.getLogger(LOG);

  private static final int BUFFER_SIZE = new Configuration().getInt(
      "image.transfer.buffer.size", 4 * 1024 * 1024);

  /**
   * Download image to local storage with throttling.
   */
  static MD5Hash downloadImageToStorage(String fsName, long imageTxId,
      FSImage fsImage, boolean needDigest) throws IOException {
    // by default do not disable throttling
    return downloadImageToStorage(fsName, imageTxId, fsImage, needDigest,
        false);
  }
  
  /**
   * Download image to local storage.
   * Throttling can be disabled.
   */
  static MD5Hash downloadImageToStorage(String fsName, long imageTxId,
      FSImage dstImage, boolean needDigest, boolean disableThrottle)
      throws IOException {

    String fileid = GetImageServlet.getParamStringForImage(
        imageTxId, dstImage.storage, disableThrottle);
    
    List<OutputStream> outputStreams = dstImage.getCheckpointImageOutputStreams(imageTxId);
    
    if (outputStreams.size() == 0) {
      throw new IOException("No targets in destination storage!");
    }
    
    MD5Hash hash = getFileClient(fsName, fileid, outputStreams, dstImage.storage, needDigest);
    LOG.info("Downloaded image files for txid: " + imageTxId);
    return hash;
  }
  
  static void downloadEditsToStorage(String fsName, RemoteEditLog log,
      NNStorage dstStorage) throws IOException {
    // by default do not disable throttling
    downloadEditsToStorage(fsName, log, dstStorage, false);
  }
  
  static void downloadEditsToStorage(String fsName, RemoteEditLog log,
      NNStorage dstStorage, boolean disableThrottle) throws IOException {
    assert log.getStartTxId() > 0 && log.getEndTxId() > 0 :
      "bad log: " + log;
    String fileid = GetImageServlet.getParamStringForLog(
        log, dstStorage, disableThrottle);
    String fileName = NNStorage.getFinalizedEditsFileName(
        log.getStartTxId(), log.getEndTxId());

    File[] dstFiles = dstStorage.getFiles(NameNodeDirType.EDITS, fileName);
    assert !(dstFiles.length == 0) : "No checkpoint targets.";
    
    for (File f : dstFiles) {
      if (f.exists() && f.canRead()) {
        LOG.info("Skipping download of remote edit log " +
            log + " since it already is stored locally at " + f);
        return;
      } else {
        LOG.debug("Dest file: " + f);
      }
    }
    List<OutputStream> outputStreams = ImageSet.convertFilesToStreams(dstFiles,
        dstStorage, fsName + fileid);
    getFileClient(fsName, fileid, outputStreams, dstStorage, false);
    LOG.info("Downloaded file " + dstFiles[0].getName() + " size " +
        dstFiles[0].length() + " bytes.");
  }
 
  /**
   * Requests that the NameNode download an image from this node.
   *
   * @param fsName the http address for the remote NN
   * @param imageListenAddress the host/port where the local node is running an
   *                           HTTPServer hosting GetImageServlet
   * @param storage the storage directory to transfer the image from
   * @param txid the transaction ID of the image to be uploaded
   */
  static void uploadImageFromStorage(String fsName,
      String machine, int port,
      NNStorage storage, long txid) throws IOException {
    
    String fileid = GetImageServlet.getParamStringToPutImage(
        txid, machine, port, storage);

    LOG.info("Image upload: Posted URL " + fsName + fileid);
    
    // this doesn't directly upload an image, but rather asks the NN
    // to connect back to the 2NN to download the specified image.
    TransferFsImage.getFileClient(fsName, fileid, null, storage, false);
    LOG.info("Uploaded image with txid " + txid + " to namenode at " +
        fsName);
  }

  
  /**
   * A server-side method to respond to a getfile http request
   * Copies the contents of the local file into the output stream.
   */
  public static void getFileServer(OutputStream outstream, File localfile,
      DataTransferThrottler throttler)
    throws IOException {
    byte buf[] = new byte[BUFFER_SIZE];
    InputStream infile = null;
    try {
      infile = new BufferedInputStream(new FileInputStream(localfile), BUFFER_SIZE);
      if (InjectionHandler.falseCondition(InjectionEvent.TRANSFERFSIMAGE_GETFILESERVER0)
          && localfile.getAbsolutePath().contains("secondary")) {
        // throw exception only when the secondary sends its image
        throw new IOException("If this exception is not caught by the " +
            "name-node fs image will be truncated.");
      }
      
      if (InjectionHandler.falseCondition(InjectionEvent.TRANSFERFSIMAGE_GETFILESERVER1)
          && localfile.getAbsolutePath().contains("fsimage")) {
          // Test sending image shorter than localfile
          long len = localfile.length();
          buf = new byte[(int)Math.min(len/2, BUFFER_SIZE)];
          // This will read at most half of the image
          // and the rest of the image will be sent over the wire
          infile.read(buf);
      }
      int num = 1;
      while (num > 0) {
        num = infile.read(buf);
        if (num <= 0) {
          break;
        }

        if (InjectionHandler.falseCondition(InjectionEvent.TRANSFERFSIMAGE_GETFILESERVER2)) {
          // Simulate a corrupted byte on the wire
          LOG.warn("SIMULATING A CORRUPT BYTE IN IMAGE TRANSFER!");
          buf[0]++;
        }
        InjectionHandler.processEvent(InjectionEvent.TRANSFERFSIMAGE_GETFILESERVER3);
        outstream.write(buf, 0, num);
        if (throttler != null) {
          throttler.throttle(num);
        }
      }
    } finally {
      if (infile != null) {
        infile.close();
      }
    }
  }
  
  /**
   * A server-side method to respond to a getfile http request
   * Copies the contents of the local file into the output stream,
   * starting at the given position, sending lengthToSend bytes.
   */
  public static void getFileServerForPartialFiles(OutputStream outstream,
      String filename, InputStream infile, DataTransferThrottler throttler,
      long startPosition, long lengthToSend) throws IOException {
    byte buf[] = new byte[BUFFER_SIZE];
    try {
      int num = 1;
      while (num > 0) {
        num = infile.read(buf, 0,
            Math.min(BUFFER_SIZE,
                (int) Math.min(lengthToSend, Integer.MAX_VALUE)));
        lengthToSend -= num;
        if (num <= 0) {
          break;
        }
        try {
          outstream.write(buf, 0, num);
        } catch (Exception e) {
          // silently ignore. connection might have been closed
          break;
        }
        if (throttler != null) {
          throttler.throttle(num);
        }
      }
      if (lengthToSend > 0) {
        LOG.warn("Could not serve requested number of bytes. Left with "
            + lengthToSend + " bytes for file: " + filename);
      }
    } finally {
      if (infile != null) {
        infile.close();
      }
    }
  }
  
  /**
   * Get connection and read timeout.
   */
  private static int getHttpTimeout(Storage st) {
    if (!(st instanceof NNStorage))
      return DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT; 
    NNStorage storage = (NNStorage) st;
    if (storage == null || storage.getConf() == null) {
      return DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT;
    }
    return storage.getConf().getInt(DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
        DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT);
  }
  
  /**
   * Client-side Method to fetch file from a server
   * Copies the response from the URL to a list of local files.
   * @param dstStorage if an error occurs writing to one of the files,
   *                   this storage object will be notified. 
   * @Return a digest of the received file if getChecksum is true
   */
  static MD5Hash getFileClient(String nnHostPort,
      String queryString, List<OutputStream> outputStreams,
      Storage dstStorage, boolean getChecksum) throws IOException {

    String proto = "http://";
    StringBuilder str = new StringBuilder(proto+nnHostPort+"/getimage?");
    str.append(queryString);
    LOG.info("Opening connection to " + str);
    //
    // open connection to remote server
    //
    URL url = new URL(str.toString());
    return doGetUrl(url, outputStreams, dstStorage, getChecksum);
  }

  /**
   * Client-side Method to fetch file from a server
   * Copies the response from the URL to a list of local files.
   * @param dstStorage if an error occurs writing to one of the files,
   *                   this storage object will be notified. 
   * @Return a digest of the received file if getChecksum is true
   */
  public static MD5Hash doGetUrl(URL url, List<OutputStream> outputStreams,
      Storage dstStorage, boolean getChecksum) throws IOException {
    byte[] buf = new byte[BUFFER_SIZE];
    String str = url.toString();
    
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    
    int timeout = getHttpTimeout(dstStorage);
    connection.setConnectTimeout(timeout);
    
    if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(
          "Image transfer servlet at " + url +
          " failed with status code " + connection.getResponseCode() +
          "\nResponse message:\n" + connection.getResponseMessage());
    }
    
    long advertisedSize;
    String contentLength = connection.getHeaderField(CONTENT_LENGTH);
    if (contentLength != null) {
      advertisedSize = Long.parseLong(contentLength);
    } else {
      throw new IOException(CONTENT_LENGTH + " header is not provided " +
                            "by the namenode when trying to fetch " + str);
    }
    
    MD5Hash advertisedDigest = parseMD5Header(connection);
    long received = 0;
    InputStream stream = connection.getInputStream();
    MessageDigest digester = null;
    if (getChecksum) {
      digester = MD5Hash.getDigester();
      stream = new DigestInputStream(stream, digester);
    }
    boolean finishedReceiving = false;
    MD5Hash computedDigest = null;
    
    try {
      int num = 1;
      int lastPrintedProgress = 0;
      while (num > 0) {
        num = stream.read(buf);
        if (num > 0) {
          received += num;
          for (OutputStream fos : outputStreams) {
            fos.write(buf, 0, num);
          }
          lastPrintedProgress = printProgress(str, received, advertisedSize,
              lastPrintedProgress);
        }
      }
      if(digester != null)
        computedDigest = new MD5Hash(digester.digest());
      finishedReceiving = true;
    } finally {
      stream.close();
      if (outputStreams != null) {
        for (OutputStream os : outputStreams) {
          if (os instanceof FileOutputStream)
            ((FileOutputStream)os).getChannel().force(true);
          os.close();
        }
      }
      if (finishedReceiving && received != advertisedSize) {
        // only throw this exception if we think we read all of it on our end
        // -- otherwise a client-side IOException would be masked by this
        // exception that makes it look like a server-side problem!
        throw new IOException("File " + str + " received length " + received +
                              " is not of the advertised size " +
                              advertisedSize);
      }
    }
    
    if (computedDigest != null) {
      if (advertisedDigest != null &&
          !computedDigest.equals(advertisedDigest)) {
        throw new IOException("File " + str + " computed digest " +
            computedDigest + " does not match advertised digest " + 
            advertisedDigest);
      }
      return computedDigest;
    } else {
      return null;
    }    
  }
  
  /**
   * Print progress when downloading files. 
   */
  private static int printProgress(String str, long received,
      long advertisedSize, int lastPrinted) {
    if (advertisedSize == 0)
      return 0;
    int currentPercent = (int) ((received * 100) / advertisedSize);
    if (currentPercent != lastPrinted)
      FLOG.info("Downloading: " + str + ", completed: " + currentPercent);
    return currentPercent;
  }

  public static MD5Hash parseMD5Header(HttpURLConnection connection) {
    String header = connection.getHeaderField(MD5_HEADER);
    MD5Hash hash = (header != null) ? new MD5Hash(header) : null;
    return hash;
  }

}
