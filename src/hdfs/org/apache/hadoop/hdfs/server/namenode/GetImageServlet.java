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

import java.util.*;
import java.io.*;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataTransferThrottler;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is used in Namesystem's jetty to retrieve a file.
 * Typically used by the Secondary NameNode to retrieve image and
 * edit file for periodic checkpointing.
 */
public class GetImageServlet extends HttpServlet {
  private static final long serialVersionUID = -7669068179452648952L;
  
  public final static String CONTENT_DISPOSITION = "Content-Disposition";
  public final static String HADOOP_IMAGE_EDITS_HEADER = "X-Image-Edits-Name";

  private static final Log LOG = LogFactory.getLog(GetImageServlet.class);

  private static final String TXID_PARAM = "txid";
  private static final String START_TXID_PARAM = "startTxId";
  private static final String END_TXID_PARAM = "endTxId";
  private static final String STORAGEINFO_PARAM = "storageInfo";
  private static final String THROTTLE_PARAM = "disableThrottler";
  
  private static Set<Long> currentlyDownloadingCheckpoints =
    Collections.<Long>synchronizedSet(new HashSet<Long>());
  
  public void doGet(final HttpServletRequest request,
                    final HttpServletResponse response
                    ) throws ServletException, IOException {
    try {
      ServletContext context = getServletContext();
      final FSImage nnImage = (FSImage)context.getAttribute("name.system.image");
      final GetImageParams parsedParams = new GetImageParams(request, response);
      final Configuration conf = 
        (Configuration)getServletContext().getAttribute("name.conf");
      
      String myStorageInfoString = nnImage.storage.toColonSeparatedString();
      String theirStorageInfoString = parsedParams.getStorageInfoString();
      if (theirStorageInfoString != null &&
          !myStorageInfoString.equals(theirStorageInfoString)) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN,
            "This namenode has storage info " + myStorageInfoString + 
            " but the secondary expected " + theirStorageInfoString);
        LOG.warn("Received an invalid request file transfer request " +
            "from a secondary with storage info " + theirStorageInfoString);
        return;
      }
      
      if (parsedParams.isGetImage()) {
        long txid = parsedParams.getTxId();
        
        File imageFile = nnImage.storage.getFsImageName(StorageLocationType.LOCAL, txid);
        if (imageFile == null) {
          throw new IOException("Could not find image with txid " + txid);
        }
        setVerificationHeaders(response, imageFile);
        // send fsImage
        TransferFsImage.getFileServer(response.getOutputStream(), imageFile,
            getThrottler(conf, parsedParams.isThrottlerDisabled())); 
      } else if (parsedParams.isGetEdit()) {
        long startTxId = parsedParams.getStartTxId();
        long endTxId = parsedParams.getEndTxId();
        
        File editFile = nnImage.storage
            .findFinalizedEditsFile(startTxId, endTxId);
        
        setVerificationHeaders(response, editFile);
        
        // send edits
        TransferFsImage.getFileServer(response.getOutputStream(), editFile,
            getThrottler(conf, parsedParams.isThrottlerDisabled()));
      } else if (parsedParams.isPutImage()) {
        final long txid = parsedParams.getTxId();

        if (! currentlyDownloadingCheckpoints.add(txid)) {
          throw new IOException(
              "Another checkpointer is already in the process of uploading a" +
              " checkpoint made at transaction ID " + txid);
        }

        try {
          if (nnImage.storage.findImageFile(txid) != null) {
            throw new IOException(
                "Another checkpointer already uploaded an checkpoint " +
                "for txid " + txid);
          }
          
          MD5Hash downloadImageDigest = TransferFsImage.downloadImageToStorage(
                    parsedParams.getInfoServer(), txid,
                    nnImage, true);
        
          nnImage.checkpointUploadDone(txid, downloadImageDigest);
          
        } finally {
          currentlyDownloadingCheckpoints.remove(txid);
        }
      }
    } catch (Exception ie) {
      String errMsg = "GetImage failed. " + StringUtils.stringifyException(ie);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }
  
  /**
   * Construct a throttler from conf
   * @param conf configuration
   * @return a data transfer throttler
   */
  public static final DataTransferThrottler getThrottler(Configuration conf, 
      boolean disableThrottler) {
    if (disableThrottler) {
      return null;
    }
    long transferBandwidth = 
      conf.getLong(HdfsConstants.DFS_IMAGE_TRANSFER_RATE_KEY,
          HdfsConstants.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
    DataTransferThrottler throttler = null;
    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
    return throttler;
  }
  
  /**
   * Set headers for content length, and, if available, md5.
   * @throws IOException 
   */
  public static void setVerificationHeaders(HttpServletResponse response, File file)
  throws IOException {
    response.setHeader(TransferFsImage.CONTENT_LENGTH,
        String.valueOf(file.length()));
    MD5Hash hash = MD5FileUtils.readStoredMd5ForFile(file);
    if (hash != null) {
      response.setHeader(TransferFsImage.MD5_HEADER, hash.toString());
    }
  }
  
  public static void setFileNameHeaders(HttpServletResponse response,
      File file) {
    response.setHeader(CONTENT_DISPOSITION, "attachment; filename=" +
        file.getName());
    response.setHeader(HADOOP_IMAGE_EDITS_HEADER, file.getName());
  }

  static String getParamStringForImage(long txid,
      StorageInfo remoteStorageInfo, boolean throttle) {
    return "getimage=1&" + TXID_PARAM + "=" + txid
      + "&" + STORAGEINFO_PARAM + "=" +
      remoteStorageInfo.toColonSeparatedString()
      + "&" + THROTTLE_PARAM + "=" + throttle;
    
  }

  static String getParamStringForLog(RemoteEditLog log,
      StorageInfo remoteStorageInfo, boolean throttle) {
    return "getedit" + "=1&" + START_TXID_PARAM + "=" + log.getStartTxId()
        + "&" + END_TXID_PARAM + "=" + log.getEndTxId()
        + "&" + STORAGEINFO_PARAM + "=" +
          remoteStorageInfo.toColonSeparatedString()
        + "&" + THROTTLE_PARAM + "=" + throttle;
  }
  
  static String getParamStringToPutImage(long txid,
      String machine, int port, NNStorage storage) {
    
    return "putimage=1" +
      "&" + TXID_PARAM + "=" + txid +
      "&port=" + port +
      "&machine=" + machine
      + "&" + STORAGEINFO_PARAM + "=" +
      storage.toColonSeparatedString();
  }

  
  public static class GetImageParams {
    private boolean isGetImage;
    private boolean isGetEdit;
    private boolean isPutImage;
    private int remoteport;
    private String machineName;
    private long startTxId, endTxId, txId;
    private String storageInfoString;
    private boolean disableThrottler;

    /**
     * @param request the object from which this servlet reads the url contents
     * @param response the object into which this servlet writes the url contents
     * @throws IOException if the request is bad
     */
    public GetImageParams(HttpServletRequest request,
                          HttpServletResponse response
                           ) throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, String[]> pmap = request.getParameterMap();
      isGetImage = isGetEdit = isPutImage = false;
      remoteport = 0;
      machineName = null;
      disableThrottler = false;

      for (Map.Entry<String, String[]> entry : pmap.entrySet()) {
        String key = entry.getKey();
        String[] val = entry.getValue();
        if (key.equals("getimage")) { 
          isGetImage = true;
          txId = parseLongParam(request, TXID_PARAM);
        } else if (key.equals("getedit")) { 
          isGetEdit = true;
          startTxId = parseLongParam(request, START_TXID_PARAM);
          endTxId = parseLongParam(request, END_TXID_PARAM);
        } else if (key.equals("putimage")) { 
          isPutImage = true;
          txId = parseLongParam(request, TXID_PARAM);
        } else if (key.equals("port")) { 
          remoteport = new Integer(val[0]).intValue();
        } else if (key.equals("machine")) { 
          machineName = val[0];
        } else if (key.equals(STORAGEINFO_PARAM)) {
          storageInfoString = val[0];
        } else if (key.equals(THROTTLE_PARAM)) {
          disableThrottler = parseBooleanParam(request, THROTTLE_PARAM);
        }
      }

      int numGets = (isGetImage?1:0) + (isGetEdit?1:0);
      if ((numGets > 1) || (numGets == 0) && !isPutImage) {
        throw new IOException("Illegal parameters to TransferFsImage");
      }
    }

    public String getStorageInfoString() {
      return storageInfoString;
    }

    public long getTxId() {
      assert isGetImage || isPutImage;
      return txId;
    }
    
    public long getStartTxId() {
      assert isGetEdit;
      return startTxId;
    }
    
    public long getEndTxId() {
      assert isGetEdit;
      return endTxId;
    }

    boolean isGetEdit() {
      return isGetEdit;
    }

    public boolean isGetImage() {
      return isGetImage;
    }

    boolean isPutImage() {
      return isPutImage;
    }
    
    public boolean isThrottlerDisabled() {
      return disableThrottler;
    }
    
    String getInfoServer() throws IOException{
      if (machineName == null || remoteport == 0) {
        throw new IOException ("MachineName and port undefined");
      }
      return NetUtils.toIpPort(machineName, remoteport);
    }
    
    private static String getParam(HttpServletRequest request, String param)
        throws IOException {
      String paramStr = request.getParameter(param);
      if (paramStr == null) {
        throw new IOException("Invalid request has no " + param + " parameter");
      }
      return paramStr;
    }
    
    private static long parseLongParam(HttpServletRequest request, String param)
        throws IOException {
      // Parse the 'txid' parameter which indicates which image is to be
      // fetched.
      String paramStr = getParam(request, param);
      return Long.valueOf(paramStr);
    }
    
    private static boolean parseBooleanParam(HttpServletRequest request, String param)
        throws IOException {
      String paramStr = getParam(request, param);
      return Boolean.valueOf(paramStr);
    }
  }
}
