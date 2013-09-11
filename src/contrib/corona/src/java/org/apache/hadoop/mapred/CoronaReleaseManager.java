/*
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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;


/**
 * Used to manage the corona releases
 */
public class CoronaReleaseManager extends Thread {
  /** the release dir to copy from */
  public static final String MAPRED_RELEASE_DIR = "mapred.release.dir";
  /** the dir that holds all the copies of the release */
  public static final String MAP_RELEASE_WORKING_DIR =
    "mapred.release.working.dir";
  /** the interval that the thread will wake up to check the releases */
  public static final String RELEASE_DIR_CLEAN_INTERVAL =
    "mapred.release.dir.cleanInterval";
  /** the threshold that old releases will be removed */
  public static final String RELEASE_DIR_CLEAN_THRESHOLD =
    "mapred.release.dir.cleanThreshold";
  /** the pattern that decides if a file in the release dir shall be copied */
  public static final String RELEASE_COPY_PATTERN =
    "mapred.release.file.pattern";
  /** if this one is specified, only this file is used to
   *  check if there is a new release
   */
  public static final String CORONA_RELEASE_FILE_CHECK =
    "mapred.release.file.check";
  /** Right after a release is copied, a tag file will be created to specify
   *  that the copy is complete. When TT is restarted, if the tag file exists
   *  for the latest release copy, no more copy is needed. When a JT gets a
   *  release, the modification time of this tag file will be changed. So
   *  thethread will know when to clean up.
   */
  public static final String RELEASE_TAG_FILE = "RELEASE_COPY_DONE";
  private static final Log LOG = LogFactory.getLog(CoronaReleaseManager.class);
 
  /** releaseDir is the original string for the release dir.
   *  The directory retrieved from releasePath may not be the same as
   *  the one specified in the classpath, for example .././
   */
  private String releaseDir;
  private Path releasePath;
  private Path workingPath;
  private long cleanThreshold;
  private long cleanInterval;
  private Pattern release_pattern;
  /** if CORONA_RELEASE_FILE_CHECK is set, only this file is used to
   *  check if there is a new release
   */
  private String coronaReleaseFileCheck;

  private final Configuration conf;
  private FileSystem fs;
  private SimpleDateFormat formatter;
  private LinkedList<CoronaRelease> releaseList;
  private boolean shutdownFlag = false;

  class CoronaRelease {
    Path copiedPath;
    long releaseTimestamp;
    LinkedList<JobID> jobids;
    boolean latest;
    byte[] fingerPrint;

    CoronaRelease(Path copiedPath, long releaseTimestamp, JobID jobid, 
                           byte[] fingerPrint) {
      this.copiedPath = copiedPath;
      this.releaseTimestamp = releaseTimestamp;
      this.latest = true;
      jobids = new LinkedList<JobID>();
      if (jobid != null) {
        jobids.add(jobid);
      }
      this.fingerPrint = fingerPrint;
    }
    
    boolean checkIntegrity(byte[] fp) {
      if (fingerPrint == null ||
          fp == null ||
          fingerPrint.length != fp.length) {
        return false;
      }
      
      for (int i = 0; i < fp.length; ++ i) {
        if (fingerPrint[i] != fp[i]) {
          return false;
        }
      }
      
      return true;
    }
  }

  public CoronaReleaseManager(Configuration conf) throws IOException {
    this.conf = conf;
    fs = FileSystem.newInstanceLocal(conf);
    releaseDir = conf.get(MAPRED_RELEASE_DIR, "");
    coronaReleaseFileCheck = conf.get(CORONA_RELEASE_FILE_CHECK, "");
    String workingDir = conf.get(MAP_RELEASE_WORKING_DIR, "");
    cleanInterval = conf.getLong(RELEASE_DIR_CLEAN_INTERVAL, 300000L);
    cleanThreshold = conf.getLong(RELEASE_DIR_CLEAN_THRESHOLD, 172800000L);
    String patternString = conf.get(RELEASE_COPY_PATTERN, "");
    if (patternString.isEmpty()) {
      release_pattern = null;
    } else {
      release_pattern = Pattern.compile(patternString);
    }
    releaseList = new LinkedList<CoronaRelease>();
    if (!releaseDir.isEmpty()) {
      releasePath = new Path(releaseDir);
    }
    if (!workingDir.isEmpty()) {
      workingPath = new Path(workingDir);
    }
    formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
  }

  @Override
  public void run() {
    if (releasePath == null || workingPath == null) {
      LOG.error("The releaseDir or workingDir is empty, exiting...");
      return;
    }
    while (!shutdownFlag) {
      // check if there is a new release to copy
      getRelease(null);

      // check if old releases can be de-referenced
      checkOldRelease();

      // check if there is any old dirs to cleanup
      try {
        long currentTime = System.currentTimeMillis();
        for (FileStatus dirStat: fs.listStatus(workingPath)) {
          if (!dirStat.isDir()) {
            continue;
          }
          // If the dir 's tag file is modifies/accessed 2 days ago,
          // and if it is not in the release list
          Path dirPath = dirStat.getPath();
          Path tagPath = new Path(dirPath, RELEASE_TAG_FILE);
          try {
            if (!fs.exists(tagPath)) {
              LOG.info("Tag " + tagPath + " is missing, removing " + dirPath);
              removeRelease(dirPath);
            } else {
              FileStatus fileStat = fs.getFileStatus(tagPath);
              if (currentTime - fileStat.getModificationTime() > cleanThreshold &&
                  checkPath(dirPath)) {
                // recursively delete all the files/dirs
                LOG.info("Remove old release " + dirPath);
                removeRelease(dirPath);
              }
            }
          } catch (IOException e) {
            LOG.error("Error in checking " + tagPath, e);
          }
        }
      } catch (IOException ioe) {
        LOG.error("IOException when clearing dir ", ioe);
      }
      try {
        Thread.sleep(cleanInterval);
      } catch (InterruptedException e) {
      }
    }
  }

  public void shutdown() {
    shutdownFlag = true;
  }

  public void returnRelease(JobID jobid) {
    synchronized (releaseList) {
      Iterator<CoronaRelease> crIt = releaseList.iterator();
      // A single jobid may have used multiple releases
      while (crIt.hasNext()) {
        CoronaRelease cr = crIt.next();
        Iterator<JobID> jobIdIt = cr.jobids.iterator();
        while (jobIdIt.hasNext()) {
          JobID existingJobID = jobIdIt.next();
          if (existingJobID.toString().equals(jobid.toString())) {
            jobIdIt.remove();
            LOG.info("Return release " + cr.copiedPath + " for " + jobid);
            return;
          }
        }
      }
    }
  }

  public String getRelease(JobID jobid) {
    if (releasePath == null || workingPath == null) {
      LOG.error("The releaseDir or workingDir is empty");
      return null;
    }
    try {
      if (!fs.exists(releasePath)) {
        LOG.info(releasePath + " is not existing");
        return null;
      }
    } catch (IOException e) {
      LOG.error("IOException in checking " + releasePath, e);
      return null;
    }

    long currentTimeStamp = getLastTimeStamp();
    CoronaRelease curCR = null;
    synchronized (releaseList) {
      // check if the most current one loaded
      for (CoronaRelease cr: releaseList) {
        if (cr.releaseTimestamp == currentTimeStamp) {
          if (jobid != null) {
            // update the timestamp
            Path donePath = new Path(cr.copiedPath, RELEASE_TAG_FILE);
            try {
              FSDataOutputStream fos = fs.create(donePath);
              fos.close();
            } catch (IOException e) {
              LOG.error("Unable to recreate " + donePath);
              return null;
            }
            cr.jobids.add(jobid);
            LOG.info("Get existing release " + cr.copiedPath +
              " for " + jobid);
          }
          // check the finger print of copied path in case some body
          // delete the file in it
          byte [] fingerPrint = getFingerPrint(cr.copiedPath);
          if (fingerPrint == null) {
            LOG.error("Unable to get the finger print " + cr.copiedPath);
            return null;
          }
          if (!cr.checkIntegrity(fingerPrint)) {
            LOG.error("The finger print of " + cr.copiedPath +
                " is not correct.");
            curCR = cr;
            break;
          }
          return cr.copiedPath.toString();
        }
      }
      // copied the most current release
      Path newWorkingPath = new Path(workingPath, 
        formatter.format(currentTimeStamp));
      LOG.info("Copy the latest release to " + newWorkingPath);
      if (copyRelease(releasePath, newWorkingPath, true, true)) {
        byte [] fingerPrint = getFingerPrint(newWorkingPath);
        if (fingerPrint == null) {
          LOG.error("Unable to get the finger print " + newWorkingPath);
          return null;
        }
        
        if (curCR != null) {
          curCR.fingerPrint = fingerPrint;
          return curCR.copiedPath.toString();
        }
        
        CoronaRelease cr = new CoronaRelease(newWorkingPath,
          currentTimeStamp, jobid, fingerPrint);
        for (CoronaRelease tmpcr: releaseList) {
          tmpcr.latest = false;
        }
        releaseList.add(cr);
        LOG.info("Done with copying the latest release to " +
          newWorkingPath);
        if (jobid != null) {
          LOG.info("copied the latest release " + newWorkingPath +
            " for " + jobid);
        }
        return newWorkingPath.toString();
      } else {
        LOG.error("Failed to copy the latest release to " + newWorkingPath);
        if (jobid != null) {
          LOG.error("Unable to get any release for " + jobid);
        }
        return null;
      }
    }
  }

  private void checkOldRelease() {
    synchronized (releaseList) {
      // check if there is any old one to remove
      Iterator<CoronaRelease> crIt = releaseList.iterator();
      while (crIt.hasNext()) {
        CoronaRelease cr = crIt.next();
        if (cr.jobids.size() == 0 && !cr.latest) {
          crIt.remove();
          LOG.info("Remove " + cr.copiedPath + " from release list");
        }
      }
    }
  }

  public String getOriginal() {
    return releaseDir;
  }

  private boolean checkPath(Path inPath) {
    synchronized (releaseList) {
      Iterator<CoronaRelease> crIt = releaseList.iterator();
      while (crIt.hasNext()) {
        CoronaRelease cr = crIt.next();
        if (cr.copiedPath.toString().equals(inPath.toUri().getPath().toString())) {
          return false;
        }
      }
    }
    return true;
  }

  /** getLastStamp will go throught all the files and directories in the release
   *  directory, and find the largest timestamp. This is used to check if there
   *  is any new release. RELEASE_COPY_PATTERN and CORONA_RELEASE_FILE_CHECK can
   *  be used to limit the files checked
   */
  private long getLastTimeStamp() {
    long result = -1;
    if (coronaReleaseFileCheck != null && !coronaReleaseFileCheck.isEmpty()) {
      result = getLastTimeStamp(new Path(releasePath, coronaReleaseFileCheck));
      if (result > 0) {
        return result;
      }
    }
    return getLastTimeStamp(releasePath);
  }

  /**
   * Get the release directory's latest timestamp
   */
  private long getLastTimeStamp(Path pathToCheck) {
    long lastTimeStamp = -1;
    long tmpTimeStamp = -1;
    try {
      for (FileStatus fileStat: fs.listStatus(pathToCheck)) {
        Path srcPath = fileStat.getPath();
        if (!fileStat.isDir()) {
          boolean checkFlag = true;
          if (release_pattern != null) {
            // just need to check the files that match the pattern
            Matcher m = release_pattern.matcher(srcPath.toString());
            if (!m.find()) {
              checkFlag = false;
            } 
          }
          if (checkFlag) {
            tmpTimeStamp = fileStat.getModificationTime();
          } else {
            continue;
          }
        } else {
          tmpTimeStamp = getLastTimeStamp(srcPath);
        }
        if (tmpTimeStamp > lastTimeStamp) {
          lastTimeStamp = tmpTimeStamp;
        }
      }
    } catch (IOException ioe) {
      LOG.error("IOException when checking timestamp ", ioe);
    }
    return lastTimeStamp;
  }

  /** For every jar files from the source, create a link in the dest
   */
  private boolean copyRelease(Path src, Path dest, boolean isTop, boolean isForced) {
    try {

      if (!fs.exists(dest)) {
        if (!fs.mkdirs(dest)) {
          LOG.error("Unable to make dir " + dest.toString());
          return false;
        }
      } else {
        if (isTop && !isForced) {
          Path donePath = new Path(dest, RELEASE_TAG_FILE);
          if (fs.exists(donePath)) {
            LOG.info(donePath + " exists. There is no need to copy again");
            return true;
          }
        }
      }
      for (FileStatus fileStat: fs.listStatus(src)) {
        Path srcPath = fileStat.getPath();
        if (!fileStat.isDir()) {
          boolean copyFlag = true;
          if (release_pattern != null) {
            Matcher m = release_pattern.matcher(srcPath.toString());
            if (!m.find()) {
              copyFlag = false;
            }
          }
          if (copyFlag) {
            Path destPath = new Path(dest, srcPath.getName());
            fs.copyFromLocalFile(srcPath, destPath);
          }
        } else {
          Path destPath = new Path(dest, srcPath.getName());
          if (!copyRelease(srcPath, destPath, false, isForced)) {
            LOG.error("Unable to create link for " + srcPath.toString() +
              " as " + destPath.toString());
            return false;
          }
        }
      }
      if (isTop) {
        // create the tag file
        Path donePath = new Path(dest, RELEASE_TAG_FILE);
        FSDataOutputStream fos = fs.create(donePath);
        fos.close();
      }
    } catch (IOException ioe) {
      LOG.error("IOException when link dir ", ioe);
      return false;
    }
    return true;
  }

  private boolean removeRelease(Path pathToRemove) {
    try {
      fs.delete(pathToRemove, true);
    } catch (IOException ioe) {
      LOG.error("IOException when remove release " +
        pathToRemove.toString(), ioe);
      return false;
    }
    return true;
  }
  
  private byte [] getFingerPrint(Path path)
  {
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
      
      long currentTime = System.currentTimeMillis();
      computeFingerPrint("", path, messageDigest);
      long endTime = System.currentTimeMillis();
      LOG.info((endTime-currentTime) + " ms spent to get finger print");
      
      return messageDigest.digest();
    } catch (NoSuchAlgorithmException e) {
      return null;
    }
  }
  
  private void computeFingerPrint(String parent, Path path, MessageDigest messageDigest) {
    try {
      for (FileStatus fileStat: fs.listStatus(path)) {
        Path srcPath = fileStat.getPath();
        if (!fileStat.isDir()) {
          String fileName = srcPath.getName();
          if (!fileName.equals(RELEASE_TAG_FILE)) {
            String finger = parent + fileName + fileStat.getModificationTime();
            messageDigest.update(finger.getBytes("UTF-8"));
          }
        } else {
          computeFingerPrint(parent + srcPath.getName(), srcPath, messageDigest);
        }
      }
    } catch (IOException ioe) {
      LOG.error("IOException when compute finger print " +
          path.toString(), ioe);
    }
  }
}
