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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/** Filesystem disk space usage statistics.  Uses the unix 'du' program*/
public class DU {
  private String  dirPath;
  private volatile boolean shouldRun = true;
  private Thread refreshUsed;
  private long refreshInterval;
  private final ConcurrentMap<Integer, NamespaceSliceDU> namespaceSliceDUMap;
  public static final Log LOG = LogFactory.getLog(DU.class);

  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param interval refresh the disk usage at this interval
   * @throws IOException if we fail to refresh the disk usage
   */
  public DU(File path, long interval) throws IOException {
    //we set the Shell interval to 0 so it will always run our command
    //and use this one to set the thread sleep interval
    this.refreshInterval = interval;
    this.dirPath = path.getCanonicalPath();
    this.namespaceSliceDUMap =
        new ConcurrentHashMap<Integer, NamespaceSliceDU>();
  }
  
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param conf configuration object
   * @throws IOException if we fail to refresh the disk usage
   */
  public DU(File path, Configuration conf) throws IOException {
    this(path, 600000L);
    //10 minutes default refresh interval
  }
  
  public NamespaceSliceDU addNamespace(int namespaceId, File path, Configuration conf)
      throws IOException{
    NamespaceSliceDU nsdu = new NamespaceSliceDU(path, conf);
    NamespaceSliceDU oldVal = namespaceSliceDUMap.putIfAbsent(namespaceId, nsdu);
    return oldVal != null? oldVal: nsdu; 
  }
  
  public void removeNamespace(int namespaceId) {
    this.namespaceSliceDUMap.remove(namespaceId);
  }

  /**
   * This thread refreshes the "used" variable.
   * 
   * Future improvements could be to not permanently
   * run this thread, instead run when getUsed is called.
   **/
  class DURefreshThread implements Runnable {
    
    public void run() {
      
      while(shouldRun) {

        try {
          Thread.sleep(refreshInterval);
          
          for (NamespaceSliceDU nsdu: namespaceSliceDUMap.values()) {
            try {
              //update the used variable
              nsdu.run();
              synchronized(nsdu.exceptionLock) { 
                //If succeed, we should set the exception to null
                nsdu.duException = null;
              }
            } catch (IOException e) {
              synchronized(nsdu.exceptionLock) {
                //save the latest exception so we can return it in getUsed()
                nsdu.duException = e;
              }
              LOG.warn("Could not get disk usage information", e);
            }
          }
        } catch (InterruptedException e) {
        }
      }
    }
  }
  
  public class NamespaceSliceDU extends Shell {
    private String dirPath;
    private AtomicLong used = new AtomicLong();
    private volatile IOException duException = null;
    final Object exceptionLock = new Object();
    HashSet<String> suspiciousFiles = null;
    
    public NamespaceSliceDU(File path, Configuration conf) throws IOException {
      super(0);
      dirPath = path.getCanonicalPath();
      //populate the used variable
      run();
    }
    
    public void processErrorOutput(ExitCodeException ece)
        throws IOException {
      // Errors outputs like
      // 'du: cannot access `<file>': No such file or directory'
      // are expected and don't impact the size estimation. The error code
      // is 1 for this case. We just log the error message without throwing
      // any exception.
      //
      if (super.getExitCode() != 1) {
        throw ece;
      }

      String errMsg = ece.getMessage();

      boolean containExpectMsg = false;
      HashSet<String> newSuspiciousFiles = new HashSet<String>();

      for (String line : errMsg.trim().split(
          System.getProperty("line.separator"))) {
        if (line.trim().startsWith("du: cannot access `")
            && line.trim().endsWith("': No such file or directory")) {
          containExpectMsg = true;
          if (suspiciousFiles != null
              && suspiciousFiles.contains(line.trim())) {
            throw new IOException("Cannot access a file at least twice", ece);
          }
          newSuspiciousFiles.add(line.trim());
        } else {
          throw ece;
        }
      }
      suspiciousFiles = newSuspiciousFiles;
      if (!containExpectMsg) {
        throw ece;
      }      
      LOG.info("DU error message: " + errMsg);
    }
    
    public void run() throws IOException{
      try {
        super.run();
      } catch (ExitCodeException ece) {
        processErrorOutput(ece);
      }
    }

    /**
     * Decrease how much disk space we use.
     * @param value decrease by this value
     */
    public void decDfsUsed(long value) {
      used.addAndGet(-value);
    }

    /**
     * Increase how much disk space we use.
     * @param value increase by this value
     */
    public void incDfsUsed(long value) {
      used.addAndGet(value);
    }
    
    /**
     * @return disk space used 
     * @throws IOException if the shell command fails
     */
    public long getUsed() throws IOException {
      //if the updating thread isn't started, update on demand
      if(refreshUsed == null) {
        run();
      } else {
        synchronized (this.exceptionLock) {
          //if an exception was thrown in the last run, rethrow
          if(duException != null) {
            IOException tmp = duException;
            duException = null;
            throw tmp;
          }
        }
      }
      
      return used.longValue();
    }
    
    /**
     * @return the path of which we're keeping track of disk usage
     */
    public String getDirPath() {
      return dirPath;
    }

    protected void parseExecResult(BufferedReader lines) throws IOException {
      String line = lines.readLine();
      if (line == null) {
        throw new IOException("Expecting a line not the end of stream");
      }
      String[] tokens = line.split("\t");
      if(tokens.length == 0) {
        throw new IOException("Illegal du output");
      }
      this.used.set(Long.parseLong(tokens[0])*1024);
    }

    public String toString() {
      return
        "du -sk " + dirPath +"\n" +
        used + "\t" + dirPath;
    }

    protected String[] getExecString() {
      return new String[] {"du", "-sk", dirPath};
    }
  }

  /**
   * @return the path of which we're keeping track of disk usage
   */
  public String getDirPath() {
    return dirPath;
  }
  
  /**
   * Start the disk usage checking thread.
   */
  public void start() {
    //only start the thread if the interval is sane
    if(refreshInterval > 0) {
      refreshUsed = new Thread(new DURefreshThread(), 
          "refreshUsed-"+dirPath);
      refreshUsed.setDaemon(true);
      refreshUsed.start();
    }
  }
  
  /**
   * Shut down the refreshing thread.
   */
  public void shutdown() {
    this.shouldRun = false;
    this.namespaceSliceDUMap.clear();
    if(this.refreshUsed != null) {
      this.refreshUsed.interrupt();
      try {
        this.refreshUsed.join();
        this.refreshUsed = null;
      } catch (InterruptedException ie) {
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0) {
      path = args[0];
    }

    System.out.println(new DU(new File(path), new Configuration()).toString());
  }
}
