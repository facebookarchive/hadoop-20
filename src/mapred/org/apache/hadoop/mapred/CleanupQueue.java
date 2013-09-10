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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class CleanupQueue {

  public static final Log LOG =
    LogFactory.getLog(CleanupQueue.class);

  private static PathCleanupThread cleanupThread;

  /**
   * Create a singleton path-clean-up queue. It can be used to delete
   * paths(directories/files) in a separate thread. This constructor creates a
   * clean-up thread and also starts it as a daemon. Callers can instantiate one
   * CleanupQueue per JVM and can use it for deleting paths. Use
   * {@link CleanupQueue#addToQueue(PathDeletionContext...)} to add paths for
   * deletion.
   */
  public CleanupQueue() {
    synchronized (PathCleanupThread.class) {
      if (cleanupThread == null) {
        cleanupThread = new PathCleanupThread();
      }
    }
  }
  
  /**
   * Force to clean the all path, it should be called when task tracker is shut down
   * Now we only called it in MiniCoronaCluster to make sure the unit test run in a clean
   * fixture
   */
  public void forceClean() {
    while(true) {
      PathDeletionContext context = null;
      try {
        context = cleanupThread.queue.poll(50L, TimeUnit.MILLISECONDS);
        if (context == null) {
          return;
        }
        if (!deletePath(context)) {
          LOG.warn("forceClean:Unable to delete path " + context.fullPath);
        }
        else {
          LOG.info("foceClean DELETED " + context.fullPath);
        }
        
      } catch (InterruptedException e) {
        return;
      } catch (Exception e) {
        LOG.warn("Error deleting path " + context.fullPath + ": " + e);
      } 
    }
  }

  /**
   * Contains info related to the path of the file/dir to be deleted
   */
  static class PathDeletionContext {
    String fullPath;// full path of file or dir
    FileSystem fs;

    public PathDeletionContext(FileSystem fs, String fullPath) {
      this.fs = fs;
      this.fullPath = fullPath;
    }
    
    protected String getPathForCleanup() {
      return fullPath;
    }

    /**
     * Makes the path(and its subdirectories recursively) fully deletable
     */
    protected void enablePathForCleanup() throws IOException {
      // do nothing
    }
  }

  /**
   * Adds the paths to the queue of paths to be deleted by cleanupThread.
   */
  void addToQueue(PathDeletionContext... contexts) {
    cleanupThread.addToQueue(contexts);
  }

  protected static boolean deletePath(PathDeletionContext context)
            throws IOException {
    context.enablePathForCleanup();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to delete " + context.fullPath);
    }
    if (context.fs.exists(new Path(context.fullPath))) {
      return context.fs.delete(new Path(context.fullPath), true);
    }
    return true;
  }

  private static class PathCleanupThread extends Thread {

    // cleanup queue which deletes files/directories of the paths queued up.
    protected LinkedBlockingQueue<PathDeletionContext> queue =
      new LinkedBlockingQueue<PathDeletionContext>();

    public PathCleanupThread() {
      setName("Directory/File cleanup thread");
      setDaemon(true);
      start();
    }

    void addToQueue(PathDeletionContext[] contexts) {
      for (PathDeletionContext context : contexts) {
        try {
          queue.put(context);
        } catch(InterruptedException ie) {}
      }
    }

    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(getName() + " started.");
      }
      PathDeletionContext context = null;
      while (true) {
        try {
          context = queue.take();
          // delete the path.
          if (!deletePath(context)) {
            LOG.warn("CleanupThread:Unable to delete path " + context.fullPath);
          }
          else if (LOG.isDebugEnabled()) {
            LOG.debug("DELETED " + context.fullPath);
          }
        } catch (InterruptedException t) {
          LOG.warn("Interrupted deletion of " + context.fullPath);
          return;
        } catch (Exception e) {
          LOG.warn("Error deleting path " + context.fullPath + ": " + e);
        } 
      }
    }
  }
}
