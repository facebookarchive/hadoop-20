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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.net.Socket;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

/*
 * This class is a container of a pool of  thread, for doing IOs to disk.
 * This class is used inside DataXceiverServer.
 * 
 */
class DataXceiverThreadPool {
  
  public static final Log LOG = LogFactory.getLog(DataXceiverThreadPool.class);
  
  // ThreadPool keep-alive time for threads over core pool size
  private static final long THREADS_KEEP_ALIVE_SECONDS = 600; 
  
  private ThreadPoolExecutor executor = null;
  private Configuration conf;

  /**
   * Create a pool of threads to do disk IO.
   * 
   * @param conf The configuration
   * @param tg The ThreadGroup of all the executor threads
   * @param maxXceiverCount The max number of threads that can do IO.
   */
  DataXceiverThreadPool(final Configuration conf, final ThreadGroup tg, 
                        final int maxXceiverCount) {
    
      ThreadFactory threadFactory = new ThreadFactory() {
          int counter = 0;

          @Override
          public Thread newThread(Runnable r) {
            int thisIndex;
            synchronized (this) {
              thisIndex = counter++;
            }
            Thread t = new Thread(tg, r);
            t.setName("disk io thread #" + thisIndex);
            t.setDaemon(true);
            return t;
          }
        };

      executor = new ThreadPoolExecutor(
          maxXceiverCount-1,
          maxXceiverCount,
          THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, 
          new LinkedBlockingQueue<Runnable>(), threadFactory);

      // This can reduce the number of running threads
      executor.allowCoreThreadTimeOut(true);
  }
  
  /**
   * Execute the task sometime in the future, using ThreadPools.
   */
  void execute(Runnable task) {
    executor.execute(task);
  }

  /**
   * How many threads are actively in use?
   */
  int getActiveCount() {
    return executor.getActiveCount();
  }
  
  /**
   * Gracefully shut down all ThreadPool. Will wait for all deletion
   * tasks to finish.
   */
  synchronized void shutdown() {
    if (executor != null) {
      LOG.info("Shutting down all disk io threads...");
      executor.shutdown();
      // do not set executor == null, otherwise we have to make 
      // getActiveCount as a synchronized method
    }
    LOG.info("All disk io threads have been shut down.");
  }
}
