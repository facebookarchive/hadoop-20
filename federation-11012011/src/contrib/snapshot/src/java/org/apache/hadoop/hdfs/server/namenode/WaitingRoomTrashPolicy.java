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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.text.*;

/**
 * Trash policy to be used with the WaitingRoom. Moves expunged
 * trash checkpoints to the waiting room rather than deleting them.
 */
public class WaitingRoomTrashPolicy extends TrashPolicyDefault {
  public static final Log LOG =  LogFactory.getLog(WaitingRoomTrashPolicy.class);

  public WaitingRoomTrashPolicy() { }

  private WaitingRoomTrashPolicy(Path home, Configuration conf) throws IOException {
    super(home, conf);
  }

  @Override
  public void deleteCheckpoint() throws IOException {
    FileStatus[] dirs = null;

    try {
      dirs = fs.listStatus(trash);            // scan trash sub-directories
    } catch (FileNotFoundException fnfe) {
      return;
    }

    if (dirs == null) return;

    long now = System.currentTimeMillis();
    for (int i = 0; i < dirs.length; i++) {
      Path path = dirs[i].getPath();
      String dir = path.toUri().getPath();
      String name = path.getName();
      if (name.equals(CURRENT.getName()))         // skip current
        continue;

      long time;
      try {
        synchronized (CHECKPOINT) {
          time = CHECKPOINT.parse(name).getTime();
        }
      } catch (ParseException e) {
        LOG.warn("Unexpected item in trash: "+dir+". Ignoring.");
        continue;
      }

      if ((now - deletionInterval) > time) {
        WaitingRoom wr = new WaitingRoom(getConf(), fs);
        if (wr.moveToWaitingRoom(path)) { // instead of delete
          LOG.info("Moved to waiting room trash checkpoint: "+dir);
        } else {
          LOG.warn("Couldn't move to waiting room checkpoint: "+dir+" Ignoring.");
        }
      }
    }
  }

  @Override
  public Runnable getEmptier() throws IOException {
    return new Emptier(getConf());
  }

  private class Emptier implements Runnable {
    private Configuration conf;
    private long emptierInterval;

    Emptier(Configuration conf) throws IOException {
      this.conf = conf;
      this.emptierInterval = (long)
                             (conf.getFloat("fs.trash.checkpoint.interval", 0) *
                              MSECS_PER_MINUTE);
      if (this.emptierInterval > deletionInterval ||
        this.emptierInterval == 0) {
        LOG.warn("The configured interval for checkpoint is " +
                 this.emptierInterval + " minutes." +
                 " Using interval of " + deletionInterval +
                 " minutes that is used for deletion instead");
        this.emptierInterval = deletionInterval;
      }
    }

    public void run() {
      if (emptierInterval == 0)
       return;                                   // trash disabled
      long now = System.currentTimeMillis();
      long end;
      while (true) {
        end = ceiling(now, emptierInterval);
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (InterruptedException e) {
          break;                                  // exit on interrupt
        }

        try {
          now = System.currentTimeMillis();
          if (now >= end) {
            FileStatus[] homes = null;
            try {
              homes = fs.listStatus(homesParent);         // list all home dirs
            } catch (IOException e) {
              LOG.warn("Trash can't list homes: "+e+" Sleeping.");
              continue;
            }

            for (FileStatus home : homes) {         // dump each trash
              if (!home.isDir())
                continue;
              try {
                WaitingRoomTrashPolicy trash = 
                                new WaitingRoomTrashPolicy(home.getPath(), conf);
                trash.deleteCheckpoint();
                trash.createCheckpoint();
              } catch (IOException e) {
                LOG.warn("Trash caught: "+e+". Skipping "+home.getPath()+".");
              }
            }
          }
        } catch (Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run(): ", e);
        }
      }

      try {
        fs.close();
      } catch(IOException e) {
        LOG.warn("Trash cannot close FileSystem: ", e);
      }
    }

    private long ceiling(long time, long interval) {
      return floor(time, interval) + interval;
    }

    private long floor(long time, long interval) {
      return (time / interval) * interval;
    }
  }
}
