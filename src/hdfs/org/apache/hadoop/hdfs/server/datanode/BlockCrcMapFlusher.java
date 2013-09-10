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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolumeSet;
import org.apache.hadoop.hdfs.server.datanode.metrics.DatanodeThreadLivenessReporter.BackgroundThread;


/**
 * The class to periodically flush Block CRC infomation to disk
 * 
 * It is supposed to run as a demon. The demon wakes up periodically and flush
 * CRC information volume by volume. Here is the procedure:
 * 
 * Loop for every known namespace, for every namespace:
 * 
 * 1. find all volumes for the namespace. Only include the volumes whose block
 * CRC has been initially loaded from disk (either successfully or failed);
 * 2. find block -> CRC mapping for all those volumes, sorted by volumes and buckets
 * 3. for every volume, flush the CRC information to the CRC file for the volume.
 * 
 * Potentially, memory consumption can be reduced, by removing the temp map but
 * streaming the data from block map all the way to files. However, the codes
 * are tricky to write not to normal block read/write operations. So far we
 * don't see memory pressure of data node so we go with a simpler approach.
 * 
 */
class BlockCrcMapFlusher implements Runnable {
  public static final Log LOG = LogFactory.getLog(BlockCrcMapFlusher.class);
  final private DataNode datanode;
  final private VolumeMap volumeMap;
  final private FSVolumeSet fsVolumeSet;
  final private long flushInterval;
  long lastFlushed;
  private volatile boolean isClosed = false;

  BlockCrcMapFlusher(DataNode datanode, VolumeMap volumeMap, FSVolumeSet fsVolumeSet,
      long flushInterval) {
    if (datanode == null) {
      throw new NullPointerException();
    }
    this.datanode = datanode;
    this.volumeMap = volumeMap;
    this.fsVolumeSet = fsVolumeSet;
    this.flushInterval = flushInterval;
    this.lastFlushed = System.currentTimeMillis();
  }

  public void setClose() {
    this.isClosed = true;
  }

  @Override
  public void run() {
    try {
      while (!isClosed) {
        datanode
            .updateAndReportThreadLiveness(BackgroundThread.BLOCK_CRC_FLUSHER);
        long timeNow = System.currentTimeMillis();
        
        if (lastFlushed + flushInterval <= timeNow) {
          try {
            Integer[] namespaces = volumeMap.getNamespaceList();
            // Process every namespace.
            for (Integer ns : namespaces) {
              NamespaceMap nsm = volumeMap.getNamespaceMap(ns);
              if (nsm == null) {
                LOG.info("Cannot find namespace map for namespace " + ns
                    + ". It's probably deleted.");
                continue;
              }
              FSVolume[] fsVolumes = fsVolumeSet.getVolumes();
              List<FSVolume> volumesToFlush = new ArrayList<FSVolume>();
              // Only pick those volumes whose on disk block CRC has been
              // tried to be loaded to make sure at least the file will
              // contain all the information from the previous on disk file.
              for (FSVolume volume : fsVolumes) {
                if (volume.isNamespaceBlockCrcLoaded(ns)) {
                  volumesToFlush.add(volume);
                } else {
                  LOG.info("Block CRC file for Volume " + volume
                      + " for namespece " + ns
                      + " is not loaded yet. Skip flushing...");
                }
              }
              Map<FSVolume, List<Map<Block, DatanodeBlockInfo>>> map = nsm
                  .getBlockCrcPerVolume(volumesToFlush);
              if (map == null) {
                continue;
              }
              for (Map.Entry<FSVolume, List<Map<Block, DatanodeBlockInfo>>> entry : map
                  .entrySet()) {
                // For every volume of the namespace, we write the information to disk.
                if (entry.getValue().size() == 0) {
                  continue;
                }
                datanode
                    .updateAndReportThreadLiveness(BackgroundThread.BLOCK_CRC_FLUSHER);

                FSVolume volume = entry.getKey();
                File crcFile = volume.getBlockCrcFile(ns);
                File crcTmpFile = volume.getBlockCrcTmpFile(ns);
                if (crcFile == null || crcTmpFile == null) {
                  LOG.warn("Cannot find CRC file to flush for namespace " + ns);
                }
                crcTmpFile.delete();
                if (crcTmpFile.exists()) {
                  LOG.warn("Not able to delete file "
                      + crcTmpFile.getAbsolutePath() + ". skip the volume");
                  continue;
                }

                try {
                  List<Map<Block, DatanodeBlockInfo>> mbds = entry.getValue();
                  DataOutputStream dos = new DataOutputStream(
                      new FileOutputStream(crcTmpFile));
                  try {
                    writeToCrcFile(mbds, dos);
                  } finally {
                    dos.close();
                  }
                  crcFile.delete();
                  crcTmpFile.renameTo(crcFile);
                  LOG.info("Flushed Block CRC file for Volume " + volume
                      + " for namespece " + ns + ".");
                } catch (InterruptedIOException e) {
                  LOG.info("InterruptedIOException ", e);
                  return;
                } catch (IOException e) {
                  LOG.warn("flushing namespace " + ns + " volume " + volume
                      + " failed.", e);
                }
              }
            }
          } finally {
            lastFlushed = timeNow;
          }
        } else {
          // Wait until the flush interval.
          long sleepTimeLeft = lastFlushed + flushInterval - timeNow;
          while (sleepTimeLeft > 0) {
            long sleepInteval = 1000;
            long timeToSleep = Math.min(sleepInteval, sleepTimeLeft);
            Thread.sleep(timeToSleep);
            datanode
                .updateAndReportThreadLiveness(BackgroundThread.BLOCK_CRC_FLUSHER);
            sleepTimeLeft -= timeToSleep;
          }
        }
      }
    } catch (InterruptedException e) {
      LOG.info("BlockCrcMapFlusher interrupted");
    } finally {
      LOG.info("BlockCrcMapFlusher exiting...");
    }
  }

  static void writeToCrcFile(List<Map<Block, DatanodeBlockInfo>> mbds,
      DataOutput dataOut) throws IOException {
    BlockCrcFileWriter writer = new BlockCrcFileWriter(dataOut,
        BlockCrcInfoWritable.LATEST_BLOCK_CRC_FILE_VERSION, mbds.size());
    writer.writeHeader();
    for (Map<Block, DatanodeBlockInfo> mbd : mbds) {
      writer.writeBucket(mbd);
    }
    writer.checkFinish();
  }
}
