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
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;


/**
 * Utility functions for testing fsimage storage.
 */
public abstract class FSImageTestUtil {
  
  final static Log LOG = LogFactory.getLog(FSImageTestUtil.class);
  
  public static StorageDirectory mockStorageDirectory(
      File currentDir, NameNodeDirType type) {
    // Mock the StorageDirectory interface to just point to this file
    StorageDirectory sd = Mockito.mock(StorageDirectory.class);
    Mockito.doReturn(type)
      .when(sd).getStorageDirType();
    Mockito.doReturn(currentDir).when(sd).getCurrentDir();
    Mockito.doReturn(currentDir).when(sd).getRoot();
    Mockito.doReturn(mockFile(true)).when(sd).getVersionFile();
    Mockito.doReturn(mockFile(false)).when(sd).getPreviousDir();
    return sd;
  }
  
  /**
   * Make a mock storage directory that returns some set of file contents.
   * @param type type of storage dir
   * @param previousExists should we mock that the previous/ dir exists?
   * @param fileNames the names of files contained in current/
   */
  static StorageDirectory mockStorageDirectory(
      StorageDirType type,
      boolean previousExists,
      String...  fileNames) {
    StorageDirectory sd = mock(StorageDirectory.class);
    
    doReturn(type).when(sd).getStorageDirType();
  
    // Version file should always exist
    doReturn(mockFile(true)).when(sd).getVersionFile();
    doReturn(mockFile(true)).when(sd).getRoot();

    // Previous dir optionally exists
    doReturn(mockFile(previousExists))
      .when(sd).getPreviousDir();   
  
    // Return a mock 'current' directory which has the given paths
    File[] files = new File[fileNames.length];
    for (int i = 0; i < fileNames.length; i++) {
      files[i] = new File(fileNames[i]);
    }
    
    File mockDir = Mockito.spy(new File("/dir/current"));
    doReturn(files).when(mockDir).listFiles();
    doReturn(mockDir).when(sd).getCurrentDir();
    

    return sd;
  }
  
  static File mockFile(boolean exists) {
    File mockFile = mock(File.class);
    doReturn(exists).when(mockFile).exists();
    return mockFile;
  }
  
  public static class CheckpointTrigger {
    volatile boolean triggerCheckpoint = false;
    volatile boolean checkpointDone = false;
    volatile Exception e;

    private volatile CountDownLatch ckptLatch = new CountDownLatch(0);

    public void checkpointDone(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.STANDBY_EXIT_CHECKPOINT) {
        ckptLatch.countDown();
      }
      if (event == InjectionEvent.STANDBY_EXIT_CHECKPOINT_EXCEPTION) {
        e = (Exception) args[0];
        ckptLatch.countDown();
      }
    }

    public boolean triggerCheckpoint(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.STANDBY_CHECKPOINT_TRIGGER
          && ckptLatch.getCount() > 0) {
        return true;
      }
      return false;
    }

    public void doCheckpoint() throws Exception {
      e = null;
      ckptLatch = new CountDownLatch(1);
      try {
        ckptLatch.await();
      } catch (InterruptedException e) {
        throw new IOException("Interruption received");
      }
      if (e!=null)
        throw e;
    }
  }
}
