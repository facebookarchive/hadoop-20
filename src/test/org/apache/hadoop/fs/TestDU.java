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

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DU.NamespaceSliceDU;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;

/** This test makes sure that "DU" does not get to run on each call to getUsed */ 
public class TestDU extends TestCase {
  final static private File DU_DIR = new File(
      System.getProperty("test.build.data","/tmp"), "dutmp");

  public void setUp() throws IOException {
      FileUtil.fullyDelete(DU_DIR);
      assertTrue(DU_DIR.mkdirs());
  }

  public void tearDown() throws IOException {
      FileUtil.fullyDelete(DU_DIR);
  }
    
  private void createFile(File newFile, int size) throws IOException {
    // write random data so that filesystems with compression enabled (e.g., ZFS)
    // can't compress the file
    Random random = new Random();
    byte[] data = new byte[size];
    random.nextBytes(data);

    newFile.createNewFile();
    RandomAccessFile file = new RandomAccessFile(newFile, "rws");

    file.write(data);
      
    file.getFD().sync();
    file.close();
  }

  /**
   * Verify that du returns expected used space for a file.
   * We assume here that if a file system crates a file of size 
   * that is a multiple of the block size in this file system,
   * then the used size for the file will be exactly that size.
   * This is true for most file systems.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws NoSuchFieldException 
   * @throws SecurityException 
   * @throws IllegalAccessException 
   * @throws IllegalArgumentException 
   */
  @SuppressWarnings("unchecked")
  public void testDU() throws IOException, InterruptedException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    int writtenSize = 32*1024;   // writing 32K
    File file = DU_DIR;
    File file0 = new File(DU_DIR, "NS-0");
    File file1 = new File(DU_DIR, "NS-1");
    createFile(file0, writtenSize);
    createFile(file1, writtenSize);
    Configuration conf = new Configuration();

    Thread.sleep(5000); // let the metadata updater catch up
    
    DU du = new DU(file, 1000);
    NamespaceSliceDU nsdu0 = du.addNamespace(0, file0, conf);
    NamespaceSliceDU nsdu1 = du.addNamespace(1, file1, conf);
    du.start();
    long duSize0 = nsdu0.getUsed();
    long duSize1 = nsdu1.getUsed();
    assertEquals(writtenSize, duSize0);
    assertEquals(writtenSize, duSize1);
    // delete the file, expect it throws exception
    file0.delete();
    file1.delete();
    Thread.sleep(2000);
    try {
      duSize0 = nsdu0.getUsed();
      assertTrue(false);
    } catch (IOException ex) {
    }
    try {
      duSize1 = nsdu1.getUsed();
      assertTrue(false);
    } catch (IOException ex) {
    }
    //change the size 
    createFile(file0, writtenSize - 4096);
    createFile(file1, writtenSize + 4096);
    Thread.sleep(5000);
    duSize0 = nsdu0.getUsed();
    duSize1 = nsdu1.getUsed();
    du.shutdown();

    assertEquals(writtenSize - 4096, duSize0);
    assertEquals(writtenSize + 4096, duSize1);
    
    //test with 0 interval, will not launch thread 
    du = new DU(file, 0);
    nsdu0 = du.addNamespace(0, file0, conf);
    nsdu1 = du.addNamespace(1, file1, conf);
    du.start();
    duSize0 = nsdu0.getUsed();
    duSize1 = nsdu1.getUsed();
    du.shutdown();
    
    assertEquals(writtenSize - 4096, duSize0);  
    assertEquals(writtenSize + 4096, duSize1);  
    
    //test without launching thread 
    du = new DU(file, 10000);
    nsdu0 = du.addNamespace(0, file0, new Configuration());
    duSize0 = nsdu0.getUsed();
    
    assertEquals(writtenSize - 4096, duSize0);
    
    // test processErrorOutput
    Field namespaceSliceDUMapField = DU.class.getDeclaredField("namespaceSliceDUMap");
    namespaceSliceDUMapField.setAccessible(true);
    ConcurrentMap<Integer, NamespaceSliceDU> namespaceSliceDUMap = (ConcurrentMap<Integer, NamespaceSliceDU>) namespaceSliceDUMapField
        .get(du);
    NamespaceSliceDU nssd = namespaceSliceDUMap.get(0);

    // Throw when exit code is not 1
    boolean thrown = false;
    try {
      nssd.processErrorOutput(new ExitCodeException(1, "du: cannot access `a1.txt': No such file or directory"));
    } catch(ExitCodeException ece) {
      thrown = true;
    }
    TestCase.assertTrue(thrown);
    
    Field exitCodeField = Shell.class.getDeclaredField("exitCode");
    exitCodeField.setAccessible(true);
    exitCodeField.set(nssd, 1);
    
    // One single file fails to be accessed.
    nssd.processErrorOutput(new ExitCodeException(1, "du: cannot access `a1.txt': No such file or directory"));

    // Two files fail to be accessed
    nssd.processErrorOutput(new ExitCodeException(1, "du: cannot access `a2.txt': No such file or directory\ndu: cannot access `a3.txt': No such file or directory"));

    // Two files fail to be accessed, one is the same as the previous one
    thrown = false;
    try {
      nssd.processErrorOutput(new ExitCodeException(
          1,
          "du: cannot access `a4.txt': No such file or directory\ndu: cannot access `a3.txt': No such file or directory"));
    } catch (IOException ioe) {
      thrown = true;
    }
    TestCase.assertTrue(thrown);

    // Two files fail to be accessed, one is the same as a previous one
    thrown = false;
    try {
      nssd.processErrorOutput(new ExitCodeException(
          1,
          "du: cannot access `a2.txt': No such file or directory\ndu: cannot access `a5.txt': No such file or directory"));
    } catch (IOException ioe) {
      thrown = true;
    }
    TestCase.assertTrue(thrown);  

    // Two files fail to be accessed
    nssd.processErrorOutput(new ExitCodeException(1, "du: cannot access `a6.txt': No such file or directory"));
  }
}
