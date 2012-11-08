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

package org.apache.hadoop.hdfs;

import org.junit.Test;

import static org.junit.Assert.*;
import org.junit.Assert;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class TestDFSUtil {
  
  final static Log LOG = LogFactory.getLog(TestDFSUtil.class);
  
  /**
   * Test conversion of LocatedBlock to BlockLocation
   */
  @Test
  public void testLocatedBlocks2Locations() {
    DatanodeInfo d = new DatanodeInfo();
    DatanodeInfo[] ds = new DatanodeInfo[1];
    ds[0] = d;

    // ok
    Block b1 = new Block(1, 1, 1);
    LocatedBlock l1 = new LocatedBlock(b1, ds, 0, false);

    // corrupt
    Block b2 = new Block(2, 1, 1);
    LocatedBlock l2 = new LocatedBlock(b2, ds, 0, true);

    List<LocatedBlock> ls = Arrays.asList(l1, l2);
    LocatedBlocks lbs = new LocatedBlocks(10, ls, false);

    BlockLocation[] bs = DFSUtil.locatedBlocks2Locations(lbs);

    assertTrue("expected 2 blocks but got " + bs.length,
               bs.length == 2);

    int corruptCount = 0;
    for (BlockLocation b: bs) {
      if (b.isCorrupt()) {
        corruptCount++;
      }
    }

    assertTrue("expected 1 corrupt files but got " + corruptCount, 
               corruptCount == 1);
    
    // test an empty location
    bs = DFSUtil.locatedBlocks2Locations(new LocatedBlocks());
    assertEquals(0, bs.length);
  }

  /**
   * Tests for empty configuration, an exception is thrown from
   * {@link DFSUtil#getNNServiceRpcAddresses(Configuration)}   *
   */
  @Test
  public void testEmptyConf() {
    Configuration conf = new Configuration();
    try {
      DFSUtil.getNNServiceRpcAddresses(conf);
      Assert.fail("Expected IOException is not thrown");
    } catch (IOException expected) {
    }
  }
  
  @Test
  public void testUTFconversion() throws Exception {
    
    
    Random r = new Random(System.currentTimeMillis());
    for (int i = 0; i < 100000; i++) {
      // create random length string
      int len = r.nextInt(50) + 1;
      // full unicode string
      validateStringConversion(RandomStringUtils.random(len));
      // only ascii characters
      validateStringConversion(RandomStringUtils.randomAscii(len));
      // only alphanumeric characters
      validateStringConversion(RandomStringUtils.randomAlphanumeric(len));
      // validate singe-character, which translates to 1,2,3 bytes
      validateStringConversion(generateOneCharString(1));
      validateStringConversion(generateOneCharString(2));
      validateStringConversion(generateOneCharString(3));
    }
  }
  
  private static final String DEFAULT_TEST_DIR = 
      "build/test/data";
  private static final String TEST_DIR =
      new File(System.getProperty("test.build.data", DEFAULT_TEST_DIR)).
      getAbsolutePath();
  
  @Test
  public void testTextSerDe() throws Exception{
    File f = new File(TEST_DIR, "test.out");
    // for testing increase the count to at least 1M
    int count = 10000;
    String randomAscii = RandomStringUtils.randomAscii(100);
    String prefAscii = RandomStringUtils.randomAscii(50);
    String randomUTF = prefAscii + RandomStringUtils.random(50);
    
    long time;
    
    FileOutputStream fos = new FileOutputStream(f);
    DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
    
    time = write(count, randomAscii, dos, false);
    LOG.info("Written " + count + " of ascii strings using default encoding in :" + time);
    
    time = write(count, randomAscii, dos, true);
    LOG.info("Written " + count + " of ascii strings using optimized in :" + time);
    
    time = write(count, randomUTF, dos, false);
    LOG.info("Written " + count + " of non-ascii strings using default encoding in :" + time);
    
    time = write(count, randomUTF, dos, true);
    LOG.info("Written " + count + " of non-ascii strings using optimized in :" + time);   
    dos.close();
    
    //////////////////////////////////////////
    
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(new BufferedInputStream(fis));
    
    time = read(count, dis, false);
    LOG.info("Read " + count + " of ascii strings using default decoding in :" + time);
    
    time = read(count, dis, true);
    LOG.info("Read " + count + " of ascii strings using optimized decoding in :" + time);
    
    time = read(count, dis, false);
    LOG.info("Read " + count + " of non-ascii strings using default decoding in :" + time);
    
    time = read(count, dis, true);
    LOG.info("Read " + count + " of non-ascii strings using optimized decoding in :" + time);
  }
  
  private long read(int count, DataInputStream dos, boolean opt)
      throws IOException {
    long start = System.currentTimeMillis();
    if (opt) {
      for (int i = 0; i < count; i++) {
        Text.readStringOpt(dos);
      }
    } else {
      for (int i = 0; i < count; i++) {
        Text.readString(dos);
      }
    }
    long stop = System.currentTimeMillis();
    return stop - start;
  }
  
  private long write(int count, String s, DataOutputStream dos, boolean opt) 
      throws IOException{
    long start = System.currentTimeMillis();
    if (opt) {
      for(int i = 0; i < count; i++) {
        Text.writeStringOpt(dos, s);
      }
    } else {
      for(int i = 0; i < count; i++) {
        Text.writeString(dos, s);
      }
    }
    long stop = System.currentTimeMillis();
    return stop - start;
  }

  @Test
  public void testPathComponents() throws Exception {
    for (int i = 0; i < 200000; i++) {
      // full unicode path, non-directory
      validatePathConversion(true, false);
      // full unicode path, directory
      validatePathConversion(true, true);
      // only ascii path, non-directory
      validatePathConversion(false, false);
      // only ascii, directory
      validatePathConversion(false, true);
    }
  }
  
  // helper functions

  private void validateStringConversion(String str)
      throws UnsupportedEncodingException {
    // convert to bytes
    byte[] bytesJava = str.getBytes("UTF8");
    byte[] bytesFast = DFSUtil.string2Bytes(str);

    // conversion is equal
    assertTrue(Arrays.equals(bytesJava, bytesFast));

    // re-convert to string
    String stringJava = new String(bytesJava, "UTF8");
    String stringFast = DFSUtil.bytes2String(bytesJava);

    // re-conversion is equal
    assertEquals(stringJava, stringFast);
    
    // test empty string and array
    assertEquals(DFSUtil.bytes2String(new byte[0]), new String(new byte[0]));
    assertTrue(Arrays.equals(DFSUtil.string2Bytes(""), "".getBytes("UTF8")));
  }
  
  private void validatePathConversion(boolean full, boolean isDirectory)
      throws UnsupportedEncodingException {
    Random r = new Random(System.currentTimeMillis());
    // random depth
    int depth = r.nextInt(20) + 1;
    // random length
    int len = r.nextInt(20) + 1;
    String path = "";
    // "xxx" are used to ensure that "/" are not neighbors at the front
    // or the path does not unintentionally end with "/"
    for (int i = 0; i < depth; i++) {
      path += "/xxx"
          + (full ? RandomStringUtils.random(len) : RandomStringUtils
              .randomAscii(len)) + "xxx";
    }
    if (isDirectory)
      path += "xxx/";

    byte[][] pathComponentsFast = DFSUtil.splitAndGetPathComponents(path);
    byte[][] pathComponentsJava = getPathComponentsJavaBased(path);
    comparePathComponents(pathComponentsFast, pathComponentsJava);

    assertNull(DFSUtil.splitAndGetPathComponents("non-separator" + path));
    assertNull(DFSUtil.splitAndGetPathComponents(null));
    assertNull(DFSUtil.splitAndGetPathComponents(""));
  }
  
  /** Generates a single character string, which after 
   * conversion to utf8 will have charLen bytes.
   */
  private String generateOneCharString(int charLen)
      throws UnsupportedEncodingException {
    String temp = "";
    while (temp.getBytes("UTF8").length != charLen)
      temp = RandomStringUtils.random(1);    
    return temp;
  }
  
  /**
   * Convert strings to byte arrays for path components. 
   * Using standard java conversion.
   */
  private byte[][] getPathComponentsJavaBased(String path)
      throws UnsupportedEncodingException {
    String[] strings = getPathNames(path);
    if (strings.length == 0) {
      return new byte[][] { null };
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++)
      bytes[i] = strings[i].getBytes("UTF8");
    return bytes;
  }
  
  // compare path components
  private void comparePathComponents(byte[][] a, byte[][] b) {
    assertEquals(a.length, b.length);
    for (int i = 0; i < a.length; i++) {
      assertTrue(Arrays.equals(a[i], b[i]));
    }
  }
  
  private String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      return null;
    }
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }
}
