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

package org.apache.hadoop.io.compress.snappy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

  import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecUnavailableException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class TestSnappyCodec extends TestCase {

  private Log LOG = LogFactory.getLog(TestSnappyCodec.class);

  private String inputDir;
  private String readFileDir;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    inputDir = System.getProperty("test.build.data", "target");
    readFileDir = System.getProperty("test.cache.data");
  }

  public void testFile() throws Exception {
    run("test.txt");
  }

  private void run(String filename) throws FileNotFoundException, IOException{
    File snappyFile = new File(inputDir, filename + new SnappyCodec().getDefaultExtension());
    Configuration conf = new Configuration();
    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(
        SnappyCodec.class, conf);

    // Compress
    InputStream is = new FileInputStream(new File(readFileDir, "testsnappy.txt"));

    FileOutputStream os = new FileOutputStream(snappyFile);

    CompressionOutputStream cos;
    try {
      cos = codec.createOutputStream(os);
    } catch (CodecUnavailableException ex) {
      LOG.error("Native codec unavailable, skipping test", ex);
      return;
    }
     

    byte buffer[] = new byte[8192];
    try {
      int bytesRead = 0;
      while ((bytesRead = is.read(buffer)) > 0) {
        cos.write(buffer, 0, bytesRead);
        System.out.println(new String(buffer));
      }
    } catch (IOException e) {
      System.err.println("Compress Error");
      e.printStackTrace();
    } finally {
      is.close();
      cos.close();
      os.close();
    }

    // Decompress
    is = new FileInputStream(new File(readFileDir, "testsnappy.txt"));
    FileInputStream is2 = new FileInputStream(snappyFile);
    CompressionInputStream cis = codec.createInputStream(is2);
    BufferedReader r = new BufferedReader(new InputStreamReader(is));
    BufferedReader cr = new BufferedReader(new InputStreamReader(cis));


    try {
      String line, rline;
      while ((line = r.readLine()) != null) {
        rline = cr.readLine();
        if (!rline.equals(line)) {
          System.err.println("Decompress error at line " + line + " of file " + filename);
          System.err.println("Original: [" + line + "]");
          System.err.println("Decompressed: [" + rline + "]");
        }
        assertEquals(rline, line);
      }
      assertNull(cr.readLine());
    } catch (IOException e) {
      System.err.println("Decompress Error");
      e.printStackTrace();
    } finally {
      cis.close();
      is.close();
      os.close();
    }
  }
}
