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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor.ImageElement;

/**
 * Test that the DelimitedImageVisistor gives the expected output based
 * on predetermined inputs
 */
public class TestDelimitedImageVisitor extends TestCase {
  private static String ROOT = System.getProperty("test.build.data","/tmp");
  private static final String delim = "--";
  
  // Record an element in the visitor and build the expected line in the output
  private void build(TextWriterImageVisitor div, ImageElement elem, String val, 
                     StringBuilder sb, boolean includeDelim) throws IOException {
    div.visit(elem, val);
    sb.append(val);
    
    if(includeDelim)
      sb.append(delim);
  }
  
  public void testBlockDelimitedImageVisistorNParts() {
    Random rand = new Random();
    BufferedReader br = null;
    String filename = ROOT + "/testDIV";
    StringBuilder sb = new StringBuilder();
    int numParts = 3;
    int numFiles = 12;
    int blocksPerInode = 3;
    try {
      BlockDelimitedImageVisitor div =
          new BlockDelimitedImageVisitor(filename, true, delim, numParts);
      div.setNumberOfFiles(numFiles);

      div.visitEnclosingElement(ImageElement.FS_IMAGE);
      sb.append(BlockDelimitedImageVisitor.defaultValue);
      sb.append(delim);
      sb.append(BlockDelimitedImageVisitor.defaultValue);
      sb.append(delim);
      build(div, ImageElement.GENERATION_STAMP, "9999", sb, false);
      sb.append("\n");
      for (int i = 0; i < numFiles; i++) {
        generateINode(rand, div, sb, blocksPerInode);
      }
      div.leaveEnclosingElement(); // FS_IMAGE
      div.finish();
      StringBuilder actual = new StringBuilder();
      int expectedLines = blocksPerInode * numFiles / numParts;
      for (int i = 0; i < numParts; i++) {
        File f = new File(filename + TextWriterImageVisitor.PART_SUFFIX +
            i);
        br = new BufferedReader(new FileReader(f));
        String curLine;
        long numLines = 0;
        while ((curLine = br.readLine()) != null) {
          actual.append(curLine);
          actual.append("\n");
          numLines++;
        }
        br.close();
        assertEquals("Number of lines should match",
            i == 0? expectedLines + 1: expectedLines, numLines);
      }
      String exepcted = sb.toString();
      System.out.println("Expect to get: " + exepcted);
      System.out.println("Actually got: " + actual);
      assertEquals(exepcted.toString(), actual.toString());
      
    } catch (IOException e) {
      fail("Error while testing delmitedImageVisitor" + e.getMessage());
    } finally {
      for (int i = 0; i < numParts; i++) {
        File f = new File(filename + TextWriterImageVisitor.PART_SUFFIX +
            i);
        if(f.exists())
          f.delete();
      }
    }
  }
  
  public void testDelimitedImageVisistor() {
    String filename = ROOT + "/testDIV";
    File f = new File(filename);
    BufferedReader br = null;
    StringBuilder sb = new StringBuilder();
    
    try {
      DelimitedImageVisitor div = new DelimitedImageVisitor(filename, true, 1, delim, null, true);

      div.visit(ImageElement.FS_IMAGE, "Not in ouput");
      div.visitEnclosingElement(ImageElement.INODE);
      div.visit(ImageElement.LAYOUT_VERSION, "not in");
      div.visit(ImageElement.LAYOUT_VERSION, "the output");
      
      build(div, ImageElement.INODE_PATH,        "hartnell", sb, true);
      build(div, ImageElement.REPLICATION,       "99", sb, true);
      build(div, ImageElement.INODE_TYPE,        INode.INodeType.REGULAR_INODE.toString(), sb, true);
      build(div, ImageElement.INODE_HARDLINK_ID, "", sb, true);
      build(div, ImageElement.MODIFICATION_TIME, "troughton", sb, true);
      build(div, ImageElement.ACCESS_TIME,       "pertwee", sb, true);
      build(div, ImageElement.BLOCK_SIZE,        "baker", sb, true);
      build(div, ImageElement.NUM_BLOCKS,        "davison", sb, true);
      build(div, ImageElement.NUM_BYTES,         "55", sb, true);
      build(div, ImageElement.NS_QUOTA,          "baker2", sb, true);
      build(div, ImageElement.DS_QUOTA,          "mccoy", sb, true);
      build(div, ImageElement.PERMISSION_STRING, "eccleston", sb, true);
      build(div, ImageElement.USER_NAME,         "tennant", sb, true);
      build(div, ImageElement.GROUP_NAME,        "smith", sb, false);
      
      div.leaveEnclosingElement(); // INode
      div.finish();
      
      br = new BufferedReader(new FileReader(f));
      String actual = br.readLine();
      
      // Should only get one line
      assertNull(br.readLine());
      br.close();
      
      String exepcted = sb.toString();
      System.out.println("Expect to get: " + exepcted);
      System.out.println("Actually got:  " + actual);
      assertEquals(exepcted, actual);
      
    } catch (IOException e) {
      fail("Error while testing delmitedImageVisitor" + e.getMessage());
    } finally {
      if(f.exists())
        f.delete();
    }
  }
  
  public void testBlockDelimitedImageVisistor() {
    Random rand = new Random();
    String filename = ROOT + "/testDIV";
    File f = new File(filename);
    BufferedReader br = null;
    StringBuilder sb = new StringBuilder();
    
    try {
      BlockDelimitedImageVisitor div =
          new BlockDelimitedImageVisitor(filename, true, delim, 1);

      div.visitEnclosingElement(ImageElement.FS_IMAGE);
      sb.append(BlockDelimitedImageVisitor.defaultValue);
      sb.append(delim);
      sb.append(BlockDelimitedImageVisitor.defaultValue);
      sb.append(delim);
      build(div, ImageElement.GENERATION_STAMP, "9999", sb, false);
      sb.append("\n");
      generateINode(rand, div, sb, 3);
      div.leaveEnclosingElement(); // FS_IMAGE
      div.finish();
      
      br = new BufferedReader(new FileReader(f));
      StringBuilder actual = new StringBuilder();
      String curLine;
      while ((curLine = br.readLine()) != null) {
        actual.append(curLine);
        actual.append("\n");
      }
      br.close();
      String exepcted = sb.toString();
      System.out.println("Expect to get: " + exepcted);
      System.out.println("Actually got: " + actual);
      assertEquals(exepcted.toString(), actual.toString());
      
    } catch (IOException e) {
      fail("Error while testing delmitedImageVisitor" + e.getMessage());
    } finally {
      if(f.exists())
        f.delete();
    }
  }
  
  public void generateINode(Random rand, BlockDelimitedImageVisitor div, 
      StringBuilder sb, int blocksPerInode) throws IOException {
    div.visitEnclosingElement(ImageElement.INODE);
    div.visit(ImageElement.LAYOUT_VERSION, "not in");
    div.visit(ImageElement.LAYOUT_VERSION, "the output");
    div.visit(ImageElement.INODE_PATH, "hartnell");
    div.visit(ImageElement.REPLICATION, "99");
    div.visit(ImageElement.MODIFICATION_TIME, "troughton");
    div.visit(ImageElement.ACCESS_TIME, "pertwee");
    div.visit(ImageElement.BLOCK_SIZE, "baker");
    
    div.visitEnclosingElement(ImageElement.BLOCKS, ImageElement.NUM_BLOCKS, 3);
    for (int i = 0; i < blocksPerInode; i++) {
      div.visitEnclosingElement(ImageElement.BLOCK);
      build(div, ImageElement.BLOCK_ID, Long.toString(rand.nextLong()),
          sb, true);
      build(div, ImageElement.NUM_BYTES, Long.toString(rand.nextLong()),
          sb, true);
      // we don't print delimiter after generation stamp 
      build(div, ImageElement.GENERATION_STAMP, Long.toString(rand.nextLong()),
          sb, false);
      sb.append("\n");
      div.leaveEnclosingElement(); //BLOCK
    }
    div.leaveEnclosingElement(); // BLOCKS
    
    div.visit(ImageElement.NS_QUOTA, "baker2");
    div.visit(ImageElement.DS_QUOTA, "mccoy");
    div.visit(ImageElement.PERMISSION_STRING, "eccleston");
    div.visit(ImageElement.USER_NAME, "tennant");
    div.visit(ImageElement.GROUP_NAME, "smith");
    
    div.leaveEnclosingElement(); // INode
  }
}
