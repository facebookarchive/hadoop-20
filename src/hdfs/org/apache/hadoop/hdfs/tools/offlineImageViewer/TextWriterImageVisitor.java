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

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.BufferedByteOutputStream;

/**
 * TextWriterImageProcessor mixes in the ability for ImageVisitor
 * implementations to easily write their output to a text file.
 *
 * Implementing classes should be sure to call the super methods for the
 * constructors, finish and finishAbnormally methods, in order that the
 * underlying file may be opened and closed correctly.
 *
 * Note, this class does not add newlines to text written to file or (if
 * enabled) screen.  This is the implementing class' responsibility.
 */
abstract class TextWriterImageVisitor extends ImageVisitor {
  
  static String PART_SUFFIX = "_part_";
  
  private boolean printToScreen = false;
  private boolean okToWrite = false;
  private DataOutputStream out;
  final private int numberOfParts;
  final private String filename;

  private long numberOfFiles = 0; 
  private long filesPerRoll = 0;
  private long filesCount = 0;
  private int currentPart = 0;

  /**
   * Create a processor that writes to the file named.
   *
   * @param filename Name of file to write output to
   */
  public TextWriterImageVisitor(String filename) throws IOException {
    this(filename, false, 1);
  }
  
  public TextWriterImageVisitor(String filename, boolean printToScreen)
      throws IOException {
    this(filename, printToScreen, 1);
  }

  /**
   * Create a processor that writes to the file named and may or may not
   * also output to the screen, as specified.
   *
   * @param filename Name of file to write output to
   * @param printToScreen Mirror output to screen?
   * @param numberOfParts how many parts of the output are to be produced
   */
  public TextWriterImageVisitor(String filename, boolean printToScreen, int numberOfParts)
         throws IOException {
    super();
    this.printToScreen = printToScreen;
    this.numberOfParts = numberOfParts;
    this.filename = filename;
    if (numberOfParts < 1) {
      throw new IllegalArgumentException("Number of parts cannot be less than 1");
    }
    createOutputStream();
    okToWrite = true;
  }
  
  private void createOutputStream() throws FileNotFoundException {
    String suffix = (numberOfParts > 1) ? "_part_" + currentPart : "";
    out = BufferedByteOutputStream.wrapOutputStream(new FileOutputStream(
        filename + suffix), 8 * ImageLoaderCurrent.BASE_BUFFER_SIZE,
        ImageLoaderCurrent.BASE_BUFFER_SIZE);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor#finish()
   */
  @Override
  void finish() throws IOException {
    close();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor#finishAbnormally()
   */
  @Override
  void finishAbnormally() throws IOException {
    close();
  }

  /**
   * Close output stream and prevent further writing
   */
  private void close() throws IOException {
    out.close();
    okToWrite = false;
  }

  /**
   * Write parameter to output file (and possibly screen).
   *
   * @param toWrite Text to write to file
   */
  protected void write(String toWrite) throws IOException  {
    if(!okToWrite)
      throw new IOException("file not open for writing.");

    if(printToScreen)
      System.out.print(toWrite);

    try {
      out.write(DFSUtil.string2Bytes(toWrite));
    } catch (IOException e) {
      okToWrite = false;
      throw e;
    } 
  }
  
  /**
   * Close current segment and start a new one if needed
   */
  void rollIfNeeded() throws IOException {
    if (numberOfParts == 1 || numberOfFiles < 1) {
      return;
    }
    filesCount++;
    if (filesCount % filesPerRoll == 0) {
      out.close();
      currentPart++;
      createOutputStream();
    }
  }
  
  @Override
  void setNumberOfFiles(long numberOfFiles) throws IOException {
    if (numberOfFiles < 1) {
      throw new IOException("Number of files cannot be less than 1");
    }
    this.numberOfFiles = numberOfFiles;
    this.filesPerRoll = (numberOfFiles + numberOfParts - 1) / numberOfParts;
  }
}
