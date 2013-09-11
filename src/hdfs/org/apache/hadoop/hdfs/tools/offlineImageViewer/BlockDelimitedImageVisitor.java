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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A BlockDelimitedImageVisitor generates a text representation of the fsimage,
 * with each element separated by a delimiter string.
 * Each line of the text file represents one block including BLOCK_ID, NUM_BYTES,
 * and GENERATION_STAMP. 
 * The default delimiter is tab. The delimiter value can be via the
 * constructor.
 */
class BlockDelimitedImageVisitor extends TextWriterImageVisitor {
  private static final String defaultDelimiter = "\t"; 
  public static final String defaultValue = "-1";
  
  final private LinkedList<ImageElement> elemQ = new LinkedList<ImageElement>();
  // Elements of fsimage we're interested in tracking
  private final Collection<ImageElement> elementsToTrack;
  // Values for each of the elements in elementsToTrack
  private final AbstractMap<ImageElement, String> elements = 
                                            new HashMap<ImageElement, String>();
  private final String delimiter;

  {
    elementsToTrack = new ArrayList<ImageElement>();
    
    // This collection determines what elements are tracked and the order
    // in which they are output
    Collections.addAll(elementsToTrack,  ImageElement.BLOCK_ID,
                                         ImageElement.NUM_BYTES,
                                         ImageElement.GENERATION_STAMP);
  }

  public BlockDelimitedImageVisitor(String outputFile, boolean printToScreen,
      int numberOfParts) throws IOException {
    this(outputFile, printToScreen, defaultDelimiter, numberOfParts);
  }
  
  public BlockDelimitedImageVisitor(String outputFile, boolean printToScreen, 
                               String delimiter, int numberOfParts)
      throws IOException {
    super(outputFile, printToScreen, numberOfParts);
    this.delimiter = delimiter;
    reset();
  }

  /**
   * Reset the values of the elements we're tracking in order to handle
   * the next file
   */
  private void reset() {
    elements.clear();
    for(ImageElement e : elementsToTrack) 
      elements.put(e, null);
    
  }
  
  @Override
  void leaveEnclosingElement() throws IOException {
    ImageElement elem = elemQ.pop();
    // If we're done with a block, write out our results and start over
    if (elem == ImageElement.BLOCK) {
      writeLine();
      reset();
    } else if (elem == ImageElement.INODE) {
      super.rollIfNeeded();
    }
  }

  /**
   * Iterate through all the elements we're tracking and, if a value was
   * recorded for it, write it out.
   */
  private void writeLine() throws IOException {
    Iterator<ImageElement> it = elementsToTrack.iterator();
    
    while(it.hasNext()) {
      ImageElement e = it.next();
      String v = elements.get(e);
      if(v != null)
        write(v);
      else 
        write(defaultValue);
      if(it.hasNext())
        write(delimiter);
    }
    write("\n");
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    if(elements.containsKey(element))
      elements.put(element, value);
    if (element.equals(ImageElement.GENERATION_STAMP)
        && (elemQ.element() == null ||
            !elemQ.element().equals(ImageElement.BLOCK))) {
      // Write fake block with current namenode generation stamp
      writeLine();
    }
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    elemQ.push(element);
  }

  @Override
  void visitEnclosingElement(ImageElement element, ImageElement key,
      String value) throws IOException {
    elemQ.push(element);
  }
  
  @Override
  void start() throws IOException { /* Nothing to do */ }
}
