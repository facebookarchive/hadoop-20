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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Class representing item (dir/file) in hadoop archive.
 * 
 * {@see HarFileSystem} for description of the format used in index files
 */
public class HarStatus {
  private boolean isDir;
  private String name;
  private List<String> children;

  private String partName;
  private long startIndex;
  private long length;

  private HarProperties properties;

  /**
   * Creates HarStatus via parsing from line in har _index file
   * with default version
   * 
   * @param harString
   *          line in _index file, representing har item
   * @throws IOException 
   */
  public HarStatus(String harString) throws IOException {
    this(harString, null, HarFileSystem.VERSION);
  }

  /**
   * Creates HarStatus via parsing from line in har _index file
   * 
   * @param harString
   *          line in _index file, representing har item
   * @param defaultFS
   *          default file status that will be used
   * @param version
   *          version of har archive to parse
   * @throws IOException
   */
  public HarStatus(String harString, FileStatus defaultFS, int version)
            throws IOException {
    String[] splits = harString.split(" ");
    if (splits.length < 5) {
      throw new IOException("Error while parsing HarStatus, expected 5 fields, found: "
          + splits.length);
    }
    this.name = HarFileSystem.decodeFileName(splits[0], version);
    this.isDir = "dir".equals(splits[1]);
    // this is equal to "none" if its a directory
    this.partName = splits[2];
    this.startIndex = Long.parseLong(splits[3]);
    this.length = Long.parseLong(splits[4]);

    String propString = null;
    if (isDir) {
      if (!this.partName.equals("none")){
        propString = this.partName;
      }
      children = new ArrayList<String>();
      for (int i = 5; i < splits.length; i++) {
        children.add(HarFileSystem.decodeFileName(splits[i], version));
      }
    } else if (splits.length >= 6) {
      propString = splits[5];
    }

    if (propString != null) {
      properties = HarProperties.fromString(propString);
    } else if (defaultFS != null) {
      properties = new HarProperties(defaultFS);
    }
  }
  
  public String serialize() throws UnsupportedEncodingException {
    List<String> parts = new ArrayList<String>();
    String propString = properties.serialize();  
    if (isDir) {
      parts.add(HarFileSystem.encode(name));
      parts.add("dir");
      parts.add(propString);
      parts.add("0");
      parts.add("0");
      for (String child: children) {
        parts.add(HarFileSystem.encode(child));
      }
    } else {
      parts.add(HarFileSystem.encode(name));
      parts.add("file");
      parts.add(partName);
      parts.add(Long.toString(startIndex));
      parts.add(Long.toString(length));
      parts.add(propString);
    }
    return StringUtils.join(parts, ' ');
  }
  
  public boolean isDir() {
    return isDir;
  }

  public String getName() {
    return name;
  }

  public List<String> getChildren() {
    return children;
  }

  public void setChildren(List<String> children) {
    this.children = children;
  }

  public String getPartName() {
    return partName;
  }
  
  public long getStartIndex() {
    return startIndex;
  }
  
  public long getLength() {
    return length;
  }
  
  public long getModificationTime() {
    return properties.getModificationTime();
  }
  
  public long getAccessTime() {
    return properties.getAccessTime();
  }
  
  public FsPermission getPermission() {
    return properties.getPermission();
  }
  
  public String getOwner() {
    return properties.getOwner();
  }
  
  public String getGroup() {
    return properties.getGroup();
  }

  public HarProperties getProperties() {
    return properties;
  }
}
