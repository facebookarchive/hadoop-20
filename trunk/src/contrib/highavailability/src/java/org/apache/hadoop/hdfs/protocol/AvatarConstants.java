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
package org.apache.hadoop.hdfs.protocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.DataInput;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * Some global definitions for AvatarNode.
 */

public interface AvatarConstants {

  /**
   * Define the various avatars of the NameNode.
   */
  static public enum Avatar {
    ACTIVE    ("Primary"),
    STANDBY   ("Standby"),
    UNKNOWN   ("UnknownAvatar");

    private String description = null;
    private Avatar(String arg) {this.description = arg;}

    public String toString() {
      return description;
    }
  }

  /**
   * Define unique names for the instances of the AvatarNode.
   * At present, there can be only two.
   */
  static public enum InstanceId {
    NODEZERO    ("FirstNode"),
    NODEONE   ("SecondNode"),
    UNKNOWN   ("Unknown");

    private String description = null;
    private InstanceId(String arg) {this.description = arg;}

    public String toString() {
      return description;
    }
  }

  /** Startup options */
  static public enum StartupOption {
    NODEZERO("-zero"),
    NODEONE("-one"),
    SYNC("-sync"),
    ACTIVE ("-active"),
    STANDBY  ("-standby"),
    FORMAT  ("-format"),     // these are namenode options
    FORMATFORCE   ("-formatforce"),  
    REGULAR ("-regular"),
    UPGRADE ("-upgrade"),
    ROLLBACK("-rollback"),
    FINALIZE("-finalize"),
    IMPORT  ("-importCheckpoint"),
    SERVICE ("-service");
    

    private String name = null;
    private StartupOption(String arg) {this.name = arg;}
    public String getName() {return name;}
    public Avatar toAvatar() {
      switch(this) {
      case STANDBY:
        return Avatar.STANDBY;
      case ACTIVE:
        return Avatar.ACTIVE;
      default:
        return Avatar.UNKNOWN;
      }
    }
  }
}
