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

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

/**
 * Random generators of common objects, used in {@link TestClientProxyRequests} and {@link
 * TestClientProxyResponses}
 */
public class RandomObjectsGenerators {
  private RandomObjectsGenerators() {
  }

  static int rndPInt(Random rnd) {
    return rnd.nextInt(100) + 1;
  }

  static Block rndBlock(Random rnd) {
    return new Block(rnd.nextInt(1000000), rnd.nextInt(1000000), rnd.nextInt(100000));
  }

  static byte[] rndByteArr(Random rnd, int count) {
    byte[] bytes = new byte[count];
    rnd.nextBytes(bytes);
    return bytes;
  }

  static HdfsFileStatus[] rndHdfsFileStatusArr(Random rnd, int count) {
    HdfsFileStatus[] files = new HdfsFileStatus[count];
    for (int i = 0; i < files.length; i++) {
      files[i] = new HdfsFileStatus(rnd.nextLong(), rnd.nextBoolean(), rnd.nextInt(5),
          rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), new FsPermission((short) rnd.nextInt()),
          "owner" + rnd.nextInt(), "group" + rnd.nextInt(), rndByteArr(rnd, rnd.nextInt(37)));
    }
    return files;
  }

  static LocatedBlocks[] rndLocatedBlocksArr(Random rnd, int count) {
    LocatedBlocks[] blockLocations = new LocatedBlocks[count];
    for (int i = 0; i < blockLocations.length; i++) {
      blockLocations[i] = new LocatedBlocks(rnd.nextInt(10000), Arrays.asList(rndLocatedBlockArr(
          rnd, 10)), rnd.nextBoolean());
    }
    return blockLocations;
  }

  static LocatedBlocksWithMetaInfo rndLocatedBlocksWithMetaInfo(Random rnd) {
    return new LocatedBlocksWithMetaInfo(rnd.nextInt(), Arrays.asList(rndLocatedBlockArr(rnd, 13)),
        rnd.nextBoolean(), rnd.nextInt(), rnd.nextInt(), rnd.nextInt());
  }

  static LocatedBlockWithMetaInfo rndLocatedBlockWithMetaInfo(Random rnd) {
    return new LocatedBlockWithMetaInfo(rndBlock(rnd), rndDatanodeInfoArr(rnd, 13), rnd.nextInt(),
        rnd.nextInt(), rnd.nextInt(), rnd.nextInt());
  }

  static LocatedBlock[] rndLocatedBlockArr(Random rnd, int count) {
    LocatedBlock[] blockLocations = new LocatedBlock[count];
    for (int i = 0; i < blockLocations.length; i++) {
      blockLocations[i] = new LocatedBlock(rndBlock(rnd), rndDatanodeInfoArr(rnd, 2), rnd.nextInt(),
          rnd.nextBoolean());
    }
    return blockLocations;
  }

  static DatanodeInfo[] rndDatanodeInfoArr(Random rnd, int count) {
    DatanodeInfo[] datanodes = new DatanodeInfo[count];
    for (int i = 0; i < datanodes.length; i++) {
      datanodes[i] = new DatanodeInfo("name" + rnd.nextInt(), "storage" + rnd.nextInt(),
          rnd.nextInt(65000), rnd.nextInt(65000), rnd.nextInt(10000), rnd.nextInt(10000),
          rnd.nextInt(10000), rnd.nextInt(1000000), rnd.nextInt(100000), rnd.nextInt(1000000),
          "location" + rnd.nextInt(), "host" + rnd.nextInt(), rnd.nextInt(
          AdminStates.values().length));
    }
    return datanodes;
  }

  static DatanodeID rndDatanodeID(Random rnd) {
    return new DatanodeID("name" + rnd.nextInt(), "storage" + rnd.nextInt(), rnd.nextInt(65000),
        rnd.nextInt(65000));
  }

  static FsPermission rndFsPermission(Random rnd) {
    return new FsPermission((short) rnd.nextInt());
  }

  static String rndString(Random rnd) {
    return "string" + rnd.nextInt();
  }

  /** Compares writable objects and outputs them verbosely in case of mismatch */
  static void assertEqualsVerbose(Writable object, Writable objectCopy) {
    assertArrayEquals("\nwritten  : " + ReflectionToStringBuilder.toString(object) +
        "\nread      : " + ReflectionToStringBuilder.toString(objectCopy),
        WritableUtils.toByteArray(object), WritableUtils.toByteArray(objectCopy));
  }
}
