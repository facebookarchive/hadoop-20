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

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Test;

/** Test NameNode port defaulting code. */
public class TestDefaultNameNodePort {

  @Test
  public void testGetAddressFromString() throws Exception {
    assertEquals(NameNode.getAddress("foo").getPort(), NameNode.DEFAULT_PORT);
    assertEquals(NameNode.getAddress("hdfs://foo/").getPort(),
        NameNode.DEFAULT_PORT);
    assertEquals(NameNode.getAddress("hdfs://foo:555").getPort(), 555);
    assertEquals(NameNode.getAddress("foo:555").getPort(), 555);
  }

  @Test
  public void testGetAddressFromConf() throws Exception {
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://foo/");
    URI uri = new URI(null, NameNode.getDefaultAddress(conf), null, null, null);
    assertEquals(-1, uri.getPort());
    FileSystem.setDefaultUri(conf, "hdfs://foo:555");
    uri = new URI(null, NameNode.getDefaultAddress(conf), null, null, null);
    assertEquals(555, uri.getPort());
    FileSystem.setDefaultUri(conf, "foo");
    uri = new URI(null, NameNode.getDefaultAddress(conf), null, null, null);
    assertEquals(-1, uri.getPort());
  }
}
