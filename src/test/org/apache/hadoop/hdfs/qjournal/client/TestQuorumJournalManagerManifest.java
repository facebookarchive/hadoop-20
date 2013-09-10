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
package org.apache.hadoop.hdfs.qjournal.client;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.protocol.RemoteImage;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.io.MD5Hash;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestQuorumJournalManagerManifest {
  private static final Log LOG = LogFactory
      .getLog(TestQuorumJournalManager.class);

  @Before
  public void setup() {
    LOG.info("----- TEST -----");
  }

  // a singleton manifest from a single node
  @Test
  public void testSingleManifestSingelNode() throws IOException {
    List<RemoteImageManifest> manifests = Lists.newArrayList();
    manifests.add(createManifest(new ImageDescriptor[] { new ImageDescriptor(
        100, true) }));
    RemoteImageManifest rmOut = QuorumJournalManager.createImageManifest(
        manifests);
    assertTrue(manifests.get(0).getImages().equals(rmOut.getImages()));
  }

  // multiple images from a single node
  @Test
  public void testMultipleManifestSingelNode() throws IOException {
    List<RemoteImageManifest> manifests = Lists.newArrayList();
    // 102 should not be in the output manifes
    manifests.add(createManifest(new ImageDescriptor[] {
        new ImageDescriptor(100, true), new ImageDescriptor(101, true),
        new ImageDescriptor(102, false) }));
    RemoteImageManifest rmOut = QuorumJournalManager.createImageManifest(
        manifests);

    // only 100 and 101 should be in the final manifest
    assertEquals(100, rmOut.getImages().get(0).getTxId());
    assertEquals(101, rmOut.getImages().get(1).getTxId());
    assertEquals(2, rmOut.getImages().size());
  }

  // empty manifest from a single node
  @Test
  public void testEmpty() throws IOException {
    List<RemoteImageManifest> manifests = Lists.newArrayList();
    manifests.add(createManifest(new ImageDescriptor[] {}));

    RemoteImageManifest rmOut = QuorumJournalManager.createImageManifest(
        manifests);
    assertEquals(0, rmOut.getImages().size());
  }

  // multiple images from multiple nodes
  // one image has md5 missing on one node
  @Test
  public void testMultipleManifestsMultipleNodesSimple() throws IOException {
    List<RemoteImageManifest> manifests = Lists.newArrayList();
    // one image is not valid since it doesn't have md5 (102) but two other
    // nodes have it)
    // one image has two copies but one of them does not have md5 (103)
    manifests.add(createManifest(new ImageDescriptor[] {
        new ImageDescriptor(100, true), new ImageDescriptor(101, true),
        new ImageDescriptor(102, false), new ImageDescriptor(103, false) }));

    // but two nodes have a valid copy with md5
    manifests.add(createManifest(new ImageDescriptor[] {
        new ImageDescriptor(100, true), new ImageDescriptor(101, true),
        new ImageDescriptor(102, true) }));

    manifests.add(createManifest(new ImageDescriptor[] {
        new ImageDescriptor(100, true), new ImageDescriptor(101, true),
        new ImageDescriptor(102, true), new ImageDescriptor(103, true) }));

    RemoteImageManifest rmOut = QuorumJournalManager.createImageManifest(
        manifests);

    // the resulting manifest should have four images
    assertTrue(rmOut.getImages().size() == 4);
  }

  @Test
  public void testMultipleManifestsMultipleNodesMixed() throws IOException {
    List<RemoteImageManifest> manifests = Lists.newArrayList();

    // 0) 100, 101
    // 1) 101, 102
    // 2) 100, 102 all valid
    manifests.add(createManifest(new ImageDescriptor[] {
        new ImageDescriptor(100, true), new ImageDescriptor(101, true) }));

    manifests.add(createManifest(new ImageDescriptor[] {
        new ImageDescriptor(101, true), new ImageDescriptor(102, true) }));

    manifests.add(createManifest(new ImageDescriptor[] {
        new ImageDescriptor(100, true), new ImageDescriptor(102, true) }));

    RemoteImageManifest rmOut = QuorumJournalManager.createImageManifest(
        manifests);

    assertEquals(3, rmOut.getImages().size());
    // should be sorted
    for (int i = 0; i < 3; i++) {
      assertEquals(100 + i, rmOut.getImages().get(i).getTxId());
    }
  }

  static class ImageDescriptor {
    long txid;
    boolean hasMd5;

    ImageDescriptor(long txid, boolean hasMd5) {
      this.txid = txid;
      this.hasMd5 = hasMd5;
    }
  }

  private RemoteImageManifest createManifest(ImageDescriptor... descriptors)
      throws IOException {
    List<RemoteImage> res = Lists.newArrayList();
    for (ImageDescriptor id : descriptors) {
      res.add(createRemoteImage(id.txid, id.hasMd5));
    }
    Collections.sort(res);
    return new RemoteImageManifest(res);
  }

  private RemoteImage createRemoteImage(long txid, boolean hasMd5)
      throws IOException {
    MessageDigest digester = MD5Hash.getDigester();
    return new RemoteImage(txid, hasMd5 ? new MD5Hash(
        digester.digest(getBytes(txid))) : null);
  }

  public static byte[] getBytes(Long val) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeLong(val);
    return baos.toByteArray();
  }
}
