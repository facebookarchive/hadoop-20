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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode.StartupInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;
import org.apache.hadoop.hdfs.server.namenode.ValidateNamespaceDirPolicy.NNStorageLocation;
import org.junit.Before;
import org.junit.Test;

public class TestAvatarStorageSetup {

  public static final Log LOG = LogFactory.getLog(TestAvatarStorageSetup.class
      .getName());
  
  private static String baseDir =
      System.getProperty("test.build.data","build/test/data/") + "/"; 

  // names for image/edits
  private final String imageLclName = baseDir + "image/";
  private final String imageShdName0 = baseDir + "imageShared0/";
  private final String imageShdName1 = baseDir + "imageShared1/";

  private final String editsLclName = baseDir + "edits/";
  private final String editsShdName0 = baseDir + "editsShared0/";
  private final String editsShdName1 = baseDir + "editsShared1/";

  // usr's for image/edits
  private URI imageLcl, imageShd0, imageShd1;
  private URI editsLcl, editsShd0, editsShd1;

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    Collection<String> dirList = new ArrayList<String>();
    dirList.add(imageLclName);
    dirList.add(imageShdName0);
    dirList.add(imageShdName1);
    dirList.add(editsLclName);
    dirList.add(editsShdName0);
    dirList.add(editsShdName1);  
    
    for(String name : dirList) {
      File dir = new File(name);
      FileUtil.fullyDelete(dir);
      dir.mkdirs();
    }
    
    imageLcl = Util.stringAsURI(imageLclName);
    imageShd0 = Util.stringAsURI(imageShdName0);
    imageShd1 = Util.stringAsURI(imageShdName1);

    editsLcl = Util.stringAsURI(editsLclName);
    editsShd0 = Util.stringAsURI(editsShdName0);
    editsShd1 = Util.stringAsURI(editsShdName1);

    conf = new Configuration();
  }

  @Test
  public void testStringToUris() {
    // set strings in configuration
    conf.set("dfs.name.dir", imageLclName + "," + imageShdName0);
    conf.set("dfs.name.edits.dir", editsLclName + "," + editsShdName0);
    
    Collection<URI> dirs = NNStorageConfiguration.getNamespaceDirs(conf, null);
    // check if conversion to URIs is correct
    assertEqualsCol(getList(imageLcl, imageShd0), dirs);

    dirs = NNStorageConfiguration.getNamespaceEditsDirs(conf, null);
    // check if conversion to URIs is correct
    assertEqualsCol(getList(editsLcl, editsShd0), dirs);
  }
  
  // test parameter correctness

  @Test(expected = IOException.class)
  public void sharedImageShoudlNotOverlap() throws IOException {
    // dfs.name.dir already contains the shared directory
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl, imageShd0),
        getList(editsLcl), 
        imageShd0, imageShd1, 
        editsShd0, editsShd1);
  }
  
  @Test(expected = IOException.class)
  public void sharedImageQJMNotEqualToEdits() throws IOException, URISyntaxException {
    // QJM shared image location
    URI qjmSharedImage = new URI("qjm://testjournalImage");
    URI qjmSharedJournal = new URI("qjm://testjournalJournal");
    
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl),
        getList(editsLcl), 
        qjmSharedImage, imageShd1, 
        qjmSharedJournal, editsShd1);
  }
  
  @Test
  public void sharedImageQJM() throws IOException, URISyntaxException {
    // QJM shared image location
    URI qjmShared = new URI("qjm://testjournal");
    
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl),
        getList(editsLcl), 
        qjmShared, imageShd1, 
        qjmShared, editsShd1);
  }

  @Test(expected = IOException.class)
  public void sharedEditsShoudlNotOverlap() throws IOException {
    // dfs.name.edits.dir already contains the shared directory
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl),
        getList(editsLcl, editsShd0), 
        imageShd0, imageShd1, 
        editsShd0, editsShd1);
  }

  @Test(expected = IOException.class)
  public void shouldNotContainSharedImageLocation() throws IOException {
    // dfs.name.dir.shared is set manually
    conf.set("dfs.name.dir.shared", "/somelocation");
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl), 
        getList(editsLcl),
        imageShd0, imageShd1, 
        editsShd0, editsShd1);
  }
  
  @Test(expected = IOException.class)
  public void shouldNotContainSharedEditsLocation() throws IOException {
    // dfs.name.edits.dir.shared is set manually
    conf.set("dfs.name.edits.dir.shared", "/somelocation");
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl), 
        getList(editsLcl),
        imageShd0, imageShd1, 
        editsShd0, editsShd1);
  }
  
  @Test(expected = IOException.class)
  public void shouldNotContainNulls() throws IOException {
    conf.set("dfs.name.edits.dir.shared", "/somelocation");
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl), 
        getList(editsLcl),
        null, imageShd1, 
        editsShd0, editsShd1);
  }
  
  // test configuration setup
  
  @Test
  public void testSetup() throws IOException {
    Configuration newconf = null;
    
    // validate
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl), 
        getList(editsLcl),
        imageShd0, imageShd1, 
        editsShd0, editsShd1);
    
    // update configuration for nodezero
    newconf = new Configuration();
    StartupInfo startInfoZero = new StartupInfo(null, InstanceId.NODEZERO,
        false, "service", false);
    updateConf(startInfoZero, newconf);

    // check configuration
    assertEqualsCol(getList(imageLcl, imageShd0),
        NNStorageConfiguration.getNamespaceDirs(newconf, null));
    assertEqualsCol(getList(editsLcl, editsShd0),
        NNStorageConfiguration.getNamespaceEditsDirs(newconf, null));

    // check shared directory
    checkKey(imageShd0, "dfs.name.dir.shared", newconf);
    checkKey(editsShd0, "dfs.name.edits.dir.shared", newconf);

    // update configuration for nodeone
    newconf = new Configuration();
    StartupInfo startInfoOne = new StartupInfo(null, InstanceId.NODEONE, true,
        "service", false);
    updateConf(startInfoOne, newconf);

    // check configuration
    assertEqualsCol(getList(imageLcl, imageShd1),
        NNStorageConfiguration.getNamespaceDirs(newconf, null));
    assertEqualsCol(getList(editsLcl, editsShd1),
        NNStorageConfiguration.getNamespaceEditsDirs(newconf, null));

    // check shared directory
    checkKey(imageShd1, "dfs.name.dir.shared", newconf);
    checkKey(editsShd1, "dfs.name.edits.dir.shared", newconf);
  }
  
  @Test
  public void testLocationMapForFiles() throws IOException {
    // standard configuration with all file journal/images
    testLocationMap(conf, imageLcl, imageShd0, imageShd1, editsLcl, editsShd0,
        editsShd1);
  }

  @Test
  public void testLocationMapForNonFiles() throws Exception {
    // configure non-file journals
    // local image and edits must be file-based
    editsShd0 = Util.stringAsURI("foo:/editsshd0");
    editsShd1 = Util.stringAsURI("foo:/editsshd1");

    testLocationMap(conf, imageLcl, imageShd0, imageShd1, editsLcl, editsShd0,
        editsShd1);
  }
  
  public void testLocationMap(Configuration conf, 
      URI imageLcl, URI imageShd0, URI imageShd1, 
      URI editsLcl, URI editsShd0, URI editsShd1) 
          throws IOException {
    Configuration newconf = null;
    
    // validate
    AvatarStorageSetup.validate(conf, 
        getList(imageLcl), 
        getList(editsLcl),
        imageShd0, imageShd1, 
        editsShd0, editsShd1);
    
    // update configuration for nodezero
    newconf = new Configuration();
    StartupInfo startInfoZero = new StartupInfo(null, InstanceId.NODEZERO,
        false, "service", false);
    updateConf(startInfoZero, newconf);
    
    // validate
    Map<URI, NNStorageLocation> map = ValidateNamespaceDirPolicy.validate(newconf);
    
    // shared image and edits
    assertTrue(map.get(imageShd0).type == StorageLocationType.SHARED);
    assertTrue(map.get(editsShd0).type == StorageLocationType.SHARED);
    
    // local are not shared
    Collection<URI> l = getList(imageLcl, editsLcl);
    for (URI u : l) {
      if ((u.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) == 0))
        assertTrue(map.get(editsLcl).type == StorageLocationType.LOCAL);
      else
        assertTrue(map.get(editsLcl).type == StorageLocationType.REMOTE);
    }
    
    // all locations are present
    for (URI u : getList(imageLcl, imageShd0, editsLcl, editsShd0)) {
      assertNotNull(map.get(u));
    }  
  }
  
  @Test
  public void testSameSharedImageLocation() throws Exception {
    
    Configuration conf = new Configuration();
    
    URI img0 = new URI("qjm://localhost:1234;localhost:1235;localhost:1236/test-id/");
    URI img1 = new URI("qjm://localhost:1234;localhost:1235;localhost:1236/test-id/");
    URI edit0 = new URI("qjm://localhost:1234;localhost:1235;localhost:1236/test-id/zero/");
    URI edit1 = new URI("qjm://localhost:1234;localhost:1235;localhost:1236/test-id/one/");
    conf.set("dfs.name.dir.shared0", img0.toString());
    conf.set("dfs.name.dir.shared1", img1.toString());
    conf.set("dfs.name.edits.dir.shared0", edit0.toString());
    conf.set("dfs.name.edits.dir.shared1", edit1.toString());
 
    // local locations for image and edits
    Collection<URI> namedirs = NNStorageConfiguration.getNamespaceDirs(conf, null);
    Collection<URI> editsdir = NNStorageConfiguration.getNamespaceEditsDirs(conf, null);
    
    try {
      AvatarStorageSetup.validate(conf, namedirs, editsdir, img0, img1, edit0, edit1);
      fail("fail of same shared image location");
    } catch (IOException ex) {
      assertTrue(ex.getMessage().contains("same image location"));
    }
  }
  
  @Test
  public void testSameSharedEditsLocation() throws Exception {
    
    Configuration conf = new Configuration();
    
    URI img0 = new URI("qjm://localhost:1234;localhost:1235;localhost:1236/test-id/zero/");
    URI img1 = new URI("qjm://localhost:1234;localhost:1235;localhost:1236/test-id/one/");
    URI edit0 = new URI("qjm://localhost:1234;localhost:1235;localhost:1236/test-id/");
    URI edit1 = new URI("qjm://localhost:1234;localhost:1235;localhost:1236/test-id/");
    conf.set("dfs.name.dir.shared0", img0.toString());
    conf.set("dfs.name.dir.shared1", img1.toString());
    conf.set("dfs.name.edits.dir.shared0", edit0.toString());
    conf.set("dfs.name.edits.dir.shared1", edit1.toString());
 
    // local locations for image and edits
    Collection<URI> namedirs = NNStorageConfiguration.getNamespaceDirs(conf, null);
    Collection<URI> editsdir = NNStorageConfiguration.getNamespaceEditsDirs(conf, null);
    
    try {
      AvatarStorageSetup.validate(conf, namedirs, editsdir, img0, img1, edit0, edit1);
      fail("fail of same shared eduts location");
    } catch (IOException ex) {
      assertTrue(ex.getMessage().contains("same edits location"));
    }
  }
  
  ///// helpers
  
  private void checkKey(URI expected, String key, Configuration conf)
      throws IOException {
    assertEquals(expected, Util.stringAsURI(conf.get(key)));
  }

  private void updateConf(StartupInfo si, Configuration newconf) {
    AvatarStorageSetup.updateConf(si, newconf, getList(imageLcl),
        imageShd0, imageShd1, "dfs.name.dir");
    AvatarStorageSetup.updateConf(si, newconf, getList(editsLcl),
        editsShd0, editsShd1, "dfs.name.edits.dir");
  }
  
  private void assertEqualsCol(Collection<URI> obj1, Collection<URI> obj2) {
    assertTrue(obj1.containsAll(obj2));
    assertTrue(obj2.containsAll(obj1));
  }
  
  private Collection<URI> getList(URI... uris) {
    List<URI> result = new ArrayList<URI>();
    for (URI u : uris) {
      result.add(u);
    }
    return result;
  }
}
