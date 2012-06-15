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
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileWriter;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.HostsFileReader;

/**
 *
 * @author dms
 */
public class TestTTMover extends TestCase {

  static final File TEST_ROOT_DIR =
          new File(System.getProperty("test.build.data", "/tmp"));

  @Override
  protected void setUp() throws Exception {
    TEST_ROOT_DIR.mkdirs();
  }

  public void testFileModifications() throws Exception {
    System.out.println(TEST_ROOT_DIR);
    Configuration conf = new Configuration();
    File hosts = new File(TEST_ROOT_DIR, "hosts.file");
    if (!hosts.exists()) {
      hosts.createNewFile();
    }
    FileWriter writer = new FileWriter(hosts);
    writer.write("host1.host.com\n");
    writer.write("host2.host.com\n");

    writer.close();



    TTMover mover = new TTMoverTestStub(TEST_ROOT_DIR.toString());
    mover.setConf(conf);

    mover.addHostToFile(hosts.getAbsolutePath(), "host3.host.com");
    HostsFileReader reader =
            new HostsFileReader(hosts.getAbsolutePath(), hosts.getAbsolutePath());
    System.out.println(reader.getHosts().toString());
    assertEquals(3, reader.getHosts().size());

    mover.removeHostFromFile(hosts.getAbsolutePath(), "host1.host.com");

    reader.refresh();
    assertEquals(2, reader.getHosts().size());

    mover.restoreFile(hosts.getAbsolutePath());

    reader.refresh();
    assertEquals(2, reader.getHosts().size());

    assertTrue(reader.getHosts().contains("host1.host.com"));
    assertFalse(reader.getHosts().contains("host3.host.com"));
  }

  public void testHostRemove() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.hosts", "hosts.include");
    conf.set("mapred.hosts.exclude", "hosts.exclude");

    File hostsInclude = new File(TEST_ROOT_DIR, "hosts.include");
    File hostsExclude = new File(TEST_ROOT_DIR, "hosts.exclude");
    File slaves = new File(TEST_ROOT_DIR, "slaves");

    if (hostsExclude.exists()) {
      hostsExclude.delete();
    }
    hostsExclude.createNewFile();
    
    FileWriter writer = new FileWriter(hostsInclude);
    writer.write("host1\nhost2\n");
    writer.close();
    writer = new FileWriter(slaves);
    writer.write("host1\nhost2\n");
    writer.close();

    TTMoverTestStub mover = new TTMoverTestStub(TEST_ROOT_DIR.toString());
    mover.setConf(conf);
    mover.run(new String[]{"-remove", "host1"});

    HostsFileReader reader =
            new HostsFileReader(hostsInclude.getAbsolutePath(),
            hostsExclude.getAbsolutePath());
    assertTrue(reader.getExcludedHosts().contains("host1"));

    assertTrue(reader.getHosts().contains("host2"));
    assertFalse(reader.getHosts().contains("host1"));
  }

  public void testHostAdd() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.hosts", "hosts.include");
    conf.set("mapred.hosts.exclude", "hosts.exclude");

    File hostsInclude = new File(TEST_ROOT_DIR, "hosts.include");
    File hostsExclude = new File(TEST_ROOT_DIR, "hosts.exclude");
    File slaves = new File(TEST_ROOT_DIR, "slaves");


    FileWriter writer = new FileWriter(hostsInclude);
    writer.write("host1\nhost2\n");
    writer.close();

    writer = new FileWriter(slaves);
    writer.write("host1\nhost2\n");
    writer.close();

    writer = new FileWriter(hostsExclude);
    writer.write("host3\n");
    writer.close();

    HostsFileReader reader =
            new HostsFileReader(hostsInclude.getAbsolutePath(),
            hostsExclude.getAbsolutePath());

    assertEquals(2, reader.getHosts().size());
    
    TTMoverTestStub mover = new TTMoverTestStub(TEST_ROOT_DIR.toString());
    mover.setConf(conf);
    mover.run(new String[]{"-add", "host3"});

    reader.refresh();
    assertFalse(reader.getExcludedHosts().contains("host3"));

    assertTrue(reader.getHosts().contains("host3"));
    
  }
}
