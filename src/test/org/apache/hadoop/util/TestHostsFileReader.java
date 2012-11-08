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
package org.apache.hadoop.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import junit.framework.TestCase;




/*
 * Test for HostsFileReader.java
 * 
 */

public class TestHostsFileReader extends TestCase {

	// Using /test/build/data/tmp directory to store temprory files
	final String HOSTS_TEST_DIR = new File(System.getProperty(
			"test.build.data", "/tmp")).getAbsolutePath();

	File EXCLUDES_FILE = new File(HOSTS_TEST_DIR, "dfs.exclude");
	File INCLUDES_FILE = new File(HOSTS_TEST_DIR, "dfs.include");

	String excludesFile = HOSTS_TEST_DIR + "/dfs.exclude";
	String includesFile = HOSTS_TEST_DIR + "/dfs.include";
	private Set<String> includes;
	private Set<String> excludes;

	public void setUp() throws Exception {

	}

	public void tearDown() throws Exception {
		// Delete test files after running tests
		EXCLUDES_FILE.delete();
		INCLUDES_FILE.delete();

	}

	/*
	 * 1.Create dfs.exclude,dfs.include file 
	 * 2.Write host names per line 
	 * 3.Write comments starting with #
	 * 4.Close file 
	 * 5.Compare if number of hosts reported by HostsFileReader
	 *   are equal to the number of hosts written
	 */
	public void testHostsFileReader() throws Exception {

		FileWriter efw = new FileWriter(excludesFile);
		FileWriter ifw = new FileWriter(includesFile);

		efw.write("#DFS-Hosts-excluded\n");
		efw.write("somehost1\n");
		efw.write("#This-is-comment\n");
		efw.write("somehost2\n");
		efw.write("somehost3 # host3\n");
		efw.write("somehost4\n");
		efw.write("somehost4 somehost5\n");
		efw.close();

		ifw.write("#Hosts-in-DFS\n");
		ifw.write("somehost1\n");
		ifw.write("somehost2\n");
		ifw.write("somehost3\n");
		ifw.write("#This-is-comment\n");
		ifw.write("somehost4 # host4\n");
		ifw.write("somehost4 somehost5\n");
		ifw.close();

		HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

		int includesLen = hfp.getHosts().size();
		int excludesLen = hfp.getExcludedHosts().size();

		assertEquals(5, includesLen);
		assertEquals(5, excludesLen);

		assertTrue(hfp.getHosts().contains("somehost5"));
		assertFalse(hfp.getHosts().contains("host3"));

		assertTrue(hfp.getExcludedHosts().contains("somehost5"));
		assertFalse(hfp.getExcludedHosts().contains("host4"));

	}

	/*
	 * Test for null file
	 */
	public void testHostFileReaderWithNull() throws Exception {
		FileWriter efw = new FileWriter(excludesFile);
		FileWriter ifw = new FileWriter(includesFile);

		efw.close();

		ifw.close();

		HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

		int includesLen = hfp.getHosts().size();
		int excludesLen = hfp.getExcludedHosts().size();

		// TestCase1: Check if lines beginning with # are ignored
		assertEquals(0, includesLen);
		assertEquals(0, excludesLen);

		// TestCase2: Check if given host names are reported by getHosts and
		// getExcludedHosts
		assertFalse(hfp.getHosts().contains("somehost5"));

		assertFalse(hfp.getExcludedHosts().contains("somehost5"));
	}

	/*
	 * Check if only comments can be written to hosts file
	 */
	public void testHostFileReaderWithCommentsOnly() throws Exception {
		FileWriter efw = new FileWriter(excludesFile);
		FileWriter ifw = new FileWriter(includesFile);

		efw.write("#DFS-Hosts-excluded\n");
		efw.close();

		ifw.write("#Hosts-in-DFS\n");
		ifw.close();

		HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

		int includesLen = hfp.getHosts().size();
		int excludesLen = hfp.getExcludedHosts().size();

		assertEquals(0, includesLen);
		assertEquals(0, excludesLen);

		assertFalse(hfp.getHosts().contains("somehost5"));

		assertFalse(hfp.getExcludedHosts().contains("somehost5"));

	}

	/*
	 * Test if spaces are allowed in host names
	 */
	public void testHostFileReaderWithSpaces() throws Exception {
		FileWriter efw = new FileWriter(excludesFile);
		FileWriter ifw = new FileWriter(includesFile);

		efw.write("#DFS-Hosts-excluded\n");
		efw.write("   somehost somehost2");
		efw.write("   somehost3 # somehost4");
		efw.close();

		ifw.write("#Hosts-in-DFS\n");
		ifw.write("   somehost somehost2");
		ifw.write("   somehost3 # somehost4");
		ifw.close();

		HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

		int includesLen = hfp.getHosts().size();
		int excludesLen = hfp.getExcludedHosts().size();

		assertEquals(3, includesLen);
		assertEquals(3, excludesLen);

    assertTrue(hfp.getHosts().contains("somehost3"));
		assertFalse(hfp.getHosts().contains("somehost5"));
    assertFalse(hfp.getHosts().contains("somehost4"));
    
    assertTrue(hfp.getExcludedHosts().contains("somehost3"));
		assertFalse(hfp.getExcludedHosts().contains("somehost5"));
		assertFalse(hfp.getExcludedHosts().contains("somehost4"));

	}

	/*
	 * Test if spaces , tabs and new lines are allowed
	 */

	public void testHostFileReaderWithTabs() throws Exception {
		FileWriter efw = new FileWriter(excludesFile);
		FileWriter ifw = new FileWriter(includesFile);

		efw.write("#DFS-Hosts-excluded\n");
		efw.write("     \n");
		efw.write("   somehost \t somehost2 \n somehost4");
		efw.write("   somehost3 \t # somehost5");
		efw.close();

		ifw.write("#Hosts-in-DFS\n");
		ifw.write("     \n");
		ifw.write("   somehost \t  somehost2 \n somehost4");
		ifw.write("   somehost3 \t # somehost5");
		ifw.close();

		HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

		int includesLen = hfp.getHosts().size();
		int excludesLen = hfp.getExcludedHosts().size();

		assertEquals(4, includesLen);
		assertEquals(4, excludesLen);

    assertTrue(hfp.getHosts().contains("somehost2"));
		assertFalse(hfp.getHosts().contains("somehost5"));

    assertTrue(hfp.getExcludedHosts().contains("somehost2"));
		assertFalse(hfp.getExcludedHosts().contains("somehost5"));

	}
	
  public void testHostsFileReaderValidate() {

    // test constructor and update
    testConstructAndSet(true);
    testConstructAndSet(false);

    // validation should throw an exception
    testFailure("/foo/bar/include", "/foo/bar/exclude");
    testFailure("", "/foo/bar/exclude");
    testFailure("/foo/bar/include", "");
    
    try {
      HostsFileReader.validateHostFiles("", "");
    } catch (IOException e) {
      fail("Validation for empty files should succeed");
    }
  }
  
  private void testConstructAndSet(boolean enforce) {
    HostsFileReader hfp = null;
    try {
      hfp = new HostsFileReader("/foo/bar/include", "/foo/bar/exclude", enforce);
      if (enforce)
        fail("This should fail since the files do not exist");
    } catch (IOException e) {
      if (enforce)
        assertTrue(e.toString().contains("does not exist"));
      else
        fail("This should succeed since we don't enforce validation");
    }

    try {
      hfp = new HostsFileReader("", "");
      hfp.updateFileNames("/foo/bar/include", "/foo/bar/exclude", enforce);
      if (enforce)
        fail("This should fail since the files do not exist");
    } catch (IOException e) {
      if (enforce)
        assertTrue(e.toString().contains("does not exist"));
      else
        fail("This should succeed since we don't enforce validation");
    }

  }
  
  private void testFailure(String in, String ex) {
    try {
      HostsFileReader.validateHostFiles(in, ex);
      fail("This should fail since the files do not exist (either of them)");
    } catch (IOException e) {
      assertTrue(e.toString().contains("does not exist"));
    }
  }

}
