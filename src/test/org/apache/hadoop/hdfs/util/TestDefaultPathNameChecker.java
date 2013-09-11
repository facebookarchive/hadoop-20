package org.apache.hadoop.hdfs.util;

import junit.framework.TestCase;

public class TestDefaultPathNameChecker extends TestCase {

  public static void doTestIsValidPath(DefaultPathNameChecker d) {
    assertFalse(d.isValidPath("abc"));
    assertFalse(d.isValidPath("//abc"));
    assertTrue(d.isValidPath("/abc/dd"));
    assertTrue(d.isValidPath("/a.bc/dd"));
    assertTrue(d.isValidPath("/*&343-!@#"));
    assertFalse(d.isValidPath("/abc/../dd"));
    assertFalse(d.isValidPath("/abc/./dd"));
    assertFalse(d.isValidPath("/:abc/dd"));
    assertFalse(d.isValidPath("/abc//dd"));
    assertTrue(d.isValidPath("/test-1/test.-1/tEsT.1-.TesT"));

    // No newlines and tabs allowed.
    assertFalse(d.isValidPath("/abc\n/dd"));
    assertFalse(d.isValidPath("/abc\r/dd"));
    assertFalse(d.isValidPath("/abc/\tdd"));
  }

  public void testisValidPath() {
    DefaultPathNameChecker d = new DefaultPathNameChecker();
    doTestIsValidPath(d);

  }
}
