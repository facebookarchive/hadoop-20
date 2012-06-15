package org.apache.hadoop.hdfs.util;

import junit.framework.TestCase;

public class TestPosixPathNameChecker extends TestCase {

  public static void doTestIsValidPosixFileName(PosixPathNameChecker checker) {
    assertTrue(checker.isValidPosixFileName("test1"));
    assertFalse(checker.isValidPosixFileName("-test1"));
    assertTrue(checker.isValidPosixFileName("test-1"));
    assertTrue(checker.isValidPosixFileName("test.-1"));
    assertTrue(checker.isValidPosixFileName("tEsT.1-.TesT"));
  }

  public static void doTestIsValidPosixPath(PosixPathNameChecker checker) {

    assertTrue(checker.isValidPath("/abs/ddd.dd"));
    assertFalse(checker.isValidPath("//abs/ddd.dd"));
    assertFalse(checker.isValidPath("/abs:/ddd.dd"));
    assertFalse(checker.isValidPath("/abs///ddd.dd"));
    assertTrue(checker.isValidPath("/test-1/test.-1/tEsT.1-.TesT"));
    assertFalse(checker.isValidPath("/*&343-!@#"));
  }

  public void testIsValidPosixPath() {
    PosixPathNameChecker checker = new PosixPathNameChecker();
    doTestIsValidPosixFileName(checker);
    doTestIsValidPosixPath(checker);
  }
}
