package org.apache.hadoop.hdfs.util;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;

public class TestPathValidator extends TestCase {

  public void testValidPosixPath() {
    Configuration conf = new Configuration();
    conf.setClass("dfs.util.pathname.checker.class", PosixPathNameChecker
      .class, PathNameChecker.class);
    PathValidator pathValidator = new PathValidator(conf);
    TestPosixPathNameChecker.doTestIsValidPosixPath(
      (PosixPathNameChecker) pathValidator.getNameChecker());
  }

  public void testDefaultPath() {
    Configuration conf = new Configuration();
    conf.setClass("dfs.util.pathname.checker.class", DefaultPathNameChecker
      .class, PathNameChecker.class);
    PathValidator pathValidator = new PathValidator(conf);
    TestDefaultPathNameChecker.doTestIsValidPath(
      (DefaultPathNameChecker) pathValidator.getNameChecker());
  }
}
