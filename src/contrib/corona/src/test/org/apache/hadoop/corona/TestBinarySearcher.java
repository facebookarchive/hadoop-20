package org.apache.hadoop.corona;

import junit.framework.TestCase;

public class TestBinarySearcher extends TestCase {

  public void testComputingSqrtUsingBinarySearch() {
    double error = 0.01;
    for (int i = 1; i < 100; ++i) {
      assertEquals(Math.sqrt(i), mySqrt(i), error);
    }
  }

  public double mySqrt(int y) {
    BinarySearcher searcher = new BinarySearcher() {
      @Override
      protected double targetFunction(double x) {
        return x * x;
      }
    };
    return searcher.getSolution(y);
  }
}
