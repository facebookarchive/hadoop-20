package org.apache.hadoop.corona;

/**
 * Given a targetFunction and a targetValue, find a positive number x so that
 * targetFunction(x) == targetValue approximately
 */
abstract public class BinarySearcher {

  private final static int MAXIMUM_ITERATION = 25;
  private final static double ERROR_ALLOW_WHEN_COMPARE_FLOATS = 0.01;

  abstract protected double targetFunction(double x);

  public double getSolution(double targetValue) {
    return getSolution(targetValue, -1);
  }

  public double getSolution(double targetValue, double initGuess) {
    double rMax = 1.0;
    double oldValue = -1;
    for (int i = 0; i < MAXIMUM_ITERATION; ++i) {
      double value = targetFunction(rMax);
      if (value >= targetValue) {
        break;
      }
      if (equals(value, oldValue)) {
        return rMax; // Target value is not feasible. Just return rMax
      }
      rMax *= 2;
    }
    double left = 0, right = rMax;
    double mid = initGuess > left && initGuess < right ?
        initGuess : (left + right) / 2.0;
    for (int i = 0; i < MAXIMUM_ITERATION; ++i) {
      double value = targetFunction(mid);
      if (equals(value, targetValue)) {
        return mid;
      }
      if (value < targetValue) {
        left = mid;
      } else {
        right = mid;
      }
      mid = (left + right) / 2.0;
    }
    return right;
  }
  private static boolean equals(double x, double y) {
    return Math.abs(x - y) < ERROR_ALLOW_WHEN_COMPARE_FLOATS;
  }

}
