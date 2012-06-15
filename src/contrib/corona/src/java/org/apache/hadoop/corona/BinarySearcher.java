/*
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

package org.apache.hadoop.corona;

/**
 * Given a targetFunction and a targetValue, find a positive number x so that
 * targetFunction(x) == targetValue approximately
 */
public abstract class BinarySearcher {
  /** Maximum iterations of the target function to find the solution */
  private static final int MAXIMUM_ITERATION = 25;
  /** Maximum about of error to allow when comparing duobles */
  private static final double ERROR_ALLOW_WHEN_COMPARE_DOUBLES = 0.01;

  /**
   * Subclasses of the Binary searcher implement the target function as
   * f(x) which needs to be solved.
   *
   * For example to solve x^2 = y the target function should return x * x
   *
   * @param x x passed into f(x)
   * @return the value of f(x)
   */
  protected abstract double targetFunction(double x);

  /**
   * Find a solution to f(x) = targetValue taking the initial guess as -1
   *
   * @param targetValue the value to solve against
   * @return the solution
   */
  public double getSolution(double targetValue) {
    return getSolution(targetValue, -1);
  }

  /**
   * Find the solution x for the target function f(x) such that
   * f(x) == targetValue
   *
   * @param targetValue the value to solve against
   * @param initGuess initial guess to take when performing binary search
   * @return the solution (x)
   */
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
    double left = 0;
    double right = rMax;
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

  /**
   * Check if the two doubles are close enough to consider them equal
   * in the context of solving the equation.
   *
   * @param x first double
   * @param y second double
   * @return true if Math.abs(x-y) is less than threshold, false otherwise
   */
  private static boolean equals(double x, double y) {
    return Math.abs(x - y) < ERROR_ALLOW_WHEN_COMPARE_DOUBLES;
  }

}
