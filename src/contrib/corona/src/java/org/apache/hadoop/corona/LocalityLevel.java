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
 * Describes the locality of scheduling.
 */
public enum LocalityLevel {
  /** Same node */
  NODE (0),
  /** Same rack */
  RACK (1),
  /** No locality specified */
  ANY  (2);

  /** Locality level */
  private final int levelNumber;

  /**
   * Construct a locality level with an associated level number
   *
   * @param levelNumber Level of this locaility
   */
  private LocalityLevel(int levelNumber) {
    this.levelNumber = levelNumber;
  }

  /**
   * Simple comparator of locality levels based on the internal level number.
   *
   * @param other Other locality level to compare to
   * @return True if other has a higher level number, false otherwise
   */
  public boolean isBetterThan(LocalityLevel other) {
    return this.levelNumber < other.levelNumber;
  }
}
