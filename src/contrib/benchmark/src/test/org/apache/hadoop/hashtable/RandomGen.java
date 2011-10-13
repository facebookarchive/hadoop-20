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

package org.apache.hadoop.hashtable;

import java.util.Random;

public class RandomGen {

  private int which = 0;

  private Random randJ;
  private MT randMT;
  private R250_521 randR;

  public RandomGen(int which) {
    this.which = which;
    randJ = new Random();
    randMT = new MT();
    randR = new R250_521();
  }

  public long next() {
    switch (which) {
    case 1:
      return randJ.nextLong();
    case 2:
      return randMT.random() * randMT.random();
    case 3:
      return randR.random() * randR.random();
    default:
      return 0;
    }
  }
}
