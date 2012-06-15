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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.util.ProgramDriver;

public class AllTestDriver {

  /**
   * A description of the test program for running all the tests using jar file
   */
  public static void main(String argv[]){
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("gentest", DFSGeneralTest.class, "A map/reduce benchmark that supports running multi-thread operations in multiple machines");
      pgd.addClass("locktest", DFSLockTest.class, "A benchmark that spawns many threads and each thread run many configurable read/write FileSystem operations to test FSNamesystem lock's concurrency.");
      pgd.addClass("dirtest", DFSDirTest.class, "A map/reduce benchmark that creates many jobs and each job spawns many threads and each thread create/delete many dirs.");
      pgd.addClass("dfstest", DFSIOTest.class, "A map/reduce benchmark that creates many jobs and each jobs can create many files to test i/o rate per task of hadoop cluster.");
      pgd.addClass("structure-gen", StructureGenerator.class, "Create a structure of files and directories as an input for data-gen");
      pgd.addClass("data-gen", DataGenerator.class, "Create files and directories on cluster as inputs for load-gen");
      pgd.addClass("load-gen", LoadGenerator.class, "A tool to test the behavior of NameNode with different client loads.");
      pgd.addClass("testnn", TestNNThroughputBenchmark.class, "Test the behavior of the namenode on localhost." +
          " Here namenode is real and others are simulated");
      pgd.driver(argv);
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }
}
