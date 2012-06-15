package org.apache.hadoop.hdfs;
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

public interface DirConstant {
  public static final long NSUBTASKS = 100;
  public static final long NTHREADS = 20; 

  public static final String ROOT = "/dirbenchmark/";
  public static final String TRASH = ROOT + ".Trash/";
  public static final String INPUT = ROOT + "input/";
  public static final String DFS_INPUT = ROOT + "dirtest-input/";
  public static final String DFS_OUTPUT = ROOT + "dirtest-output/";
  public static final String OUTPUT = ROOT + "output/";
}
