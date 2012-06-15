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

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

public class TestNNThroughputBenchmark {

	/**
	 * This test runs all benchmarks defined in {@link NNThroughputBenchmark}.
	 * @throws Exception
	 */
	public static void main(String[] arg) throws Exception {

		// make the configuration before benchmark
		Configuration conf = new Configuration();
		FileSystem.setDefaultUri(conf, "hdfs://0.0.0.0:" + 9000);
		conf.set("dfs.http.address", "0.0.0.0:0");
		Random rand = new Random();
		String dir = "/tmp/testNN" + rand.nextInt(Integer.MAX_VALUE);
		conf.set("dfs.name.dir", dir);
		conf.set("dfs.name.edits.dir", dir);
		conf.set("dfs.namenode.support.allowformat", "true");
		//conf.set("fs.default.name", "hdfs://0.0.0.0:9000");
		NameNode.format(conf);
		// create the first benchmark
		NNThroughputBenchmark.runBenchmark(conf, Arrays.asList(arg));
	}
}
