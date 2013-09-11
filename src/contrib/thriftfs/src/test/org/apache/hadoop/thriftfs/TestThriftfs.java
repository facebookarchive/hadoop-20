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

package org.apache.hadoop.thriftfs;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.thrift.TException;


/**
 * This class is supposed to test ThriftHadoopFileSystem but has a long long
 * way to go.
 */
public class TestThriftfs extends TestCase
{
  final static int numDatanodes = 1;

  public TestThriftfs() throws IOException
  {
  }

  public void testServer() throws Exception
  {
    final HadoopThriftServer server = new HadoopThriftServer(new String[0]);
    Thread t = new Thread(new Runnable() {
      public void run() {
        try {
          server.server.serve();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    t.start();
    Thread.sleep(1000);
    server.server.stop();
  }
}
