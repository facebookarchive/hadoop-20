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

package org.apache.hadoop.hdfs.notifier.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.notifier.benchmark.TxnConsumer;
import org.apache.hadoop.hdfs.notifier.benchmark.TxnGenerator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NotifierShell extends Configured implements Tool{

  public static final Log LOG = LogFactory.getLog(NotifierShell.class);
  private final PrintStream out;
  private Configuration conf;
  
  /**
   * Start RaidShell.
   * <p>
   * The RaidShell connects to the specified RaidNode and performs basic
   * configuration options.
   * @throws IOException
   */
  public NotifierShell(Configuration conf) throws IOException {
    this(conf, System.out);
  }
  
  public NotifierShell(Configuration conf, PrintStream out) throws IOException {
    super(conf);
    this.conf = conf;
    this.out = out;
  }
  
  public void close() {
    
  }
  
  @Override
  public int run(String[] args) throws Exception {
    int i = 0;
    String cmd = args[i++];
    
    if ("-generatetxn".equals(cmd)) {
      TxnGenerator generator = new TxnGenerator(conf);
      generator.start(args, i);
    } else if ("-consumetxn".equals(cmd)) {
      TxnConsumer consumer = new TxnConsumer(conf);
      consumer.start(args, i);
    }
    return 0;
  }

  public static void main(String argv[]) throws Exception {
    NotifierShell shell = null;
    
    try {
      shell = new NotifierShell(new Configuration());
      int res = ToolRunner.run(shell, argv);
      System.exit(res);
    } finally {
      if (shell != null) {
        shell.close();
      }
    }
  }
}
