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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalClosure;

/**
 * JournalSetWorker applies JournalClosure operation on JournalAndStream. It is
 * submitted as a task to an executor, the call method return the journal in
 * case of failure, so we can handle it outside.
 */
class JournalSetWorker implements Callable<JournalAndStream> {

  static final Log LOG = LogFactory.getLog(JournalSetWorker.class);

  private JournalAndStream jas;
  private JournalClosure closure;
  private String status;

  JournalSetWorker(JournalAndStream jas, JournalClosure closure, String status) {
    this.jas = jas;
    this.closure = closure;
    this.status = status;
  }

  @Override
  public JournalAndStream call() throws Exception {
    try {
      closure.apply(jas);
    } catch (Throwable t) {
      LOG.error("Error: " + status + " failed for (journal " + jas + ")", t);
      // pass it back to handle as a failed journal
      return jas;
    }
    // if the call was successful, just return null;
    return null;
  }
}
