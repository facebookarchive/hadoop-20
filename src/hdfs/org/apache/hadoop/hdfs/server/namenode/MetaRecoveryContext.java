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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Context data for an ongoing NameNode metadata recovery process. */
public final class MetaRecoveryContext  {
  public static final Log LOG = LogFactory.getLog(MetaRecoveryContext.class.getName());

  /**
   * Display a prompt to the user and get his or her choice.
   *  
   * @param prompt      The prompt to display
   * @param default     First choice (will be taken if autoChooseDefault is
   *                    true)
   * @param choices     Other choies
   *
   * @return            The choice that was taken
   * @throws IOException
   */
  public static String ask(String prompt, String firstChoice, String... choices) 
      throws IOException {
    while (true) {
      LOG.info(prompt);
      StringBuilder responseBuilder = new StringBuilder();
      while (true) {
        int c = System.in.read();
        if (c == -1 || c == '\r' || c == '\n') {
          break;
        }
        responseBuilder.append((char)c);
      }
      String response = responseBuilder.toString();
      if (response.equalsIgnoreCase(firstChoice))
        return firstChoice;
      for (String c : choices) {
        if (response.equalsIgnoreCase(c)) {
          return c;
        }
      }
      LOG.error("I'm sorry, I cannot understand your response.\n");
    }
  }

  public static void editLogLoaderPrompt(String prompt)
      throws IOException {
    LOG.error(prompt);
    String answer = ask("\nEnter 'c' to continue, \n" +
      "Enter 'q' to shutdown \n" +
      "(c/q)\n", "c", "q");
    if (answer.equals("c")) {
      LOG.info("Continuing");
      return;
    } else if (answer.equals("q")) {
      LOG.error("Exiting on user request.");
      System.exit(-1);
    } 
  }
}
