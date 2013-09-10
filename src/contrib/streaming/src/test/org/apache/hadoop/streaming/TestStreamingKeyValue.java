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

package org.apache.hadoop.streaming;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class tests hadoopStreaming in MapReduce local mode.
 * This testcase looks at different cases of tab position in input. 
 */
public class TestStreamingKeyValue extends TestStreaming
{
  static final Log LOG = LogFactory.getLog(TestStreamingKeyValue.class);
  
  protected File INPUT_FILE = new File("input.txt");
  protected File OUTPUT_DIR = new File("stream_out");
  // First line of input has 'key' 'tab' 'value'
  // Second line of input starts with a tab character. 
  // So, it has empty key and the whole line as value.
  // Third line of input does not have any tab character.
  // So, the whole line is the key and value is empty.
  protected String input = 
    "roses are \tred\t\n\tviolets are blue\nbunnies are pink\n" +
    "this is for testing a big\tinput line\n" +
    "small input\n";
  protected String outputExpect = 
    "\tviolets are blue\nbunnies are pink\t\n" + 
    "roses are \tred\t\n" +
    "small input\t\n" +
    "this is for testing a big\tinput line\n";

  public TestStreamingKeyValue() throws IOException
  {
    super();
    setInputFile(INPUT_FILE);
    setOutputDir(OUTPUT_DIR);
    setInputString(input);
    setExpectedOutput(outputExpect);
  }

  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", "cat",
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.non.zero.exit.is.failure=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
  }
  
  public static void main(String[]args) throws Exception
  {
    new TestStreamingKeyValue().testCommandLine();
  }

}
