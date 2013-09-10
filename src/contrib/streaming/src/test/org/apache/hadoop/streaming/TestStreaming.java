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

import junit.framework.TestCase;
import java.io.*;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;


/**
 * This class tests hadoopStreaming in MapReduce local mode.
 */
public class TestStreaming extends TestCase
{
  static final Log LOG = LogFactory.getLog(TestStreaming.class);
  
  // "map" command: grep -E (red|green|blue)
  // reduce command: uniq
  protected String inputOutputPrefix =
      System.getProperty("test.build.data", "/tmp") + "/" + UUID.randomUUID().toString();
  protected File INPUT_FILE = new File(inputOutputPrefix + "_input.txt");
  protected File OUTPUT_DIR = new File(inputOutputPrefix + "_out");
  protected String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  // map behaves like "/usr/bin/tr . \\n"; (split words into lines)
  protected String map = StreamUtil.makeJavaCommand(TrApp.class, new String[]{".", "\\n"});
  // reduce behave like /usr/bin/uniq. But also prepend lines with R.
  // command-line combiner does not have any effect any more.
  protected String reduce = StreamUtil.makeJavaCommand(UniqApp.class, new String[]{"R"});
  protected String outputExpect = "Rare\t\nRblue\t\nRbunnies\t\nRpink\t\nRred\t\nRroses\t\nRviolets\t\n";
  protected boolean testBytesWritable = true;
  
  protected StreamJob job;

  public TestStreaming() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
  }

  protected void setExpectedOutput(String out) {
    outputExpect = out;
  }

  protected void setInputFile(File input) {
    INPUT_FILE = input;
  }
  
  protected void setOutputDir(File output) {
    OUTPUT_DIR = output;
  }
  
  protected void setInputString(String in) {
    input= in;
  }
  
  protected void setTestBytesWritable(boolean test) {
    testBytesWritable = test;
  }
 
  protected void createInput() throws IOException
  {
    DataOutputStream out = new DataOutputStream(
                                                new FileOutputStream(INPUT_FILE.getAbsoluteFile()));
    out.write(input.getBytes("UTF-8"));
    out.close();
  }
  
  /**
   * Different test cases should just provide a customized
   * args and optionaly INPUT_FILE, OUTPUT_DIR, input string
   * and expected output.
   * @return
   */
  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", map,
      "-reducer", reduce,
      //"-verbose",
      //"-jobconf", "stream.debug=set"
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
  }
  
  public void testCommandLine() throws IOException
  {
    // 1st test case:
    //  Both Key and Value are Text
    String[] textArgs = genArgs();
    testCommandLineInternal(textArgs);
    
    // 2nd Test case:
    //   Map output key/value be BytesWritable and
    //   turn on blocked map output sort
    if (testBytesWritable) {
      String[] bytesArgs = addArgs(textArgs, 
        new String[] {
          "-jobconf", "stream.key_value.class=BytesWritable",
          "-jobconf", "mapred.map.output.blockcollector=true"});
      testCommandLineInternal(bytesArgs);
    }
  }

  String[] addArgs(String[] args, String[] more_args) {
    String[] newargs = new String[args.length + more_args.length];
    System.arraycopy(args, 0, newargs, 0, args.length);
    System.arraycopy(more_args, 0, newargs, args.length, more_args.length);
    return newargs;
  }
 
  protected void testCommandLineInternal(String[] args) throws IOException
  {
    try {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (Exception e) {
      }

      createInput();
      boolean mayExit = false;

      // During tests, the default Configuration will use a local mapred
      // So don't specify -config or -cluster
      job = new StreamJob(args, mayExit);      
      job.go();
      File outFile = new File(OUTPUT_DIR, "part-00000").getAbsoluteFile();
      String output = StreamUtil.slurp(outFile);
      outFile.delete();
      System.err.println("outEx1=" + outputExpect);
      System.err.println("  out1=" + output);
      assertEquals(outputExpect, output);
    } finally {
      INPUT_FILE.delete();
      FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
    }
  }

  public static void main(String[]args) throws Exception
  {
    new TestStreaming().testCommandLine();
  }
}
