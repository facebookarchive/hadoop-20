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

import java.io.File;
import java.io.IOException;

/** Test ignoring the key when preparing input for the stream reducer */
public class TestStreamingIgnoreKey extends TestStreaming {

  public TestStreamingIgnoreKey() throws IOException {
    super();
  }

  protected String[] genArgs() {
    return new String[] {
        "-input", INPUT_FILE.getAbsolutePath(),
        "-output", OUTPUT_DIR.getAbsolutePath(),
        "-mapper", map,
        "-reducer", reduce,
        "-jobconf", "stream.tmpdir=" + System.getProperty("test.build.data","/tmp"),
        "-jobconf", "stream.reduce.input.ignoreKey=true"
    };
  }

  public void testCommandLine() throws IOException {
    createInput();
    boolean mayExist = false;
    StreamJob job = new StreamJob(genArgs(), mayExist);
    job.go();
    File outFile = new File(OUTPUT_DIR, "part-00000").getAbsoluteFile();
    String output = StreamUtil.slurp(outFile);
    outFile.delete();
    System.out.println("Output: " + output + " (length: " + output.length() + ")");
    assertEquals("R\t\n", output);
  }
}
