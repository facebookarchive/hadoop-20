/*
 * ContextFactory.java
 *
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

package org.apache.hadoop.metrics;

import java.io.FileWriter;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.File;
import java.net.URL;
import org.junit.Test;
import org.junit.Assert;

public class TestContextFactory {
  final static String SEPARATOR = System.getProperty("file.separator");

  @Test
  public void testPropertiesFileLoad() throws Exception {
    BufferedWriter propertiesWriter = null;
    BufferedWriter propertiesCustomWriter = null;
    try {
      // Prepare .properties files
      propertiesWriter = getWriter(ContextFactory.PROPERTIES_FILE);
      propertiesWriter.write("property1 = value1");
      propertiesWriter.newLine();
      propertiesWriter.write("property2 = value2");
      propertiesWriter.newLine();
      propertiesWriter.flush();

      propertiesCustomWriter = getWriter(ContextFactory.PROPERTIES_FILE_CUSTOM);
      propertiesCustomWriter.write("property1 = value3");
      propertiesCustomWriter.newLine();
      propertiesCustomWriter.write("property3 = value4");
      propertiesCustomWriter.newLine();
      propertiesCustomWriter.flush();

      // Verify ContextFactory
      ContextFactory factory = ContextFactory.getFactory();
      Assert.assertEquals(
          factory.getAttribute("property1"), "value3"); // rewritten
      Assert.assertEquals(
          factory.getAttribute("property2"), "value2"); // the same
      Assert.assertEquals(
          factory.getAttribute("property3"), "value4"); // added
    } finally {
      if (propertiesCustomWriter != null) {
        propertiesCustomWriter.close();
      }
      if (propertiesWriter != null) {
        propertiesWriter.close();
      }
    }
  }

  private BufferedWriter getWriter(String propertiesFile) throws Exception {
    URL url = getClass().getResource(SEPARATOR);
    String path = url.getPath();
    File file = new File(path + propertiesFile);
    file.deleteOnExit();
    return new BufferedWriter(
        new FileWriter(path + propertiesFile));
  }
}
