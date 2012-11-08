/**	
 * Licensed to the Apache Software Foundation (ASF) under one	
 * or more contributor license agreements. See the NOTICE file	
 * distributed with this work for additional information	
 * regarding copyright ownership. The ASF licenses this file	
 * to you under the Apache License, Version 2.0 (the	
 * "License"); you may not use this file except in compliance	
 * with the License. You may obtain a copy of the License at	
 *	
 * http://www.apache.org/licenses/LICENSE-2.0	
 *	
 * Unless required by applicable law or agreed to in writing, software	
 * distributed under the License is distributed on an "AS IS" BASIS,	
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.	
 * See the License for the specific language governing permissions and	
 * limitations under the License.	
 */
package org.apache.hadoop.hdfs.tools;

import static java.lang.System.err;
import java.io.*;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataDirFileReader{

  public static final Log LOG = LogFactory.getLog(DataDirFileReader.class);
  private String DirectoryFile;
  private String DirPaths;

  public DataDirFileReader(String dirFile) throws IOException{
    DirectoryFile = dirFile;
  }

  private void readFileToSet() throws IOException{
    File file;
    try {
      file = new File(DirectoryFile);
    } catch (Exception e) {
      LOG.error("Received exception: " + e);
      throw new IOException(e);
    }
    FileInputStream fis = new FileInputStream(file);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      //only the last line of the file is required since it contains the list
      // of working datadirs. Anything before is not required. 
      while ((line = reader.readLine()) != null) {
        DirPaths = line;
      }
    } catch (IOException e) {
      LOG.error("Received exception: " + e);
      throw new IOException(e);
    }
    finally {
      if(reader != null) {
        reader.close();
      }
      fis.close(); 
    }
  }

  public String getNewDirectories() {
    if(DirPaths != null) {
      return DirPaths;
    } else {
      try {
        this.readFileToSet();
        return DirPaths;
      } catch (Exception e) {
        //Unable to get the new directories so the string would be empty.
        LOG.error("Received exception: " + e);
        return null;
      }
    }
  }  
}
