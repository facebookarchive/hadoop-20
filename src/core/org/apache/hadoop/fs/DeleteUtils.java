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
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;;

/**
 * main logic of delete a file
 */
public class DeleteUtils {  
  /* delete a file */
  static public void delete(Configuration conf, Path src, FileSystem srcFs, boolean recursive,
                      boolean skipTrash, boolean ignoreNonEmpty,
                      boolean onlyDeleteFile) throws IOException {
    FileStatus fs = null;
    try {
      fs = srcFs.getFileStatus(src);
    } catch (FileNotFoundException fnfe) {
      // Have to re-throw so that console output is as expected
      throw new FileNotFoundException("cannot remove "
          + src + ": No such file or directory.");
    }

    if (fs.isDir() && !recursive) {
      // We may safely delete empty directories if
      // the recursive option is not specified
      FileStatus children[] = srcFs.listStatus(src);
      if (onlyDeleteFile || (children != null && children.length != 0)) {
        if (ignoreNonEmpty) {
          return;
        } else {
          throw new IOException("Cannot remove directory \"" + src + "\"," +
              " use -rmr instead");
        }
      } else if (children == null) {
        throw new IOException(src + " no longer exists");
      }
    }

    if(!skipTrash) {
      try {
        Trash trashTmp = new Trash(srcFs, conf);
        if (trashTmp.moveToTrash(src)) {
          System.err.println("Moved to trash: " + src);
          return;
        }
      } catch (IOException e) {
        Exception cause = (Exception) e.getCause();
        String msg = "";
        if(cause != null) {
          msg = cause.getLocalizedMessage();
        }
        System.err.println("Problem with Trash." + msg +". Consider using -skipTrash option");
        throw e;
      }
    }

    if (srcFs.delete(src, recursive, true)) {
      System.err.println("Deleted " + src);
    } else {
      throw new IOException("Delete failed " + src);
    }
  }
  
  public static boolean isTempPath(Configuration conf, String file) {
    // TODO: implement a version that accepts a list of path patterns, instead
    // of prefixes. The current implementation is temporary and
    // inefficient. Several string copying and object creations can be avoided.
    return isTempPath(getTempPaths(conf), file);
  }
  
  static String[] getTempPaths(Configuration conf) {
    return conf.getStrings("fs.trash.tmp.paths", "/tmp");
  }

  static boolean isTempPath(String[] tempPathList, String file) {
    for (String tmpPath : tempPathList) {
      String cleanedTmpPath = tmpPath.trim();
      while (cleanedTmpPath.endsWith("/")) {
        cleanedTmpPath = cleanedTmpPath.substring(0,
            cleanedTmpPath.length() - 1);
      }
      if (file.equals(cleanedTmpPath) || file.indexOf(cleanedTmpPath + "/") == 0) {
        return true;
      }
    }
    return false;
  }
}
