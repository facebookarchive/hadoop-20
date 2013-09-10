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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.fs.FsShell.CmdHandler;

class FsShellTouch {
  static String TOUCH_USAGE = "-touch [-acdmu] PATH...";

  protected static final SimpleDateFormat dateFmt =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static class TouchWorker {
    FileSystem fs;
    boolean createFiles;
    boolean setATime;
    boolean setMTime;
    Date newDate;
    Long unixTime;

    TouchWorker(FileSystem fs) {
      this.fs = fs;
      this.createFiles = true;
      this.setATime = false;
      this.setMTime = false;
      this.newDate = null;
      this.unixTime = null;
    }

    void touch(String src) throws IOException {
      Path f = new Path(src);
      fs = f.getFileSystem(fs.getConf());
      boolean justCreated = false;

      if (!fs.exists(f)) {         // file doesn't exist
        if (createFiles) {
          fs.create(f).close();
          justCreated = true;
        } else {
          throw new IOException("File doesn't exist: " + f);
        }
      }

      if (newDate == null && justCreated) {
        // We just created this file, no need to update its atime or mtime to now.
        return;
      }

      Long ut = null;
      if (newDate != null) {
        ut = newDate.getTime();
      }
      if (ut == null && unixTime != null) {
        ut = unixTime;
      }
      if (ut == null) {
        // Date was not specified. Using current date for each file.
        ut = new Date().getTime();
      }

      long atime = -1;
      long mtime = -1;
      if (setATime) {
        atime = ut;
      }
      if (setMTime) {
        mtime = ut;
      }
      fs.setTimes(f, mtime, atime);
    }
  }

  public static void touchFiles(FileSystem fs, String argv[], int startIndex)
                                throws IOException {
    TouchWorker worker = new TouchWorker(fs);

    // Parsing command line options
    for (; startIndex < argv.length && argv[startIndex].startsWith("-");
         ++startIndex) {
      String arg = argv[startIndex];

      if (arg.startsWith("--")) { // long option
        if (arg.equals("--no-create")) {
              worker.createFiles = false;
        } else if (arg.startsWith("--date")) {
          String dateArg = null;
          int dateLength = "--date".length();
          if (arg.length() > dateLength + 1 && arg.charAt(dateLength) == '=') {
            dateArg = arg.substring(dateLength + 1);
          } else if (arg.length() == dateLength && startIndex + 1 < argv.length) {
            ++startIndex;
            dateArg = argv[startIndex];
          } else {
            throw new IOException("expected date after '--date' option");
          }

          try {
            worker.newDate = dateFmt.parse(dateArg);
          } catch (ParseException pe) {
            throw new IOException("Unable to parse date: " + argv[startIndex]);
          }
        } else {
          throw new IOException("invalid option: " + arg);
        }
      } else {
        for (int i = 1; i < arg.length(); ++i) {
          switch(arg.charAt(i)) {
            case 'a':
              worker.setATime = true;
              break;
            case 'c':
              worker.createFiles = false;
              break;
            case 'u':
              if (i != arg.length() - 1 || startIndex == argv.length - 1) {
                throw new IOException("expected timestamp after 'u' option");
              }
              ++startIndex;
              worker.unixTime = Long.parseLong(argv[startIndex]);
              break;
            case 'd':
              if (i != arg.length() - 1 || startIndex == argv.length - 1) {
                throw new IOException("expected date after 'd' option");
              }
              ++startIndex;
              try {
                worker.newDate = dateFmt.parse(argv[startIndex]);
              } catch (ParseException pe) {
                throw new IOException("Unable to parse date: " + argv[startIndex]);
              }
              break;
            case 'm':
              worker.setMTime = true;
              break;
            default:
              throw new IOException("invalid option: " + arg.charAt(i));
          }
        }
      }
    }
    if (!worker.setATime && !worker.setMTime) {
      worker.setATime = true;
      worker.setMTime = true;
    }

    for (; startIndex < argv.length; ++startIndex) {
      worker.touch(argv[startIndex]);
    }
  }

}
