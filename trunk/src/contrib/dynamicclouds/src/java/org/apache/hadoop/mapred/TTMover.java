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
package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author dms
 */
public class TTMover extends Configured implements Tool {

  public static final String COMMENT = "# added by dynamic cloud balancer";
  private AdminOperationsProtocol adminOperationsProtocol;

  private void displayUsage() {
    System.out.println("Usage: TTMover <command> <args>");
    System.out.println("\t[-restore]");
    System.out.println("\t[-remove hostname]");
    System.out.println("\t[-add hostname]");
  }

  public int run(String[] args) throws Exception {
    this.getConf().addResource("mapred-site.xml");
    this.getConf().reloadConfiguration();


    int exitCode = -1;

    if (args.length < 1) {
      displayUsage();
      return exitCode;
    }

    if ("-restore".equals(args[0])) {
      if (args.length > 1) {
        displayUsage();
        return exitCode;
      }
      exitCode = restore();
    } else if ("-remove".equals(args[0])) {
      if (args.length != 2) {
        displayUsage();
        return exitCode;
      }
      String hostName = args[1];
      // Remove hostName from the JobTracker running in this env
      exitCode = removeHost(hostName);
    } else if ("-add".equals(args[0])) {
      if (args.length != 2) {
        displayUsage();
        return exitCode;
      }
      String hostName = args[1];
      // Add hostName to the JobTracker in this environment
      exitCode = addHost(hostName);
    } else {
      displayUsage();
    }

    return exitCode;
  }

  public String getFileLocation(String fileName) throws IOException {
    if (fileName == null) {
      throw new NullPointerException();
    }
    File file = new File(fileName);
    if (file.exists()) {
      return file.getAbsolutePath();
    }
    URL fileURL = JobTracker.class.getClassLoader().
            getResource(fileName);
    if (fileURL == null) {
      throw new FileNotFoundException(fileName);
    }
    return fileURL.getFile();
  }

  int restoreFile(String fileLocation) {
    int exitCode = 0;

    File file = new File(fileLocation);
    BufferedReader reader = null;
    FileWriter writer = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      String fileLine;
      List<String> lines = new LinkedList<String>();

      while ((fileLine = reader.readLine()) != null) {
        if (fileLine.trim().startsWith(COMMENT)) {
          lines.add(fileLine.substring(COMMENT.length()));
          continue;
        } else if (fileLine.trim().endsWith(COMMENT)) {
          continue;
        }
        lines.add(fileLine);
      }

      writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line);
        writer.write("\n");
      }
    } catch (IOException ex) {
      exitCode = 1;
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
        if (writer != null) {
          writer.close();
        }
      } catch (IOException ioex) {
        // We did all we could to close
      }
    }

    return exitCode;
  }

  int removeHostFromFile(String fileLocation, String hostName) {
    int exitCode = 0;
    File file = new File(fileLocation);
    try {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String fileLine;
      List<String> lines = new LinkedList<String>();
      while ((fileLine = reader.readLine()) != null) {
        if (fileLine.trim().startsWith(hostName)) {
          if (!fileLine.contains(COMMENT)) {
            // Comment it out unless this tool added it in the first place
            // if so - lose the line
            lines.add(COMMENT + " " + fileLine);
          }
        } else {
          lines.add(fileLine);
        }
      }

      FileWriter writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line);
        writer.write("\n");
      }
      writer.close();
    } catch (IOException ex) {
      System.err.println(ex.getMessage());
      exitCode = 1;
    }
    return exitCode;
  }

  int addHostToFile(String fileLocation, String hostName) {
    int exitCode = 0;
    File file = new File(fileLocation);
    try {
      boolean hostNameExists = false;
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String fileLine;
      List<String> lines = new LinkedList<String>();
      while ((fileLine = reader.readLine()) != null) {
        if (fileLine.trim().startsWith(COMMENT) &&
                fileLine.trim().contains(hostName)) {
          // This hostName was previously commented out by this tool
          lines.add(fileLine.substring(COMMENT.length()).trim());
          hostNameExists = true;
        } else if (fileLine.contains(hostName)) {
          hostNameExists = true;
          lines.add(fileLine);
        } else {
          lines.add(fileLine);
        }
      }

      if (!hostNameExists) {
        // Add hostname as the new line, but mark it with the comment
        lines.add(hostName + " " + COMMENT);
      }

      FileWriter writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line);
        writer.write("\n");
      }
      writer.close();
    } catch (IOException ex) {
      System.err.println(ex.getMessage());
      exitCode = 1;
    }
    return exitCode;
  }

  void refreshTracker() throws IOException {
    getAdminOperationsProtocol().refreshNodes();
  }

  private int restore() throws IOException {
    int exitCode = 0;

    String slaves = getFileLocation("slaves");
    exitCode = restoreFile(slaves);

    String includes = getFileLocation(getConf().get("mapred.hosts"));
    exitCode = restoreFile(includes);

    String excludes = getFileLocation(getConf().get("mapred.hosts.exclude"));
    exitCode = restoreFile(excludes);

    refreshTracker();

    return exitCode;
  }

  private int removeHost(String hostName) throws IOException {
    int exitCode = 0;

    String slaves = getFileLocation("slaves");
    exitCode = removeHostFromFile(slaves, hostName);

    String includes = getFileLocation(getConf().get("mapred.hosts"));
    exitCode = removeHostFromFile(includes, hostName);

    String excludes = getFileLocation(getConf().get("mapred.hosts.exclude"));
    exitCode = addHostToFile(excludes, hostName);

    refreshTracker();

    return exitCode;
  }

  private int addHost(String hostName) throws IOException {
    int exitCode = 0;

    String slaves = getFileLocation("slaves");
    exitCode = addHostToFile(slaves, hostName);

    String includes = getFileLocation(getConf().get("mapred.hosts"));
    exitCode = addHostToFile(includes, hostName);

    String excludes = getFileLocation(getConf().get("mapred.hosts.exclude"));
    exitCode = removeHostFromFile(excludes, hostName);

    refreshTracker();

    return exitCode;
  }

  private static UnixUserGroupInformation getUGI(Configuration conf)
          throws IOException {
    UnixUserGroupInformation ugi = null;
    try {
      ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException) (new IOException(
              "Failed to get the current user's information.").initCause(e));
    }
    return ugi;
  }

  private AdminOperationsProtocol getAdminOperationsProtocol()
          throws IOException {
    // Create the client
    if (adminOperationsProtocol == null) {
      adminOperationsProtocol =
              (AdminOperationsProtocol) RPC.getProxy(
              AdminOperationsProtocol.class,
              AdminOperationsProtocol.versionID,
              JobTracker.getAddress(getConf()), getUGI(getConf()), getConf(),
              NetUtils.getSocketFactory(getConf(),
              AdminOperationsProtocol.class));
    }

    return adminOperationsProtocol;
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new TTMover(), args);
    System.exit(exitCode);
  }
}
