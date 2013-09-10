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
package org.apache.hadoop.hdfs.fsshellservice;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class FsShellServiceClient {
  FsShellService.Client client;
  TTransport tr;

  FsShellServiceClient() throws TTransportException {
    tr = new TFramedTransport(new TSocket("localhost", 62001));
    TProtocol proto = new TBinaryProtocol(tr);
    client = new FsShellService.Client(proto);
    tr.open();
  }

  void execute() throws FsShellException, FsShellFileNotFoundException,
      TException {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    try {
      String line = null;

      while (true) {
        System.err.print("> ");
        try {
          line = br.readLine();
        } catch (IOException ioe) {
          System.err.println("Error when accepting inputs");
          break;
        }
        if (line != null && line.toLowerCase().equals("exit")) {
          break;
        }
        String[] tokens = line.split(" ");
        String command = tokens[0].toLowerCase();
        if (command.equals("mkdirs")) {
          System.err.println("result: " + client.mkdirs(tokens[1]));
        } else if (command.equals("copyfromlocal")) {
          client.copyFromLocal(tokens[1], tokens[2], true);
          System.err.println("done");
        } else if (command.equals("copytolocal")) {
          client.copyToLocal(tokens[1], tokens[2], true);
          System.err.println("done");
        } else if (command.equals("remove")) {
          System.err.println("result: "
              + client.remove(tokens[1], Boolean.parseBoolean(tokens[2]),
                  Boolean.parseBoolean(tokens[3])));
        } else if (command.equals("rename")) {
          System.err.println("result: " + client.rename(tokens[1], tokens[2]));
        } else if (command.equals("ls")) {
          System.err.println("result: " + client.listStatus(tokens[1]));
        } else if (command.equals("status")) {
          System.err.println("result: " + client.getFileStatus(tokens[1]));
        } else if (command.equals("exists")) {
          System.err.println("result: " + client.exists(tokens[1]));
        } else if (command.equals("filecrc")) {
          System.err.println("result: " + client.getFileCrc(tokens[1]));
        } else {
          System.err.println("Invalid Command");
        }
      }

    } finally {
      tr.close();
    }
  }

  public static void main(String[] args) throws FsShellException,
      FsShellFileNotFoundException, TException {
    FsShellServiceClient client = new FsShellServiceClient();
    client.execute();
  }
}
