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
import static org.junit.Assert.assertEquals;

import java.lang.management.ManagementFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Cookie;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.hdfs.util.InjectionEvent;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * Class for testing {@link NameNode Load Http} implementation
 */
public class TestNameNodeLoadHttp extends TestCase{
  private URL baseUrl;
  private Object syncRoot = new Object();
  private boolean removeServletNotified = false;

  /**
   * test the conditional load of http endpoint based on initial configuration.
   * positive case
   * the configuration settings are in hdfs-default.xml
   */
  @Test
  public void testLoadExtraHttpByInitialConfigPositive() throws Exception  {
    MiniDFSCluster cluster = setupHttpServer(true);
    verifyLoadServletGeneric("listPaths", "listPaths", true);
    verifyLoadServletGeneric("data", "data", true);
    verifyLoadServletGeneric("checksum", "fileChecksum", true);
    cluster.shutdown();
  }

  /**
   * test the conditional load of http endpoint based on initial configuration.
   * negative case
   * the configuration settings are in hdfs-default.xml
   */
  @Test
  public void testLoadExtraHttpByInitialConfigNegative() throws Exception {
    MiniDFSCluster cluster = setupHttpServer(false);
    verifyLoadServletGeneric("listPaths", "listPaths", false);
    verifyLoadServletGeneric("checksum", "fileChecksum", false);
    verifyLoadServletGeneric("data", "data", false);
    cluster.shutdown();
  }

  /**
   * test dynamically removing servlets through reconfigureProperty call
   */
  @Test
  public void testDynamicLoadRemoveServlet() throws Exception {
    MiniDFSCluster cluster = setupHttpServer(true);

    long start = System.currentTimeMillis();
    cluster.getNameNode().reconfigureProperty("dfs.enableHftp", "false");
    long end = System.currentTimeMillis();
    System.out.println("it takes "+(end-start)+" ms to remove 3 servlets");

    verifyLoadServletGeneric("listPaths", "listPaths", false);
    verifyLoadServletGeneric("data", "data", false);
    verifyLoadServletGeneric("checksum", "fileChecksum", false);
    cluster.shutdown();
  }

  /**
   * test dynamically removing servlets through reconfigureProperty call
   */
  @Test
  public void testDynamicLoadAddServlet() throws Exception {
    MiniDFSCluster cluster = setupHttpServer(false);

    long start = System.currentTimeMillis();
    cluster.getNameNode().reconfigureProperty("dfs.enableHftp", "true");
    long end = System.currentTimeMillis();
    System.out.println("it takes "+(end-start)+" ms to add 3 servlets");

    verifyLoadServletGeneric("listPaths", "listPaths", true);
    verifyLoadServletGeneric("data", "data", true);
    verifyLoadServletGeneric("checksum", "fileChecksum", true);
    cluster.shutdown();
  }

  /**
   * test dynamically removing servlets through reconfigureProperty call
   */
  @Test
  public void testRemoveServletWhilePageHit() throws Exception {
    MiniDFSCluster cluster = setupHttpServer(true);
    TestNameNodeReconfigHftpInjectionHandler h = new TestNameNodeReconfigHftpInjectionHandler();
    InjectionHandler.set(h);
    AccessServletPageThread servletPageAccesThread = new AccessServletPageThread();
    servletPageAccesThread.start();
  
    cluster.getNameNode().reconfigureProperty("dfs.enableHftp", "false");

    cluster.shutdown();
  }
  

  private MiniDFSCluster setupHttpServer(Boolean testPositive) 
          throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.enableHftp", testPositive);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    NameNode nameNode = cluster.getNameNode();
    HttpServer httpServer = nameNode.getHttpServer();
    assertNotNull("httpServer should not be null", httpServer);
    
    int port = httpServer.getPort();
    baseUrl = new URL("http://localhost:" + port + "/");
    return cluster;
  }

  private void verifyLoadServletGeneric(String configName, 
      String pathName, Boolean testPositive) throws Exception {
    //get http response from the end point
    URL url = new URL(baseUrl, "/" + pathName);
    URLConnection connection = url.openConnection();
    Boolean exceptionCaught = false;

    try {
      String response = readConnection(connection);
    } catch (java.io.FileNotFoundException e) {
      //this is the exception we need to check: if the servlet is not loaded,
      //this exception will be thrown by the network client
      exceptionCaught = true;
    } catch (java.net.ConnectException e) {
      //we don't care if the connection is refused, this means the servlet is 
      //loaded, but we are serving with bad request url, which is fine
      exceptionCaught = false;
    } catch (java.io.IOException e) {
      //we don't care if the servlet returns 400 (bad request), as long as 
      //it is loaded
      if(e.getMessage().contains("Server returned HTTP response code: 400")) {
        exceptionCaught = false;  
      } else {
        exceptionCaught = true;
      }
    }

    //test if the io exception (expected in negative case only) is caught
    if(testPositive) {
      assertFalse("servlet " + pathName + " should have been loaded",
                  exceptionCaught);
    } else {
      assertTrue("servlet" + pathName + " should NOT have been loaded",
                  exceptionCaught);
    }
  }

  private String readConnection(URLConnection connection) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = connection.getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }

  class AccessServletPageThread extends Thread {
    public void run() {
      //wait for the InjectionHandler's notify, then try to access the servlet
      try {
        synchronized(syncRoot){
          while(!removeServletNotified)   
            syncRoot.wait();
          
          verifyLoadServletGeneric("listPaths", "listPaths", true);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  class TestNameNodeReconfigHftpInjectionHandler extends InjectionHandler {
    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if(event == InjectionEvent.NAMENODE_RECONFIG_HFTP && (Boolean)args[0] == false) {
        synchronized(syncRoot) {
          removeServletNotified=true;
          syncRoot.notify();
        }
      }
    }
  }
}


