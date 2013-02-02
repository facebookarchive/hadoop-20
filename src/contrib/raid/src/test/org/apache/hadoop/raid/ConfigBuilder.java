package org.apache.hadoop.raid;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ConfigBuilder {
  private List<String> policies;
  private String configFile;

  public ConfigBuilder(String configFile) {
    policies = new java.util.ArrayList<String>();
    this.configFile = configFile;
  }

  public void addFileListPolicy(String name, String fileListPath, String parent) {
    String str =
      "<policy name = \"" + name + "\"> " +
        "<fileList>" + fileListPath + "</fileList>" +
        "<parentPolicy>" + parent + "</parentPolicy>" +
      "</policy>";
    policies.add(str);
  }

  public void addPolicy(String name, String path, String parent) {
    String str =
      "<policy name = \"" + name + "\"> " +
        "<srcPath prefix=\"" + path + "\"/> " +
        "<parentPolicy>" + parent + "</parentPolicy>" +
      "</policy>";
    policies.add(str);
  }

  public void addAbstractPolicy(String name, 
                        long targetReplication, long metaReplication, String codecId) {
    String str =
        "<policy name = \"" + name + "\"> " +
           "<srcPath/> " +
           "<codecId>" + codecId + "</codecId> " +
           "<property> " +
             "<name>targetReplication</name> " +
             "<value>" + targetReplication + "</value> " +
             "<description>" + 
               "after RAIDing, decrease the replication factor of a file to this value." +
             "</description> " +
           "</property> " +
           "<property> " +
             "<name>metaReplication</name> " +
             "<value>" + metaReplication + "</value> " +
             "<description> replication factor of parity file" +
             "</description> " +
           "</property> " +
           "<property> " +
             "<name>modTimePeriod</name> " +
             "<value>2000</value> " +
             "<description> time (milliseconds) after a file is modified to make it " +
                            "a candidate for RAIDing " +
             "</description> " +
           "</property> " +
        "</policy>";
    policies.add(str);
  }
  
  public void addPolicy(String name, String path,
                        long targetReplication, long metaReplication) {
    addPolicy(name, path, targetReplication, metaReplication, 
        "xor");
  }
  
  public void addFileListPolicy(String name, String fileListPath, 
                        long targetReplication, long metaReplication,
                        String code) {
    String str =
        "<policy name = \"" + name + "\"> " +
           "<fileList>" + fileListPath + "</fileList>" +
           "<codecId>" + code + "</codecId> " +
           "<property> " +
             "<name>targetReplication</name> " +
             "<value>" + targetReplication + "</value> " +
             "<description>after RAIDing, decrease the replication factor of a file to this value." +
             "</description> " + 
           "</property> " +
           "<property> " +
             "<name>metaReplication</name> " +
             "<value>" + metaReplication + "</value> " +
             "<description> replication factor of parity file" +
             "</description> " + 
           "</property> " +
           "<property> " +
             "<name>modTimePeriod</name> " +
             "<value>2000</value> " + 
             "<description> time (milliseconds) after a file is modified to make it " +
                            "a candidate for RAIDing " +
             "</description> " + 
           "</property> " +
        "</policy>";
    policies.add(str);
  }

  public void addPolicy(String name, String path, 
                        long targetReplication, long metaReplication,
                        String code) {
    String str =
        "<policy name = \"" + name + "\"> " +
          "<srcPath prefix=\"" + path + "\"/> " +
           "<codecId>" + code + "</codecId> " +
           "<property> " +
             "<name>targetReplication</name> " +
             "<value>" + targetReplication + "</value> " +
             "<description>after RAIDing, decrease the replication factor of a file to this value." +
             "</description> " + 
           "</property> " +
           "<property> " +
             "<name>metaReplication</name> " +
             "<value>" + metaReplication + "</value> " +
             "<description> replication factor of parity file" +
             "</description> " + 
           "</property> " +
           "<property> " +
             "<name>modTimePeriod</name> " +
             "<value>2000</value> " + 
             "<description> time (milliseconds) after a file is modified to make it " +
                            "a candidate for RAIDing " +
             "</description> " + 
           "</property> " +
        "</policy>";
    policies.add(str);
  }

  public void persist() throws IOException {
    FileWriter fileWriter = new FileWriter(configFile);
    fileWriter.write("<configuration>");
    for (String policy: policies) {
      fileWriter.write(policy);
    }
    fileWriter.write("</configuration>");
    fileWriter.close();
  }
}
