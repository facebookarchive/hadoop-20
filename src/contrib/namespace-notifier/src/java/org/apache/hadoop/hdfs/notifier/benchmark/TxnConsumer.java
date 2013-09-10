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

package org.apache.hadoop.hdfs.notifier.benchmark;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.notifier.EventType;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.NotConnectedToServerException;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.WatchAlreadyPlacedException;
import org.apache.hadoop.hdfs.notifier.NotifierUtils;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;
import org.apache.hadoop.hdfs.notifier.Watcher;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TxnConsumer {
  
  final static Log LOG = LogFactory.getLog(TxnConsumer.class);
  
  public static String TEST_FILE_LABEL = "test.file.label";
  public static String TEST_DIR_LABEL = "test.dir.label";
  public static String SERVER_ADDR_STR_LABEL = "server.addr.str.label";
  public static String SERVER_PORT_STR_LABEL = "server.port.str.label";
  
  private static final String NUM_MAPPERS_KEY = "-numtasks";
  private static final String NUM_SUBDIRS_KEY = "-numsubdirs";
  private static final String NUM_FILES_SUB_DIR_KEY = "-numfiles";
  private static final String NUM_ROUND_KEY = "-round";
  private static final String WORKPLACE_KEY = "-workplace";
  private static final String NOTIFIER_SERVER_ADDR_KEY = "-notifieraddr";
  private static final String NOTIFIER_SERVER_PORT_KEY = "-notifierport";
  
  private static final int SERVER_DEFAULT_PORT = 31000;
  private static final int CLIENT_PROT = 32000;
  
  private static int numMappers = 10;
  private static int numSubDirs = 100;
  private static int numFilesEachSubDirs = 100;
  private static int round = 100;
  private static String workplace = "";
  
  // the namespace notifier server address string
  // sample: address1,address2
  private static String notifierServerAddrStr = "";
  // the namespace notifier server port string
  // sample: port1, port2
  private static String notifierServerPortStr = "";
  
  private Configuration conf;
  
  public TxnConsumer(Configuration conf) {
    this.conf = conf;
  }
  
  static class ConsumerMapper implements Mapper<LongWritable, Text, Text, Text> {

    private JobConf jobConf;
    private String workplace;
    private NamespaceNotifierClient myClient;
    
    private long expectedTotal;
    private long expectedFileAdd;
    private long expectedFileClose;
    private long expectedDirAdd;
    private long expectedNodeDelete;
    
    private static List<String> notifierServerAddrList;
    private static List<Integer> notifierServerPortList;

    @Override
    public void configure(JobConf job) {
      this.jobConf = job;
      this.workplace = jobConf.get(TEST_DIR_LABEL);
      
      expectedFileAdd = round * numSubDirs * numFilesEachSubDirs;
      expectedFileClose = expectedFileAdd;
      expectedDirAdd = round * numSubDirs;
      expectedNodeDelete = expectedFileAdd + expectedDirAdd;
      expectedTotal = expectedFileAdd + expectedFileClose + expectedDirAdd + expectedNodeDelete; 
      
      String serverAddrStr = jobConf.get(NOTIFIER_SERVER_ADDR_KEY);
      String serverPortStr = jobConf.get(NOTIFIER_SERVER_PORT_KEY);
      
      LOG.info("serverAddr: " + serverAddrStr + ", serverPort: " + serverPortStr);
      
      notifierServerAddrList = Arrays.asList(serverAddrStr.split(","));
      int index = 0;
      notifierServerPortList = new ArrayList<Integer>(notifierServerAddrList.size());
      if (!notifierServerPortStr.trim().isEmpty()) {
        List tmpList = Arrays.asList(serverPortStr.split(","));
        while (index < notifierServerAddrList.size() && index < tmpList.size()) {
          notifierServerPortList.add(Integer.valueOf((String) tmpList.get(index++)));
        }
      }
      
      while (index++ < notifierServerAddrList.size()) {
        notifierServerPortList.add(SERVER_DEFAULT_PORT);
      }
    }

    @Override
    public void close() throws IOException {
      
    }

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      workplace += value.toString();
      
      SimpleWatcher myWatcher = new SimpleWatcher();
      
      try {
        myClient = new NamespaceNotifierClient(myWatcher, 
            notifierServerAddrList, notifierServerPortList, CLIENT_PROT);
      
        Thread clientThread = new Thread(myClient);
        clientThread.start();
        
        LOG.info("Expected number of notifications:\nTotal Notifications: " + expectedTotal+ 
          "\nAdd_file Notifications: " + expectedFileAdd + 
          "\nClose_file Notifications: " + expectedFileClose + 
          "\nAdd_dir Notifications: " + expectedDirAdd + 
          "\nNode_delete Notifications: " + expectedNodeDelete);
        
        while (!getAllExpectedNotis(myWatcher)) {
          countReceivedNotis(myWatcher);
          Thread.sleep(10000);
          reporter.progress();
        }
        
        output.collect(value, new Text(myWatcher.toString()));
        
      } catch (Exception e) {
        LOG.error("Got error: " + e.getMessage(), e);
        throw new IOException (e);
      }
    }
    
    private boolean getAllExpectedNotis(SimpleWatcher watcher) {
      
      return (watcher.totalNoti.get() == expectedTotal) 
          && (watcher.totalAddFile.get() == expectedFileAdd) 
          && (watcher.totalCloseFile.get() == expectedFileClose) 
          && (watcher.totalAddDir.get() == expectedDirAdd)
          && (watcher.totalDeleteNode.get() == expectedNodeDelete);
    }
    
    private void countReceivedNotis(SimpleWatcher watcher) {
      LOG.info("Received: \nTotal Notifications: " + watcher.totalNoti.get() + 
          "\nAdd_file Notifications: " + watcher.totalAddFile.get() + 
          "\nClose_file Notifications: " + watcher.totalCloseFile.get() + 
          "\nAdd_dir Notifications: " + watcher.totalAddDir.get() + 
          "\nNode_delete Notifications: " + watcher.totalDeleteNode.get());
    }
    
    private void placeWatch() throws TransactionIdTooOldException, 
          NotConnectedToServerException, InterruptedException, 
          WatchAlreadyPlacedException {
      String path = new Path(workplace).toUri().getPath();
      myClient.placeWatch(path, EventType.FILE_ADDED, -1);
      myClient.placeWatch(path, EventType.FILE_CLOSED, -1);
      myClient.placeWatch(path, EventType.DIR_ADDED, -1);
      myClient.placeWatch(path, EventType.NODE_DELETED, -1);
    }
    
    public class SimpleWatcher implements Watcher {
      public volatile boolean connected = false;
      public AtomicLong totalNoti = new AtomicLong();
      public AtomicLong totalAddFile = new AtomicLong();
      public AtomicLong totalCloseFile = new AtomicLong();
      public AtomicLong totalAddDir = new AtomicLong();
      public AtomicLong totalDeleteNode = new AtomicLong();
      
      @Override
      public void handleNamespaceNotification(NamespaceNotification notification) {
        LOG.info("Received notification: " + NotifierUtils.asString(notification));
        totalNoti.incrementAndGet();
        
        if (notification.type == EventType.FILE_ADDED.getByteValue()) {
          totalAddFile.incrementAndGet();
        } else if (notification.type == EventType.FILE_CLOSED.getByteValue()) {
          totalCloseFile.incrementAndGet();
        } else if (notification.type == EventType.NODE_DELETED.getByteValue()) {
          totalDeleteNode.incrementAndGet();
        } else if (notification.type == EventType.DIR_ADDED.getByteValue()) {
          totalAddDir.incrementAndGet();
        }
      }

      @Override
      public void connectionFailed() {
        LOG.warn("Connection failed.");
        connected = false;
      }

      @Override
      public void connectionSuccesful() {
        LOG.info("Connection successful.");
        connected = true;
        
        try {
          placeWatch();
          LOG.info("Watch placed.");
        } catch (TransactionIdTooOldException e) {
          LOG.warn(e.getMessage(), e);
        } catch (NotConnectedToServerException e) {
          LOG.warn(e.getMessage(), e);
        } catch (InterruptedException e) {
          LOG.warn(e.getMessage(), e);
        } catch (WatchAlreadyPlacedException e) {
          LOG.warn(e.getMessage(), e);
        }
      }
      
      public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("total Notification: ").append(totalNoti.get());
        sb.append("total File Add: ").append(totalAddFile.get());
        sb.append("total File Close: ").append(totalCloseFile.get());
        sb.append("total Add Dir: ").append(totalAddDir.get());
        sb.append("total Delete Node: ").append(totalDeleteNode.get());
        
        return sb.toString();
      }
    }
  }
  
  static class ConsumerReducer implements Reducer<Text, Text, Text, Text> {
    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        output.collect(key, values.next());
      }
    }
  }
  
  
  private static void createSplitFiles(Configuration conf, Path splitDir) 
      throws IOException {
    FileSystem fs = splitDir.getFileSystem(conf);
    for (int i = 0; i < numMappers; i++) {
      String mapperDir = "mapper" + i;
      Path path = new Path(splitDir, mapperDir);
      OutputStream os = fs.create(path, true);
      os.write(mapperDir.getBytes());
      os.close();
    }
  }
  
  private JobConf createJobConf(Configuration conf2) throws IOException {
    JobConf jobConf = new JobConf(conf);
    String jobName = "transaction_consumer";
    jobConf.setJobName(jobName);
    
    String splitDir = workplace + "split/";
    
    jobConf.set(TEST_DIR_LABEL, workplace);
    jobConf.set(NOTIFIER_SERVER_ADDR_KEY, notifierServerAddrStr);
    jobConf.set(NOTIFIER_SERVER_PORT_KEY, notifierServerPortStr);
    
    jobConf.setMapSpeculativeExecution(false);
    jobConf.setReduceSpeculativeExecution(false);
    
    jobConf.setJarByClass(TxnConsumer.class);
    jobConf.setMapperClass(ConsumerMapper.class);
    jobConf.setReducerClass(ConsumerReducer.class);
    
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);
    
    FileInputFormat.addInputPath(jobConf, new Path(splitDir));
    Random random = new Random();
    FileOutputFormat.setOutputPath(jobConf, new Path(workplace, "output" + random.nextLong()));
    
    jobConf.setNumMapTasks(numMappers);
    
    createSplitFiles(conf, new Path(splitDir));
    
    return jobConf;
  }
  
  private void printUsage() {
    System.out.println("NotifierShell -consumetxn -workplace workplace " +
              NOTIFIER_SERVER_ADDR_KEY + " notifierAddrList " +
              "[" + NOTIFIER_SERVER_PORT_KEY + " notifierPortList] " + 
              "[" + NUM_MAPPERS_KEY + " numMappers] " +
              "[" + NUM_SUBDIRS_KEY + " numSubDirs] " + 
              "[" + NUM_FILES_SUB_DIR_KEY + " numFiles] " + 
              "[" + NUM_ROUND_KEY + " numRound]");
  }
  
  public void start(String[] args, int startIndex) throws IOException {
    try {
      while(startIndex < args.length) {
        String cmd = args[startIndex ++];
        if (cmd.equals(NUM_MAPPERS_KEY)) {
          numMappers = Integer.valueOf(args[startIndex ++]);
        } else if (cmd.equals(NUM_SUBDIRS_KEY)) {
          numSubDirs = Integer.valueOf(args[startIndex ++]);
        } else if (cmd.equals(NUM_FILES_SUB_DIR_KEY)) {
          numFilesEachSubDirs = Integer.valueOf(args[startIndex ++]);
        } else if (cmd.equals(NUM_ROUND_KEY)) {
          round = Integer.valueOf(args[startIndex ++]);
        } else if (cmd.equals(WORKPLACE_KEY)) {
          workplace = args[startIndex ++];
        } else if (cmd.equals(NOTIFIER_SERVER_ADDR_KEY)) {
          notifierServerAddrStr = args[startIndex ++];
        } else if (cmd.equals(NOTIFIER_SERVER_PORT_KEY)) {
          notifierServerPortStr = args[startIndex ++];
        } else {
          printUsage();
          System.exit(-1);
        }
      }
    } catch (Exception e) {
      printUsage();
      System.exit(-1);
    }
    
    if (workplace.trim().isEmpty() ||
        notifierServerAddrStr.trim().isEmpty()) {
      printUsage();
      System.exit(-1);
    }
    
    if (!workplace.endsWith(Path.SEPARATOR)) {
      workplace += Path.SEPARATOR;
    }
    
    JobConf jobConf = createJobConf(conf);
    
    JobClient client = new JobClient(jobConf);
    RunningJob runningJob = client.submitJob(jobConf);
    runningJob.waitForCompletion();
  }
}
