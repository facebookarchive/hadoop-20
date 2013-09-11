/**
 * $Id$
 *
 * Author: Thilee Subramaniam
 *
 * Copyright 2012 Quantcast Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * This Java client performs filesystem meta opetarions on the Hadoop namenode
 * using HDFS DFSClient.
 */

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;

public class MStress_Client
{
	// all of the files creates in HDFS are stored under TEST_BASE_DIR
  static final String TEST_BASE_DIR = new String("/mstress");

  static DFSClient dfsClient_ = null;
  static StringBuilder path_  = new StringBuilder(4096);
  static int pathLen_         = 0;
  static int totalCreateCount = 0;
  static final int COUNT_INCR = 500;

  //From commandline
  static String dfsServer_    = "";
  static int dfsPort_         = 0;
  static String testName_     = "";
  static String prefix_       = "PATH_PREFIX_";
  static int prefixLen_       = 0;
  static String planfilePath_ = "";
  static String hostName_     = "";
  static String processName_  = "";

  //From plan file
  static String type_         = "";
  static int levels_          = 0;
  static int inodesPerLevel_  = 0;
  static int pathsToStat_     = 0;
  
  /*
   * record keeping.  files_ is populuated by CreateDFSPaths(), and is later
   * used in createWriteDFSPaths() to write to the list of files created by
   * this instance of the benchmark
   */
  static private HashMap<String, OutputStream> files_
  	= new HashMap<String, OutputStream>();
  
  // latency metrics for DFSClient RPCs.
  private static ArrayList<Double> timingMkdirs_ = new ArrayList<Double>();
  private static ArrayList<Double> timingCreate_ = new ArrayList<Double>();
  private static ArrayList<Double> timingWrite_ = new ArrayList<Double>();
  private static ArrayList<Double> timingStat_ = new ArrayList<Double>();
  private static ArrayList<Double> timingListPaths_ = new ArrayList<Double>();
  private static ArrayList<Double> timingOpen_ = new ArrayList<Double>();
  private static ArrayList<Double> timingDelete_ = new ArrayList<Double>();
  private static ArrayList<Double> timingRename_ = new ArrayList<Double>();
  
  // this is the data written to the files created by the create-write benchmark
  static private String data_ = "SOME GARBAGE DATA";

  public static void main(String args[]) {
    parseOptions(args);
    int result = 0;

    try {
      Configuration conf = new Configuration(true);
      String confSet = "hdfs://" + dfsServer_ + ":" + dfsPort_;
      conf.set("fs.default.name", confSet);
      conf.set("fs.trash.interval", "0");
      InetSocketAddress inet = new InetSocketAddress(dfsServer_, dfsPort_);
      dfsClient_ = new DFSClient(inet, conf);

      if (parsePlanFile() < 0) {
        System.exit(-1);
      }

      /*
       * create:  creates directory and file inodes 
       * create_write: creates directory and file inodes, and writes data_ to them
       * stat: getFileInfo() on all the files
       * readdir:  listPaths() on all the directories
       * read: reads a byte from all the files
       * rename: renames all the files by appending '_x' to them
       * delete: delets all the files and directories (assumes they end with '_x') 
       */
      if (testName_.equals("create")) {
        result = createDFSPaths();
      } else if (testName_.equals("create_write")) {
      	result = createWriteDFSPaths();
      } else if (testName_.equals("stat")) {
        result = statDFSPaths();
      } else if (testName_.equals("readdir")) {
        result = listDFSPaths();
      } else if (testName_.equals("read")) {
      	result = readDFSPaths();
      } else if (testName_.equals("rename")) {
      	result = renameDFSPaths();
      } else if (testName_.equals("delete")) {
        result = removeDFSPaths();
      } else {
        System.out.printf("Error: unrecognized test \'%s\'\n", testName_);
        System.exit(-1);
      }

      if (result != 0) {
      	System.err.printf("test %s finished with a return of -1.\n", testName_);
      }
      
      Collections.sort(timingMkdirs_);
      Collections.sort(timingCreate_);
      Collections.sort(timingWrite_);
      Collections.sort(timingStat_);
      Collections.sort(timingListPaths_);
      Collections.sort(timingOpen_);
      Collections.sort(timingDelete_);
      Collections.sort(timingRename_);

      System.out.println("[benchmark] mkdirs: " + getTimings(timingMkdirs_));
      System.out.println("[benchmark] create: " + getTimings(timingCreate_));
      System.out.println("[benchmark] write: " + getTimings(timingWrite_));
      System.out.println("[benchmark] getFileInfo: " + getTimings(timingStat_));
      System.out.println("[benchmark] listPaths: " + getTimings(timingListPaths_));
      System.out.println("[benchmark] open: " + getTimings(timingOpen_));
      System.out.println("[benchmark] delete: " + getTimings(timingDelete_));
      System.out.println("[benchmark] rename: " + getTimings(timingRename_));
      
    } catch( IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    if (result != 0) {
      System.exit(-1);
    }

    return;
  }

  /**
   * Parses command line options.
   * the hostName, processName are used to namespace the files created by this
   * instance of the benchmark.
   */
  private static void parseOptions(String args[])
  {
    if (!(args.length == 14 || args.length == 12 || args.length == 5)) {
      usage();
    }

    /*
     * As described in usage():
     * -s dfs-server
     * -p dfs-port [-t [create|create-write|stat|readdir|read|rename|delete]
     * -a planfile-path
     * -c host
     * -n process-name
     * -P prefix
     */
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-s") && i+1 < args.length) {
        dfsServer_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-p") && i+1 < args.length) {
        dfsPort_ = Integer.parseInt(args[i+1]);
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-t") && i+1 < args.length) {
        testName_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-a") && i+1 < args.length) {
        planfilePath_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-c") && i+1 < args.length) {
        hostName_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-n") && i+1 < args.length) {
        processName_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-P") && i+1 < args.length) {
        prefix_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      }
    }

    if (dfsServer_.length() == 0    ||
        testName_.length() == 0     ||
        planfilePath_.length() == 0 ||
        hostName_.length() == 0     ||
        processName_.length() == 0  ||
        dfsPort_ == 0) {
      usage();
    }
    if (prefix_ == null) {
      prefix_ = new String("PATH_PREFIX_");
    }
    prefixLen_ = prefix_.length();
  }

  /**
   * Prints usage information to standard out.
   */
  private static void usage()
  {
    String className = MStress_Client.class.getName();
    System.out.printf("Usage: java %s -s dfs-server -p dfs-port" +
    		"[-t [create|stat|read|readdir|delete|rename] -a planfile-path -c host -n process-name" +
    		" -P prefix]\n",
                      className);
    System.out.printf("   -t: this option requires -a, -c, and -n options.\n");
    System.out.printf("   -P: default prefix is PATH_.\n");
    System.out.printf("eg:\n");
    System.out.printf("    java %s -s <metaserver-host> -p <metaserver-port> -t create" +
    		" -a <planfile> -c localhost -n Proc_00\n", className);
    System.exit(1);
  }

  /**
   * Parses the plan file that contains parameters for the benchmark.
   */
  private static int parsePlanFile()
  {
    int ret = -1;
    try {
      FileInputStream fis = new FileInputStream(planfilePath_);
      DataInputStream dis = new DataInputStream(fis);
      BufferedReader br = new BufferedReader(new InputStreamReader(dis));

      if (prefix_.isEmpty()) {
        prefix_ = "PATH_PREFIX_";
      }

      String line;
      while ((line = br.readLine()) != null) {
        if (line.length() == 0 || line.startsWith("#")) {
          continue;
        }
        if (line.startsWith("type=")) {
          type_ = line.substring(5);
          continue;
        }
        if (line.startsWith("levels=")) {
          levels_ = Integer.parseInt(line.substring(7));
          continue;
        }
        if (line.startsWith("inodes=")) {
          inodesPerLevel_ = Integer.parseInt(line.substring(7));
          continue;
        }
        if (line.startsWith("nstat=")) {
          pathsToStat_ = Integer.parseInt(line.substring(6));
          continue;
        }
      }
      dis.close();
      if (levels_ > 0 && !type_.isEmpty() && inodesPerLevel_ > 0 && pathsToStat_ > 0) {
        ret = 0;
      }
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
    }
    return ret;
  }

  /**
   * Measure the elapsed time between alpha and zigma.
   */
  private static long timeDiffMilliSec(Date alpha, Date zigma)
  {
    return zigma.getTime() - alpha.getTime();
  }

  /**
   * Recursively creates directories and files.
   * @param level current level of depth
   * @param parentPath the prefix to the path we have traversed into so far
   * @return -1 on error, 0 on success
   */
  private static int CreateDFSPaths(int level, String parentPath) {
    Boolean isLeaf = false;
    Boolean isDir = false;
    if (level + 1 >= levels_) {
      isLeaf = true;
    }
    if (isLeaf) {
      if (type_.equals("dir")) {
        isDir = true;
      } else {
        isDir = false;
      }
    } else {
      isDir = true;
    }

    for (int i = 0; i < inodesPerLevel_; i++) {
      String path = parentPath + "/" + prefix_ + Integer.toString(i);
      //System.out.printf("Creating (isdir=%b) [%s]\n", isDir, path.toString());

      if (isDir) {
        try {
        	
          long startTime = System.nanoTime();
          if (dfsClient_.mkdirs(path) == false) {
            System.out.printf("Error in mkdirs(%s)\n", path);
            return -1;
          }
          timingMkdirs_.add(new Double((System.nanoTime() - startTime)/(1E9)));
          
          System.out.printf("Creating dir %s\n", path);

          totalCreateCount ++;
          if (totalCreateCount % COUNT_INCR == 0) {
            System.out.printf("Created paths so far: %d\n", totalCreateCount);
          }
          if (!isLeaf) {
            if (CreateDFSPaths(level+1, path) < 0) {
              System.out.printf("Error in CreateDFSPaths(%s)\n", path);
              return -1;
            }
          }
        } catch(IOException e) {
          e.printStackTrace();
          return -1;
        }	
      } else {
        try {
        	System.out.printf("Creating file %s\n", path);
        	
        	long startTime = System.nanoTime();
        	OutputStream os = dfsClient_.create(path, true);
          timingCreate_.add(new Double((System.nanoTime() - startTime)/(1E9)));
          
        	files_.put(path, os);

          totalCreateCount ++;
          if (totalCreateCount % COUNT_INCR == 0) {
            System.out.printf("Created paths so far: %d\n", totalCreateCount);
          }
        } catch( IOException e) {
          e.printStackTrace();
          return -1;
        }
      }
    }
    
    return 0;
  }
  
  /**
   * Creates directories and files.  See CreateDFSPath(), the main recursive
   * portion of this method.
   * @return -1 on error, 0 on success
   */
  private static int createDFSPaths()
  {
    String basePath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;
    try {
      long startTime = System.nanoTime();
      Boolean ret = dfsClient_.mkdirs(basePath);
      timingMkdirs_.add(new Double((System.nanoTime() - startTime)/(1E9)));
      if (!ret) {
        System.out.printf("Error: failed to create test base dir [%s]\n", basePath);
        return -1;
      }
    } catch( IOException e) {
      e.printStackTrace();
      throw new RuntimeException();
    }

    Date alpha = new Date();

    if (CreateDFSPaths(0, basePath) < 0) {
      return -1;
    }

    Date zigma = new Date();
    System.out.printf("Client: %d paths created in %d msec\n",
    		totalCreateCount, timeDiffMilliSec(alpha, zigma));
    return 0;
  }
  
  /**
   * This creates DFS paths and writes data_ to them in one go.
   * @return -1 on error, 0 on success
   */
  private static int createWriteDFSPaths()
  {

  	if (createDFSPaths() != 0) {
  		return -1;
  	}
  	
	  try {
		  // write to all the files!
	  	for (Map.Entry<String, OutputStream> file : files_.entrySet()) {
	  		OutputStream os = file.getValue();

	  		long startTime = System.nanoTime();
	  		os.write(data_.getBytes());
        timingWrite_.add(new Double((System.nanoTime() - startTime)/(1E9)));
	  		os.close();
	  	}
	  	
	  } catch (IOException e) {
	  	e.printStackTrace();
	  	return -1;
	  }
  	  	
  	return 0;
  }
  
  /**
   * This calls getFileInfo() on all the files under this hostname + process' namespace.
   * @return -1 on error, 0 on success
   */
  private static int statDFSPaths()
  {
    String statPath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;

    System.out.printf("Stating %s ...\n", statPath);

    int countLeaf = (int) Math.round(Math.pow(inodesPerLevel_, levels_));
    int[] leafIdxRangeForDel = new int[countLeaf];
    for(int i=0;i<countLeaf;i++)
        leafIdxRangeForDel[i] = i;
    Collections.shuffle(Arrays.asList(leafIdxRangeForDel));

    Date alpha = new Date();
    try {
      for(int idx : leafIdxRangeForDel) {
          String path = "";
          for(int lev=0; lev < levels_; lev++) {
             int delta = idx % inodesPerLevel_;
             idx /= inodesPerLevel_;
             if(path.length() > 0) {
                 path = prefix_ + delta + "/" + path;
             } else {
                 path = prefix_ + delta;
             }
          }
  	  	  long startTime = System.nanoTime();
          dfsClient_.getFileInfo(statPath + "/" + path);
          timingStat_.add(new Double((System.nanoTime() - startTime)/(1E9)));
      }
    } catch(IOException e) {
      e.printStackTrace();
      return -1;
    }
    Date zigma = new Date();
    System.out.printf("Client: Stat'd all files in %s. Stat took %d msec\n",
    		statPath, timeDiffMilliSec(alpha, zigma));
    return 0;
  }

  /**
   * Renames all the files in this hostName/process' namespace by appending
   * '_x' to them.
   * @return -1 on error, 0 on success
   */
  private static int renameDFSPaths()
  {
    String renamePath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;

    System.out.printf("Renaming %s ...\n", renamePath);

    int countLeaf = (int) Math.round(Math.pow(inodesPerLevel_, levels_));
    int[] leafIdxRangeForDel = new int[countLeaf];
    for(int i=0;i<countLeaf;i++)
        leafIdxRangeForDel[i] = i;
    Collections.shuffle(Arrays.asList(leafIdxRangeForDel));

    Date alpha = new Date();
    try {
      for(int idx : leafIdxRangeForDel) {
          String path = "";
          for(int lev=0; lev < levels_; lev++) {
             int delta = idx % inodesPerLevel_;
             idx /= inodesPerLevel_;
             if(path.length() > 0) {
                 path = prefix_ + delta + "/" + path;
             } else {
                 path = prefix_ + delta;
             }
          }
  	  	  long startTime = System.nanoTime();
          dfsClient_.rename(renamePath + "/" + path, renamePath + "/" + path + "_x");
          timingRename_.add(new Double((System.nanoTime() - startTime)/(1E9)));
      }
    } catch(IOException e) {
      e.printStackTrace();
      return -1;
    }
    Date zigma = new Date();
    System.out.printf("Client: Renamed all files in %s. Rename took %d msec\n",
    		renamePath, timeDiffMilliSec(alpha, zigma));
    return 0;
  }
  
  /**
   * Lists all the directories contents in this hostname/process' namespace.
   * @return -1 on error, 0 on success
   */
  private static int listDFSPaths()
  {
    Date alpha = new Date();
    int inodeCount = 0;

    String basePath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;
    Queue<String> pending = new LinkedList<String>();
    pending.add(basePath);

    while (!pending.isEmpty()) {
      String parent = pending.remove();
      try {
      	
	  		long startTime = System.nanoTime();
        FileStatus[] children = dfsClient_.listPaths(parent);
        timingListPaths_.add(new Double((System.nanoTime() - startTime)/(1E9)));

        if (children == null || children.length == 0) {
          continue;
        }
        
        for (int i = 0; i < children.length; i++) {
        	String localName = children[i].getPath().getName();
        	if (localName.equals(".") || localName.equals("..")) {
        		continue;
        	}
        	inodeCount ++;
        	if (inodeCount % COUNT_INCR == 0) {
        		System.out.printf("Readdir paths so far: %d\n", inodeCount);
        	}
        	if (children[i].isDir()) {
        		pending.add(parent + "/" + localName);
        	} else {
        		files_.put(parent + "/" + localName, null);
        	}
        }
      } catch (IOException e) {
        e.printStackTrace();
        return -1;
      }
    }

    Date zigma = new Date();
    System.out.printf("Client: Directory walk done over %d inodes in %d msec\n",
    		inodeCount, timeDiffMilliSec(alpha, zigma));
    return 0;
  }
  
  /**
   * lists, and then reads the first byte in all of the files in
   * this hostname/process' namespace
   * @return -1 on error, 0 on success
   */
  private static int readDFSPaths() {
  	if (listDFSPaths() != 0) {
  		return -1;
  	}
  	
  	try{ 
	  	for (Map.Entry<String, OutputStream> file : files_.entrySet()) {
	  		
	  		long startTime = System.nanoTime();
	  		DFSInputStream os = dfsClient_.open(file.getKey());
        timingOpen_.add(new Double((System.nanoTime() - startTime)/(1E9)));
        
	  		os.read();
	  		os.close();
	  	}
  	} catch (IOException e) {
  		e.printStackTrace();
  	}
  	
  	return 0;
  }
  
  /**
   * deletes all the files in this hostname/process' namespace.
   * assumes that they've all been renamed (i.e., the rename benchmark already
   * ran)
   * @return -1 on error, 0 on sucess
   */
  private static int removeDFSPaths()
  {
    String rmPath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;

    System.out.printf("Deleting %s ...\n", rmPath);

    int countLeaf = (int) Math.round(Math.pow(inodesPerLevel_, levels_));
    int[] leafIdxRangeForDel = new int[countLeaf];
    for(int i=0;i<countLeaf;i++)
        leafIdxRangeForDel[i] = i;
    Collections.shuffle(Arrays.asList(leafIdxRangeForDel));

    Date alpha = new Date();
    try {
      for(int idx : leafIdxRangeForDel) {
          String path = "";
          for(int lev=0; lev < levels_; lev++) {
             int delta = idx % inodesPerLevel_;
             idx /= inodesPerLevel_;
             if(path.length() > 0) {
                 path = prefix_ + delta + "/" + path;
             } else {
                 path = prefix_ + delta;
             }
          }
  	  	  long startTime = System.nanoTime();
          dfsClient_.delete(rmPath + "/" + path + "_x", true);
          timingDelete_.add(new Double((System.nanoTime() - startTime)/(1E9)));
      }
      dfsClient_.delete(rmPath, true);
    } catch(IOException e) {
      e.printStackTrace();
      return -1;
    }
    Date zigma = new Date();
    System.out.printf("Client: Deleted %s. Delete took %d msec\n",
    		rmPath, timeDiffMilliSec(alpha, zigma));
    return 0;
  }
  
  private static String getTimings(ArrayList<Double> array) {
  	StringBuilder output = new StringBuilder();
  	
  	for (Double i : array) {
  		output.append(i.toString());
  		output.append(",");
  	}
  	if (output.length() > 0) {
  		output.deleteCharAt(output.length() - 1);
  	}
  	
  	return output.toString();
  }
  
}
