package org.apache.hadoop.hdfs;

import junit.framework.TestCase;


import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.NameSpaceSliceStorage;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageAdapter;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import static org.apache.hadoop.hdfs.AppendTestUtil.loseLeases;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

/* Test name-node restarting when client is writing a file
 */
public class TestNameNodeRestart extends TestCase {
  {
    DataNode.LOG.getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)NameNode.LOG).getLogger().setLevel(Level.ALL);
  }

  static final int blockSize = 1024;
  static final int numBlocks = 10;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;

  private long seed;
  private byte[] fileContents = null;

  //
  // create a buffer that contains the entire test file data.
  //
  private void initBuffer(int size) {
    seed = AppendTestUtil.nextLong();
    fileContents = AppendTestUtil.randomBytes(seed, size);
  }

  /*
   * creates a file but does not close it
   */ 
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    return createFile(fileSys, name, repl, (long)blockSize);
  }

  /*
   * creates a file but does not close it
   */
  private FSDataOutputStream createFile(FileSystem fileSys, Path name,
      int repl, long myBlockSize) throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt("io.file.buffer.size", 4096), (short) repl, myBlockSize);
    return stm;
  }
 

  private void checkFullFile(FileSystem fs, Path name) throws IOException {
    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[fileContents.length];
    stm.readFully(0, actual);
    checkData(actual, 0, fileContents, "Read 2");
    stm.close();
  }

  private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }
  
  /**
   * Test restarting Name node when writing to a  HDFS file.
   */
  public void testNnRestart1() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("dfs.write.packet.size", 511);
    conf.setBoolean("dfs.support.append", false);

    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/nnRestart1.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file simpleFlush.dat");

      // write to file
      stm.write(fileContents, 0, blockSize/2);
      System.out.println("Wrote and Flushed first part of file.");
      Thread.sleep(1000);

      // restart NN
      cluster.restartNameNode(0);

      // write second of the file
      boolean hasFailed = false;
      try {
        stm.write(fileContents, blockSize/2, blockSize);
        stm.close();
      } catch(IOException ioe) {
        hasFailed = true;
      }
      TestCase.assertTrue("Write didn't fail", hasFailed);
    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test restarting Name node when writing to a  HDFS file.
   */
  public void testNnRestart2() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("dfs.write.packet.size", 511);
    conf.setBoolean("dfs.support.append", false);
    
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/nnRestart2.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file simpleFlush.dat");

      // write to file
      stm.write(fileContents, 0, blockSize * 3/2);
      System.out.println("Wrote and Flushed first part of file.");
      Thread.sleep(1000);
      
      // restart NN
      cluster.restartNameNode(0);
      
      // write the remainder of the file
      // write second of the file
      boolean hasFailed = false;
      try {
        stm.write(fileContents, blockSize * 3 / 2, fileSize - blockSize * 3
            / 2);
        stm.close();
      } catch(IOException ioe) {
        hasFailed = true;
      }
      TestCase.assertTrue(hasFailed);
    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }  
  
  /**
   * Test restarting Name node when writing to a  HDFS file.
   */
  public void testNnRestart3() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("dfs.write.packet.size", 511);
    conf.setBoolean("dfs.support.append", true);

    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/nnRestart3.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file simpleFlush.dat");

      // write to file
      stm.write(fileContents, 0, blockSize/2);
      stm.sync();
      System.out.println("Wrote and Flushed first part of file.");
      
      // restart NN
      cluster.restartNameNode(0);

      // write second of the file
      stm.write(fileContents, blockSize/2, blockSize);
      stm.sync();
      System.out.println("Written second part of file");
      System.out.println("Wrote and Flushed second part of file.");

      // restart NN
      cluster.restartNameNode(0);
      
      // write the remainder of the file
      stm.write(fileContents, blockSize / 2 + blockSize, fileSize
          - (blockSize / 2 + blockSize));
      System.out.println("Written remainder of file");
      stm.sync();

      stm.close();
      System.out.println("Closed file.");

      // verify that entire file is good
      checkFullFile(fs, file1);

    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test restarting Name node when writing to a  HDFS file.
   */
  public void testNnRestart4() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("dfs.write.packet.size", 511);
    conf.setBoolean("dfs.support.append", false);
    
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/nnRestart4.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file simpleFlush.dat");

      // write to file
      stm.write(fileContents, 0, blockSize * 3/2);
      System.out.println("Wrote and Flushed first part of file.");
      Thread.sleep(1000);
      
      // restart NN
      cluster.restartNameNode(0);
      
      // write the remainder of the file
      // write second of the file
      boolean hasFailed = false;
      try {
        stm.close();
      } catch(IOException ioe) {
        hasFailed = true;
      }
      TestCase.assertTrue(hasFailed);
    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
  
  /**
   * Test restarting Name node when writing to a  HDFS file.
   */
  public void testNnRestart5() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("dfs.write.packet.size", 511);
    conf.setBoolean("dfs.support.append", false);
    
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/nnRestart5.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file simpleFlush.dat");

      // write to file
      stm.write(fileContents, 0, blockSize /2);
      System.out.println("Wrote and Flushed first part of file.");
      Thread.sleep(1000);
      
      // restart NN
      cluster.restartNameNode(0);
      
      // write the remainder of the file
      // write second of the file
      boolean hasFailed = false;
      try {
        stm.write(fileContents, blockSize / 2, blockSize / 4);
        stm.close();
      } catch(IOException ioe) {
        hasFailed = true;
      }
      TestCase.assertTrue(hasFailed);
    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
