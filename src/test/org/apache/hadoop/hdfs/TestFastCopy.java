package org.apache.hadoop.hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.FastCopy;
import org.apache.hadoop.hdfs.tools.FastCopy.FastFileCopyRequest;
import org.junit.Test;
import junit.framework.TestCase;

public class TestFastCopy extends TestCase {

  private Random random = new Random();
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.conf = new Configuration();
    // Require the complete file to be replicated before we return in unit
    // tests.
    this.conf.setInt("dfs.replication.min", 3);
    this.cluster = new MiniDFSCluster(conf, 3, true, null);
    this.fs = (DistributedFileSystem) cluster.getFileSystem();
  }

  @Override
  public void tearDown() throws Exception {
    this.fs.close();
    this.cluster.shutdown();
    super.tearDown();
  }

  private final byte[] buffer = new byte[4096]; // 4KB
  private static Log LOG = LogFactory.getLog(TestFastCopy.class);

  /*
   * Generates a random file and returns the path.
   *
   * @param fs the FileSystem on which to generate the file
   *
   * @param filename the full path name of the file
   */
  public void generateRandomFile(FileSystem fs, String filename, int filesize)
      throws IOException {
    Path filePath = new Path(filename);
    OutputStream out = fs.create(filePath, true, 4096);
    int bytesWritten = 0;
    while (bytesWritten < filesize) {
      random.nextBytes(buffer);
      out.write(buffer);
      bytesWritten += buffer.length;
    }
    out.close();
  }

  @Test
  public void testFastCopy() throws Exception {
    // Create a source file.
    String src = "/testFastCopySrc";
    generateRandomFile(fs, src, 1024 * 1024 * 100); // Create a file of size
                                                    // 100MB.
    String destination = "/testFastCopyDestination";
    FastCopy fastCopy = new FastCopy(conf);
    int ncopies = 10;
    try {
      for (int i = 0; i < ncopies; i++) {
        fastCopy.copy(src, destination + i);
        assertTrue(verifyCopiedFile(src, destination + i));
      }
    } catch (Exception e) {
      LOG.error("Fast Copy failed with exception : ", e);
      fail("Fast Copy failed");
    }
  }

  @Test
  public void testFastCopyMultiple() throws Exception {
    // Create a source file.
    String src = "/testFastCopyMultipleSrc";
    generateRandomFile(fs, src, 1024 * 1024 * 100); // Create a file of size
    // 100MB.
    String destination = "/testFastCopyMultipleDestination";
    FastCopy fastCopy = new FastCopy(conf);
    int files = 10;
    List<FastFileCopyRequest> requests = new ArrayList<FastFileCopyRequest>();
    for (int i = 0; i < files; i++) {
      requests.add(new FastFileCopyRequest(src, destination + i));
    }
    try {
      fastCopy.copy(requests);
      for (FastFileCopyRequest r : requests) {
        assertTrue(verifyCopiedFile(r.getSrc(), r.getDestination()));
      }
    } catch (Exception e) {
      fail("testFastCopyMultiple failed with exception " + e);
    }
  }

  public boolean verifyBlockLocations(String src, String destination)
      throws IOException {
    NameNode namenode = cluster.getNameNode();
    List<LocatedBlock> srcblocks = namenode.getBlockLocations(src, 0,
        Long.MAX_VALUE).getLocatedBlocks();
    List<LocatedBlock> dstblocks = namenode.getBlockLocations(destination, 0,
        Long.MAX_VALUE).getLocatedBlocks();

    assertEquals(srcblocks.size(), dstblocks.size());

    Iterator<LocatedBlock> srcIt = srcblocks.iterator();
    Iterator<LocatedBlock> dstIt = dstblocks.iterator();
    while (srcIt.hasNext()) {
      LocatedBlock srcBlock = srcIt.next();
      LocatedBlock dstBlock = dstIt.next();
      List<DatanodeInfo> srcLocations = Arrays.asList(srcBlock
          .getLocations());
      List<DatanodeInfo> dstLocations = Arrays.asList(dstBlock
          .getLocations());

      System.out.println("Locations for src block : " + srcBlock.getBlock() +
          " file : " + src);
      for (DatanodeInfo info : srcLocations) {
        System.out.println("Datanode : " + info.toString());
      }

      System.out.println("Locations for dst block : " + dstBlock.getBlock() +
          " file : " + destination);
      for (DatanodeInfo info : dstLocations) {
        System.out.println("Datanode : " + info.toString());
      }

      assertEquals(srcLocations.size(), dstLocations.size());

      // Verifying block locations for both blocks are same.
      assertTrue(srcLocations.containsAll(dstLocations));
      assertTrue(dstLocations.containsAll(srcLocations));
    }
    return true;
  }

  public boolean verifyCopiedFile(String src, String destination)
      throws Exception {

    verifyBlockLocations(src, destination);

    Path srcFilePath = new Path(src);
    Path destFilePath = new Path(destination);
    FSDataInputStream srcStream = fs.open(srcFilePath, 4096);
    FSDataInputStream destStream = fs.open(destFilePath, 4096);
    int counter = 0;
    byte[] buffer1 = new byte[4096]; // 4KB
    byte[] buffer2 = new byte[4096]; // 4KB
    while (true) {
      try {
        srcStream.readFully(buffer1);
      } catch (EOFException e) {
        System.out.print("Src file EOF reached");
        counter++;
      }
      try {
        destStream.readFully(buffer2);
      } catch (EOFException e) {
        System.out.print("Destination file EOF reached");
        counter++;
      }
      if (counter == 1) {
        System.out.println("One file larger than other");
        return false;
      } else if (counter == 2) {
        return true;
      }
      if (!Arrays.equals(buffer1, buffer2)) {
        System.out.println("Files Mismatch");
        return false;
      }
    }
  }
}
