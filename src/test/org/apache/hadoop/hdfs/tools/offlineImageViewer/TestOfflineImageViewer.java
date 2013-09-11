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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;

/**
 * Test function of OfflineImageViewer by:
 *   * confirming it can correctly process a valid fsimage file and that
 *     the processing generates a correct representation of the namespace
 *   * confirming it correctly fails to process an fsimage file with a layout
 *     version it shouldn't be able to handle
 *   * confirm it correctly bails on malformed image files, in particular, a
 *     file that ends suddenly.
 */
public class TestOfflineImageViewer extends TestCase {
  private static final int NUM_DIRS = 10;
  private static final int FILES_PER_DIR = 5;

  private class FileStatusWithHardLink {
    public FileStatus stat;
    public long hardLinkId;

    public FileStatusWithHardLink(FileStatus stat, long hardlinkId) {
      this.stat = stat;
      this.hardLinkId = hardlinkId;
    }
  }

  // Elements of lines of ls-file output to be compared to FileStatus instance
  private class LsElements {
    public String perms;
    public int replication;
    public long hardlinkId;
    public String username;
    public String groupname;
    public long filesize;
    public char dir; // d if dir, - otherwise
    
    public boolean equals(Object obj) {
      if (!(obj instanceof LsElements))
        return false;
      LsElements o = (LsElements) obj;
      return perms.equals(o.perms) &&
          replication == o.replication &&
          hardlinkId == o.hardlinkId &&
          username.equals(o.username) &&
          groupname.equals(o.groupname) &&
          filesize == o.filesize &&
          dir == o.dir;
    }
  }
  
  // namespace as written to dfs, to be compared with viewer's output
  final HashMap<String, FileStatusWithHardLink> writtenFiles = new HashMap<String, FileStatusWithHardLink>();
  
  
  private static String ROOT = System.getProperty("test.build.data",
                                                  "build/test/data");
  
  // Main entry point into testing.  Necessary since we only want to generate
  // the fsimage file once and use it for multiple tests. 
  public void testOIV() throws Exception {
    File originalFsimage = null;
    try {
    originalFsimage = initFsimage();
    assertNotNull("originalFsImage shouldn't be null", originalFsimage);
    
    // Tests:
    outputOfLSVisitor(originalFsimage);
    outputOfLSVisitorPartitioned(originalFsimage);
    outputOfFileDistributionVisitor(originalFsimage);
    
    unsupportedFSLayoutVersion(originalFsimage);
    
    truncatedFSImage(originalFsimage);
    
    } finally {
      if(originalFsimage != null && originalFsimage.exists())
        originalFsimage.delete();
    }
  }

  // Create a populated namespace for later testing.  Save its contents to a
  // data structure and store its fsimage location.
  private File initFsimage() throws IOException {
    MiniDFSCluster cluster = null;
    File orig = null;
    try {
      Configuration conf = new Configuration();
      //cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      cluster = new MiniDFSCluster(conf, 4, true, (String[]) null);      
      FileSystem hdfs = cluster.getFileSystem();
      
      int filesize = 256;
      
      // Create a reasonable namespace 
      for(int i = 0; i < NUM_DIRS; i++)  {
        Path dir = new Path("/dir" + i);
        Path hardLinkDstDir = new Path("/hardLinkDstDir" + i);
        hdfs.mkdirs(dir);
        writtenFiles.put(dir.toString(),
            pathToFileEntry(hdfs, dir.toString(), cluster));
        
        hdfs.mkdirs(hardLinkDstDir);
        writtenFiles.put(hardLinkDstDir.toString(),
            pathToFileEntry(hdfs, hardLinkDstDir.toString(), cluster));

        for(int j = 0; j < FILES_PER_DIR; j++) {
          Path file = new Path(dir, "file" + j);
          FSDataOutputStream o = hdfs.create(file);
          o.write(new byte[ filesize++ ]);
          o.close();
          
          Path dstFile = new Path(hardLinkDstDir, "hardlinkDstFile" + j);
          hdfs.hardLink(file, dstFile);
          writtenFiles.put(dstFile.toString(),
              pathToFileEntry(hdfs, dstFile.toString(), cluster));
          writtenFiles.put(file.toString(),
              pathToFileEntry(hdfs, file.toString(), cluster));
        }
      }

      // Write results to the fsimage file
      cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNode().saveNamespace();
      
      // Determine location of fsimage file
      File [] files = cluster.getNameDirs().toArray(new File[0]);
      orig = FSImageTestUtil.findNewestImageFile(files[0].getPath() + "/current/");
      
      if (!orig.exists()) {
        fail("Didn't generate or can't find fsimage.");
      }
    } finally {
      if(cluster != null)
        cluster.shutdown();
    }
    return orig;
  }
  
  // Convenience method to generate a file status from file system for 
  // later comparison
  private FileStatusWithHardLink pathToFileEntry(FileSystem hdfs, String file,
      MiniDFSCluster cluster)
        throws IOException {
    long hardlinkId = -1;
    try {
      hardlinkId = cluster.getNameNode().namesystem.dir.getHardLinkId(file);
    } catch (IOException ie) {
      System.out.println("IOException for file : " + file + " " + ie);
    }
    return new FileStatusWithHardLink(hdfs.getFileStatus(new Path(file)),
        hardlinkId);
  }

  // Verify that we can correctly generate an ls-style output for a valid 
  // fsimage
  @SuppressWarnings("unchecked")
  private void outputOfLSVisitor(File originalFsimage) throws IOException {
    File testFile = new File(ROOT, "/basicCheck");
    File outputFile = new File(ROOT, "/basicCheckOutput");
    HashMap<String, FileStatusWithHardLink> tempWritten = (HashMap<String, FileStatusWithHardLink>) writtenFiles
        .clone();

    try {
      copyFile(originalFsimage, testFile);
      
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true, 1, true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);

      oiv.go();
      
      HashMap<String, LsElements> fileOutput = readLsfile(outputFile);
      
      compareNamespaces(tempWritten, fileOutput);
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
    System.out.println("Correctly generated ls-style output.");
  }
  
  @SuppressWarnings("unchecked")
  private void outputOfLSVisitorPartitioned(File originalFsimage) throws IOException {
    File testFile = new File(ROOT, "/partCheck");
    File outputFile = new File(ROOT, "/partCheckOutput");
    HashMap<String, FileStatusWithHardLink> tempWritten = (HashMap<String, FileStatusWithHardLink>) writtenFiles
        .clone();
    int parts = 10;
    
    try {
      copyFile(originalFsimage, testFile);
      
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true, parts, true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);
      oiv.go();
      
      HashMap<String, LsElements> fileOutputParts = new HashMap<String, LsElements>();      
      for (int i = 0; i < parts; i++) {
        File partFile = new File(ROOT, "/partCheckOutput"
            + TextWriterImageVisitor.PART_SUFFIX + i);
        fileOutputParts.putAll(readLsfile(partFile));
      }
      
      compareNamespaces(tempWritten, fileOutputParts);
      
      // compare that the output is the same as for one part
      v = new LsImageVisitor(outputFile.getPath(), true, 1, true);
      oiv = new OfflineImageViewer(testFile.getPath(), v, false);
      oiv.go();
      // if number of parts is 1, the output file name does not change
      assertTrue(outputFile.exists());
      
      HashMap<String, LsElements> fileOutput = readLsfile(outputFile);
      assertEquals(fileOutput, fileOutputParts);
      
      try {
        v = new LsImageVisitor(outputFile.getPath(), true, -1, true);
        fail("Should fail because the number of parts is < 1");
      } catch(Exception e) { }
           
    } finally {
      if(testFile.exists()) testFile.delete();
      for (int i = 0; i < parts; i++) {
        File partFile = new File(ROOT, "/partCheckOutput"
            + TextWriterImageVisitor.PART_SUFFIX + i);
        if(partFile.exists()) {
          partFile.delete();
        }
      }
      if(outputFile.exists()) outputFile.delete();
    }
    System.out.println("Correctly generated ls-style output.");
  }
  
  // Confirm that attempting to read an fsimage file with an unsupported
  // layout results in an error
  public void unsupportedFSLayoutVersion(File originalFsimage) throws IOException {
    File testFile = new File(ROOT, "/invalidLayoutVersion");
    File outputFile = new File(ROOT, "invalidLayoutVersionOutput");
    
    try {
      int badVersionNum = -432;
      changeLayoutVersion(originalFsimage, testFile, badVersionNum);
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);
      
      try {
        oiv.go();
        fail("Shouldn't be able to read invalid laytout version");
      } catch(IOException e) {
        if(!e.getMessage().contains(Integer.toString(badVersionNum)))
          throw e; // wasn't error we were expecting
        System.out.println("Correctly failed at reading bad image version.");
      }
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
  }
  
  // Verify that image viewer will bail on a file that ends unexpectedly
  private void truncatedFSImage(File originalFsimage) throws IOException {
    File testFile = new File(ROOT, "/truncatedFSImage");
    File outputFile = new File(ROOT, "/trucnatedFSImageOutput");
    try {
      copyPartOfFile(originalFsimage, testFile);
      assertTrue("Created truncated fsimage", testFile.exists());
      
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);

      try {
        oiv.go();
        fail("Managed to process a truncated fsimage file");
      } catch (EOFException e) {
        System.out.println("Correctly handled EOF");
      }

    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
  }
  
  // Test that our ls file has all the same compenents of the original namespace
  private void compareNamespaces(
      HashMap<String, FileStatusWithHardLink> written,
      HashMap<String, LsElements> fileOutput) {
    assertEquals( "Should be the same number of files in both, plus one for root"
            + " in fileoutput", fileOutput.keySet().size(), 
                                written.keySet().size() + 1);
    Set<String> inFile = fileOutput.keySet();

    // For each line in the output file, verify that the namespace had a
    // filestatus counterpart 
    for (String path : inFile) {
      if (path.equals("/")) // root's not included in output from system call
        continue;

      assertTrue("Path in file (" + path + ") was written to fs", written
          .containsKey(path));
      
      compareFiles(written.get(path), fileOutput.get(path));
      
      written.remove(path);
    }

    assertEquals("No more files were written to fs", 0, written.size());
  }
  
  // Compare two files as listed in the original namespace FileStatus and
  // the output of the ls file from the image processor
  private void compareFiles(FileStatusWithHardLink fsh, LsElements elements) {
    FileStatus fs = fsh.stat;
    char type = '-';
    if (fs.isDir()) {
      type = 'd';
    } else if (fsh.hardLinkId != -1) {
      type = 'h';
    }
    assertEquals("file type not equal for : " + fs.getPath(), type,
        elements.dir);
    assertEquals("perms string equal", 
                                fs.getPermission().toString(), elements.perms);
    assertEquals("replication equal", fs.getReplication(), elements.replication);
    assertEquals("owner equal", fs.getOwner(), elements.username);
    assertEquals("group equal", fs.getGroup(), elements.groupname);
    assertEquals("lengths equal", fs.getLen(), elements.filesize);
  }

  // Read the contents of the file created by the Ls processor
  private HashMap<String, LsElements> readLsfile(File lsFile) throws IOException {
    assertTrue(lsFile.exists());
    BufferedReader br = new BufferedReader(new FileReader(lsFile));
    String line = null;
    HashMap<String, LsElements> fileContents = new HashMap<String, LsElements>();
    
    while((line = br.readLine()) != null) 
      readLsLine(line, fileContents);
    
    return fileContents;
  }
  
  // Parse a line from the ls output.  Store permissions, replication, 
  // username, groupname and filesize in hashmap keyed to the path name
  private void readLsLine(String line, HashMap<String, LsElements> fileContents) {
    String elements [] = line.split("\\s+");
    
    assertEquals("Not enough elements in ls output", 9, elements.length);
    
    LsElements lsLine = new LsElements();
    
    lsLine.dir = elements[0].charAt(0);
    lsLine.perms = elements[0].substring(1);
    lsLine.replication = elements[1].equals("-") 
                                             ? 0 : Integer.valueOf(elements[1]);
    lsLine.hardlinkId = elements[2].equals("-") ? -1 : Long
        .valueOf(elements[2]);
    lsLine.username = elements[3];
    lsLine.groupname = elements[4];
    lsLine.filesize = Long.valueOf(elements[5]);
    // skipping date and time 
    
    String path = elements[8];
    
    // Check that each file in the ls output was listed once
    assertFalse("LS file had duplicate file entries", 
        fileContents.containsKey(path));
    
    fileContents.put(path, lsLine);
  }
  
  // Copy one fsimage to another, changing the layout version in the process
  private void changeLayoutVersion(File src, File dest, int newVersion) 
         throws IOException {
    DataInputStream in = null; 
    DataOutputStream out = null; 
    
    try {
      in = new DataInputStream(new FileInputStream(src));
      out = new DataOutputStream(new FileOutputStream(dest));
      
      in.readInt();
      out.writeInt(newVersion);
      
      byte [] b = new byte[1024];
      while( in.read(b)  > 0 ) {
        out.write(b);
      }
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }
  
  // Only copy part of file into the other.  Used for testing truncated fsimage
  private void copyPartOfFile(File src, File dest) throws IOException {
    InputStream in = null;
    OutputStream out = null;
    
    byte [] b = new byte[256];
    int bytesWritten = 0;
    int count;
    int maxBytes = 700;
    
    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest);
      
      while( (count = in.read(b))  > 0 && bytesWritten < maxBytes ) {
        out.write(b);
        bytesWritten += count;
      } 
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }
  
  // Copy one file's contents into the other
  private void copyFile(File src, File dest) throws IOException {
    InputStream in = null;
    OutputStream out = null;
    
    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest);

      byte [] b = new byte[1024];
      while( in.read(b)  > 0 ) {
        out.write(b);
      }
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }

  private void outputOfFileDistributionVisitor(File originalFsimage) throws IOException {
    File testFile = new File(ROOT, "/basicCheck");
    File outputFile = new File(ROOT, "/fileDistributionCheckOutput");

    int totalFiles = 0;
    try {
      copyFile(originalFsimage, testFile);
      ImageVisitor v = new FileDistributionVisitor(outputFile.getPath(), 0, 0);
      OfflineImageViewer oiv = 
        new OfflineImageViewer(testFile.getPath(), v, false);

      oiv.go();

      BufferedReader reader = new BufferedReader(new FileReader(outputFile));
      String line = reader.readLine();
      assertEquals(line, "Size\tNumFiles");
      while((line = reader.readLine()) != null) {
        String[] row = line.split("\t");
        assertEquals(row.length, 2);
        totalFiles += Integer.parseInt(row[1]);
      }
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
    assertEquals(totalFiles, 2 * NUM_DIRS * FILES_PER_DIR);
  }
}
