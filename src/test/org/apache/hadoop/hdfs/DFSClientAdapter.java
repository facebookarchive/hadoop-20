package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class DFSClientAdapter {
  public static void abortForTest(FSDataOutputStream out) throws IOException {
    OutputStream stream = out.getWrappedStream();

    if (stream instanceof DFSOutputStream) {
      DFSOutputStream dfsOutputStream =
        (DFSOutputStream) stream;
      dfsOutputStream.abortForTests();
    }
    //no-op otherwise
  }
}
