package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class OpenFilesInfo implements Writable {
  private final List<FileStatusExtended> openFiles;
  private long genStamp;
  
  public OpenFilesInfo() {
    this.openFiles = new ArrayList<FileStatusExtended>();
  }

  public OpenFilesInfo(List<FileStatusExtended> openFiles, long genStamp) {
    this.openFiles = (openFiles != null) ? openFiles
        : new ArrayList<FileStatusExtended>();
    this.genStamp = genStamp;
  }

  public List<FileStatusExtended> getOpenFiles() {
    return this.openFiles;
  }

  public long getGenStamp() {
    return this.genStamp;
  }

  public void write(DataOutput out) throws IOException {
    int nFiles = openFiles.size();
    out.writeInt(nFiles);
    for (FileStatusExtended stat : openFiles) {
      stat.write(out);
    }
    out.writeLong(genStamp);
  }

  public void readFields(DataInput in) throws IOException {
    int nFiles = in.readInt();
    for (int i = 0; i < nFiles; i++) {
      FileStatusExtended stat = new FileStatusExtended();
      stat.readFields(in);
      this.openFiles.add(stat);
    }
    this.genStamp = in.readLong();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    OpenFilesInfo other = (OpenFilesInfo) obj;
    if (genStamp != other.genStamp || !openFiles.equals(other.openFiles))
      return false;
    return true;
  }
}
