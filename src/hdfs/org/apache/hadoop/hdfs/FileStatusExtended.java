package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.Writable;

public class FileStatusExtended extends FileStatus implements Writable {
  private Block[] blocks;
  private String leaseHolder;
  
  public FileStatusExtended() {}

  public FileStatusExtended(FileStatus stat, Block[] blocks, String leaseHolder) {
    super(stat.getLen(), stat.isDir(), stat.getReplication(),
        stat.getBlockSize(), stat.getModificationTime(), stat.getAccessTime(),
        stat.getPermission(), stat.getOwner(), stat.getGroup(), 
        stat.getPath());
    this.blocks = blocks;
    this.leaseHolder = (leaseHolder == null) ? "" : leaseHolder;
  }
  
  public Block[] getBlocks() {
    return this.blocks;
  }

  public String getHolder() {
    return leaseHolder;
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    int nblocks = (blocks == null) ? 0 : blocks.length;
    out.writeInt(nblocks);
    for (int i = 0; i < nblocks; i++) {
      blocks[i].write(out);
    }
    out.writeUTF(leaseHolder);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int nblocks = in.readInt();
    blocks = new Block[nblocks];
    for (int i = 0; i < nblocks; i++) {
      blocks[i] = new Block();
      blocks[i].readFields(in);
    }
    leaseHolder = in.readUTF();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    FileStatusExtended other = (FileStatusExtended) obj;
    if (!Arrays.equals(blocks, other.blocks))
      return false;
    if (!leaseHolder.equals(other.leaseHolder))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "FileStatusExtended [blocks=" + Arrays.toString(blocks)
        + ", leaseHolder=" + leaseHolder + "] " + super.toString();
  }
}
