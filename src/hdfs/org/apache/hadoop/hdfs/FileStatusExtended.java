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
  // HardLink id of -1 denotes the file is not hardlinked.
  private long hardlinkId;
  
  public FileStatusExtended() {}

  public FileStatusExtended(FileStatus stat, Block[] blocks,
      String leaseHolder, long hardlinkId) {
    super(stat.getLen(), stat.isDir(), stat.getReplication(),
        stat.getBlockSize(), stat.getModificationTime(), stat.getAccessTime(),
        stat.getPermission(), stat.getOwner(), stat.getGroup(), 
        stat.getPath());
    this.blocks = blocks;
    this.leaseHolder = (leaseHolder == null) ? "" : leaseHolder;
    this.hardlinkId = hardlinkId;
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
    out.writeLong(hardlinkId);
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
    hardlinkId = in.readLong();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    FileStatusExtended other = (FileStatusExtended) obj;
    if (!leaseHolder.equals(other.leaseHolder)
        || hardlinkId != other.hardlinkId)
      return false;
    boolean closedFile = leaseHolder.isEmpty();
    if (!super.compareFull(obj, closedFile)) {
      return false;
    }
    if (!blocksEquals(blocks, other.blocks, closedFile))
      return false;
    return true;
  }
  
  /**
   * Comapre two arrays of blocks. If the file is open, do not compare 
   * sizes of the blocks.
   */
  private boolean blocksEquals(Block[] a1, Block[] a2, boolean closedFile) {
    if (a1 == a2)
      return true;
    if (a1 == null || a2 == null || a2.length != a1.length)
      return false;

    for (int i = 0; i < a1.length; i++) {
      Block b1 = a1[i];
      Block b2 = a2[i];

      if (b1 == b2)
        continue;
      if (b1 == null || b2 == null)
        return false;

      // compare ids and gen stamps
      if (!(b1.getBlockId() == b2.getBlockId() && b1.getGenerationStamp() == b2
          .getGenerationStamp()))
        return false;

      // for open files check len-2 blocks only
      if (!closedFile && i >= a1.length - 2)
        continue;

      // check block size
      if (b1.getNumBytes() != b2.getNumBytes())
        return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "FileStatusExtended [blocks=" + Arrays.toString(blocks)
        + ", leaseHolder=" + leaseHolder + ", hardlinkId =" + hardlinkId + "]"
        + super.toString();
  }
}
