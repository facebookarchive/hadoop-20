package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Denotes an operation written to the transaction log of the namenode.
 */
public class FSEditOp implements Writable {
  private byte op;
  private long txid;

  public static FSEditOp getFSEditOp(byte op, long txid) {
    return new FSEditOp(op, txid);
  }

  /**
   * Read a {@link FSEditOp} from the given stream.
   * 
   * @param in
   *          the input stream to read data from
   * @return an instance of {@link FSEditOp}
   * @throws IOException
   *           if there was an error reading from the stream
   */
  public static FSEditOp getFSEditOp(DataInput in) throws IOException {
    FSEditOp op = new FSEditOp();
    op.readFields(in);
    return op;
  }

  private FSEditOp() {
  }

  private FSEditOp(byte op, long txid) {
    this.op = op;
    this.txid = txid;
  }

  public byte getOpCode() {
    return this.op;
  }

  public long getTxId() {
    return this.txid;
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(op);
    out.writeLong(txid);
  }

  public void readFields(DataInput in) throws IOException {
    this.op = in.readByte();
    // Don't want to throw any exception when we have an OP_INVALID opcode since
    // we would end up reading a non existent long from the stream (possibility
    // of an EOFException)
    if (this.op != FSEditLog.OP_INVALID) {
      this.txid = in.readLong();
    }
  }

}
