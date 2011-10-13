package org.apache.hadoop.util;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Maintains a Write Ahead Log (WAL).
 * The WAL is a stream of LogRecords backed by a persistent store.
 * Clients of this class create LogRecord objects and call writeRecord().
 * A client can call flush() when persistence is required.
 */
public class WriteAheadLog {
  /**
   * Interface to be implemented by clients interested in recovery from WAL.
   */
  public interface LogRecovery {
    public void consumeLog(LogRecord record);
  }

  /**
   * Generic Log Record in the WAL.
   * -----------------------------------------
   * | 64-bit LSN | 32-bit size | data ...    |
   * -----------------------------------------
   */
  public class LogRecord {
    private long lsn;
    private byte[] data;

    public LogRecord(long lsn) { this.lsn = lsn; }

    public long getLSN() { return this.lsn; }

    public void setData(byte[] data) { this.data = data; }

    public void writeRecord(DataOutputStream out) throws IOException {
      // Write header: lsn, data size.
      out.writeLong(lsn);
      out.writeInt(data.length);
      // Write data.
      out.write(data, 0, data.length);
    }

    public void readRecord(DataInputStream in) throws IOException {
      // Read header.
      lsn = in.readLong();
      int size = in.readInt();
      data = new byte[size];
      in.readFully(data);
    }
  }

  /**
   * Clients should call this with records in increasing order of LSN.
   * @return true on success, false on failure;
   */
  public boolean writeRecord(LogRecord record) {
    return false;
  }

  /**
   * Initiate flushing of log records.
   * @return LSN of last log record.
   */
  public long flush() {
    return 0;
  }

  /**
   * Inserts a Checkpoint record in the log stream and persists everything
   * till the record to disk.
   * @return the LSN of the checkpoint record.
   */
  public long doCheckpoint() {
    return 0;
  }
}
