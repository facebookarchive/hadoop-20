package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.util.SerializableUtils;

/**
 * Represents the last transaction id in the NameNode (before it shuts down)
 * which is synced to zookeeper upon shutdown. It also contains the session id
 * for the cluster as well as the total blocks for the cluster.
 */
public class ZookeeperTxId implements Serializable {
  private static final long serialVersionUID = 4616549874617128792L;
  private long ssid;
  private long txid;
  private long totalBlocks;
  private long totalInodes;

  public ZookeeperTxId() {
  }

  public ZookeeperTxId(long ssid, long txid, long totalBlocks, long totalInodes) {
    this.ssid = ssid;
    this.txid = txid;
    this.totalBlocks = totalBlocks;
    this.totalInodes = totalInodes;
  }
  
  public static ZookeeperTxId getFromBytes(byte[] data) throws IOException,
      ClassNotFoundException {
    return (ZookeeperTxId) SerializableUtils.getFromBytes(data,
        ZookeeperTxId.class);
  }

  public byte[] toBytes() throws IOException {
    return SerializableUtils.toBytes(this);
  }
  
  public String toString() {
    return String.format(
        "TxId(ssid: %d, txid: %d, totalBlocks: %d, totalInodes: %d)", ssid,
        txid, totalBlocks, totalInodes);
  }

  private void writeObject(ObjectOutputStream out)
  throws IOException {
    out.writeLong(ssid);
    out.writeLong(txid);
    out.writeLong(totalBlocks);
    out.writeLong(totalInodes);
  }

  private void readObject(ObjectInputStream in) throws IOException,
      ClassNotFoundException {
    this.ssid = in.readLong();
    this.txid = in.readLong();
    this.totalBlocks = in.readLong();
    this.totalInodes = in.readLong();
  }

  public long getTotalInodes() {
    return this.totalInodes;
  }

  public long getSessionId() {
    return this.ssid;
  }

  public long getTransactionId() {
    return this.txid;
  }

  public long getTotalBlocks() {
    return this.totalBlocks;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    ZookeeperTxId other = (ZookeeperTxId) obj;
    return (ssid == other.ssid && txid == other.txid
        && totalBlocks == other.totalBlocks && totalInodes == other.totalInodes);
  }
}
