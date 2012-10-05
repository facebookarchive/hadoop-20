package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Failover client interface for various implementations to invoke their own
 * processing during failover.
 */
public interface FailoverClient {

  /**
   * The client tries to failover to the new primary and reports success or
   * failure.
   *
   * @return true if we failed over successfully, false otherwise.
   */
  public boolean tryFailover() throws IOException;

  /**
   * Returns whether or not the client is shut down
   */
  public boolean isShuttingdown();

  /**
   * Denotes whether or not we are in failvoer and don't know who the primary is
   */
  public boolean isFailoverInProgress();

  /**
   * Tells the client that the namenode it was contacting earlier is down and it
   * should now try to perform a failover
   */
  public void nameNodeDown();

  /**
   * Once we know about a new namenode, instruct the client that it should
   * failover to this new namenode
   */
  public void newNamenode(VersionedProtocol namenode);
}
