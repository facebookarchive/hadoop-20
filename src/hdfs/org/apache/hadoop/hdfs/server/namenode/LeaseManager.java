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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.util.InjectionHandler;

/**
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 * 
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p

 * 2.3) p obtains a new generation stamp form the namenode
 * 2.4) p get the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 *      with the new generation stamp and the minimum block length 
 * 2.7) p acknowledges the namenode the update results

 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 *      and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
public class LeaseManager {
  public static final Log LOG = LogFactory.getLog(LeaseManager.class);

  private final FSNamesystem fsnamesystem;

  private long softLimit = FSConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = FSConstants.LEASE_HARDLIMIT_PERIOD;
  private int rpcBatchSize = 1000; // safe default. real value fetched from config
  //max number of paths to check per batch
  private int maxPathsPerCheck = Integer.MAX_VALUE; 
  /** if lease recovery could discard the last block without sync data */
  private boolean discardLastBlockIfNoSync = false;
  private long leaseCheckInterval = 2000;

  //
  // Used for handling lock-leases
  // Mapping: leaseHolder -> Lease
  //
  private SortedMap<String, Lease> leases = new TreeMap<String, Lease>();
  // Set of: Lease
  private LightWeightLinkedSet<Lease> sortedLeases = new LightWeightLinkedSet<Lease>();

  // 
  // Map path names to leases. It is protected by the sortedLeases lock.
  // The map stores pathnames in lexicographical order.
  //
  private SortedMap<String, LeaseOpenTime> sortedLeasesByPath =
    new TreeMap<String, LeaseOpenTime>();

  /**
   * A Lease and an associated open time
   */
  private static class LeaseOpenTime {
    public LeaseOpenTime(Lease lease, long openTime) {
      this.lease = lease;
      this.openTime = openTime;
    }

    Lease lease;        // the lease object
    long openTime;      // the time the lease was created
  }

  LeaseManager(FSNamesystem fsnamesystem) {
    this.fsnamesystem = fsnamesystem;
    if (fsnamesystem != null) {
      Configuration conf = fsnamesystem.getConf();
      this.rpcBatchSize = conf.getInt(
          "dfs.namemode.leasemanager.rpc.batchsize", 1000);
      this.maxPathsPerCheck = conf.getInt(
          "dfs.namenode.leasemanager.maxpathspercheck", Integer.MAX_VALUE);
      this.discardLastBlockIfNoSync = conf.getBoolean(
          "dfs.leaserecovery.discardlastblock.ifnosync", false);
      this.leaseCheckInterval = conf.getLong("lease.check.interval", 2000);
    }
    
  }

  Lease getLease(String holder) {
    return leases.get(holder);
  }
  
  LightWeightLinkedSet<Lease> getSortedLeases() {return sortedLeases;}

  /** @return the lease containing src */
  public Lease getLeaseByPath(String src) {
    LeaseOpenTime leaseOpenTime = sortedLeasesByPath.get(src);
    if (leaseOpenTime != null)
      return leaseOpenTime.lease;
    else
      return null;
  }

  /** @return the number of leases currently in the system */
  public synchronized int countLease() {return sortedLeases.size();}

  /** @return the number of paths contained in all leases */
  public synchronized int countPath() {
    int count = 0;
    for(Lease lease : sortedLeases) {
      count += lease.getPaths().size();
    }
    return count;
  }
  
  /**
   * Adds (or re-adds) the lease for the specified file.
   * @param client that will hold the lease
   * @param src file path to associated with the lease.
   * @param timestamp time that the file was opened. (could be in the
   * past if loaded from FsImage
   */
  synchronized Lease addLease(String holder, String src, long timestamp) {
    Lease lease = getLease(holder);
    if (lease == null) {
      lease = new Lease(holder);
      leases.put(holder, lease);
      sortedLeases.add(lease);
    } else {
      renewLease(lease);
    }
    sortedLeasesByPath.put(src, new LeaseOpenTime(lease, timestamp));
    lease.paths.add(src);

    return lease;
  }

  /**
    * Reassign lease for file src to the new holder.
    */
   synchronized Lease reassignLease(Lease lease, String src, String newHolder) {
     assert newHolder != null : "new lease holder is null";
     LeaseOpenTime leaseOpenTime = null;
     if (lease != null) {
       leaseOpenTime = removeLease(lease, src);
     }
     return addLease(newHolder, src,
                     leaseOpenTime != null ?
                     leaseOpenTime.openTime :
                     System.currentTimeMillis());
   }
   
   /**
    * Find the pathname for the specified pendingFile
    */
   synchronized String findPath(INodeFileUnderConstruction pendingFile) 
         throws IOException {
     Lease lease = getLease(pendingFile.getClientName());
     if (lease != null) {
       String src = lease.findPath(pendingFile);
       if (src != null) {
         return src;
       }
     }
     throw new IOException("pendingFile (=" + pendingFile + ") not found."
           + "(lease=" + lease + ")");
   }
 
  /**
   * Remove the specified lease and src.
   */
  synchronized LeaseOpenTime removeLease(Lease lease, String src) {
    LeaseOpenTime leaseOpenTime = sortedLeasesByPath.remove(src);
    if (!lease.removePath(src)) {
      LOG.error(src + " not found in lease.paths (=" + lease.paths + ")");
    }

    if (!lease.hasPath()) {
      leases.remove(lease.holder);
      if (!sortedLeases.remove(lease)) {
        LOG.error(lease + " not found in sortedLeases");
      }
    }
    return leaseOpenTime;
  }

  /**
   * Remove the lease for the specified holder and src
   */
  synchronized void removeLease(String holder, String src) {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, src);
    }
  }

  /**
   * Fetch the list of files that have been open longer than a
   * specified amount of time.
   * @param prefix path prefix specifying subset of files to examine
   * @param millis select files that have been open longer that this
   * @param startAfter null, or the last path value returned by previous call
   * @return array of OpenFileInfo objects
   * @throw IOException
   */
  synchronized public OpenFileInfo[] iterativeGetOpenFiles(
    String prefix, int millis, String startAfter) {
      final long thresholdMillis = System.currentTimeMillis() - millis;

      // this flag is for subsequent calls that included a 'start'
      // parameter. in those cases, we need to throw out the first
      // result
      boolean skip = false;
      String jumpTo = startAfter;
      if (jumpTo == null || jumpTo.compareTo("") == 0)
        jumpTo = prefix;
      else
        skip = true;

      ArrayList<OpenFileInfo> entries =  new ArrayList<OpenFileInfo>();

      final int srclen = prefix.length();
      for(Map.Entry<String, LeaseOpenTime> entry : sortedLeasesByPath.tailMap(jumpTo).entrySet()) {
        final String p = entry.getKey();
        if (!p.startsWith(prefix)) {
          // traversed past the prefix, so we're done
          OpenFileInfo[] result = entries.toArray(
            new OpenFileInfo[entries.size()]);
          return result;
        }
        if (skip) {
          skip = false;
        } else if (p.length() == srclen || p.charAt(srclen) == Path.SEPARATOR_CHAR) {
          long openTime = entry.getValue().openTime;
          if (openTime <= thresholdMillis) {
            entries.add(new OpenFileInfo(entry.getKey(), openTime));
            if (entries.size() >= rpcBatchSize) {
              // reached the configured batch size, so return this subset
              OpenFileInfo[] result = entries.toArray(
                new OpenFileInfo[entries.size()]);
              return result;
            }
          }
        }
      }
      // reached the end of the list of files, so we're done
      OpenFileInfo[] result = entries.toArray(
        new OpenFileInfo[entries.size()]);
      return result;
    }
  
  /**
   * Return the current view of all lease holders.
   * The view is unmodifiable to prevent outside changes.
   */
  synchronized Collection<String> getLeaseHolders() {
    return Collections.unmodifiableSet(leases.keySet());
  }

  /**
   * Renew the lease(s) held by the given client
   */
  synchronized void renewLease(String holder) {
    renewLease(getLease(holder));
  }
  synchronized void renewLease(Lease lease) {
    if (lease != null) {
      sortedLeases.remove(lease);
      lease.renew();
      sortedLeases.add(lease);
    }
  }

  /**
   * for testing only 
   */
  synchronized void replaceLease(Lease newLease) {
    leases.put(newLease.getHolder(), newLease);
    sortedLeases.remove(newLease);
    sortedLeases.add(newLease);

    for (String path : newLease.paths) {
      sortedLeasesByPath.put(
        path, new LeaseOpenTime(newLease, System.currentTimeMillis()));
    }
  }
  /************************************************************
   * A Lease governs all the locks held by a single client.
   * For each client there's a corresponding lease, whose
   * timestamp is updated when the client periodically
   * checks in.  If the client dies and allows its lease to
   * expire, all the corresponding locks can be released.
   *************************************************************/
  class Lease implements Comparable<Lease> {
    private final String holder;
    private long lastUpdate;
    private final Collection<String> paths = new TreeSet<String>();
  
    /** Only LeaseManager object can create a lease */
    private Lease(String holder) {
      this.holder = holder;
      renew();
    }
    /** Only LeaseManager object can renew a lease */
    private void renew() {
      this.lastUpdate = FSNamesystem.now();
    }

    /** @return true if the Hard Limit Timer has expired */
    public boolean expiredHardLimit() {
      return FSNamesystem.now() - lastUpdate > hardLimit;
    }

    /** @return true if the Soft Limit Timer has expired */
    public boolean expiredSoftLimit() {
      return FSNamesystem.now() - lastUpdate > softLimit;
    }
    
    /**
     * @return the path associated with the pendingFile and null if not found
     */
    private String findPath(INodeFileUnderConstruction pendingFile) {
      for (String src : paths) {
        if (fsnamesystem.dir.getFileINode(src) == pendingFile) {
          return src;
        }
      }
      return null;
    }

    /** Does this lease contain any path? */
    boolean hasPath() {return !paths.isEmpty();}

    boolean removePath(String src) {
      return paths.remove(src);
    }

    /** {@inheritDoc} */
    public String toString() {
      return "[Lease.  Holder: " + holder
          + ", pendingcreates: " + paths.size() + "]";
    }
  
    /** {@inheritDoc} */
    public int compareTo(Lease o) {
      Lease l1 = this;
      Lease l2 = o;
      long lu1 = l1.lastUpdate;
      long lu2 = l2.lastUpdate;
      if (lu1 < lu2) {
        return -1;
      } else if (lu1 > lu2) {
        return 1;
      } else {
        return l1.holder.compareTo(l2.holder);
      }
    }
  
    /** {@inheritDoc} */
    public boolean equals(Object o) {
      if (!(o instanceof Lease)) {
        return false;
      }
      Lease obj = (Lease) o;
      if (lastUpdate == obj.lastUpdate &&
          holder.equals(obj.holder)) {
        return true;
      }
      return false;
    }
  
    /** {@inheritDoc} */
    public int hashCode() {
      return holder.hashCode();
    }
    
    Collection<String> getPaths() {
      return paths;
    }
    
    void replacePath(String oldpath, String newpath) {
      paths.remove(oldpath);
      paths.add(newpath);
    }
    
    String getHolder() {
      return holder;
    }
  }

  synchronized void changeLease(String src, String dst,
      String overwrite, String replaceBy) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".changelease: " +
               " src=" + src + ", dest=" + dst + 
               ", overwrite=" + overwrite +
               ", replaceBy=" + replaceBy);
    }

    final int len = overwrite.length();
    for(Map.Entry<String, LeaseOpenTime> entry : findLeaseWithPrefixPath(src, sortedLeasesByPath)) {
      final String oldpath = entry.getKey();
      final Lease lease = entry.getValue().lease;
      //overwrite must be a prefix of oldpath
      final String newpath = replaceBy + oldpath.substring(len);
      if (LOG.isDebugEnabled()) {
        LOG.debug("changeLease: replacing " + oldpath + " with " + newpath);
      }
      lease.replacePath(oldpath, newpath);
      LeaseOpenTime openTime = sortedLeasesByPath.remove(oldpath);
      sortedLeasesByPath.put(newpath, openTime);
    }
  }

  synchronized void removeLeaseWithPrefixPath(String prefix) {
    for(Map.Entry<String, LeaseOpenTime> entry : findLeaseWithPrefixPath(prefix, sortedLeasesByPath)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(LeaseManager.class.getSimpleName()
            + ".removeLeaseWithPrefixPath: entry=" + entry);
      }
      removeLease(entry.getValue().lease, entry.getKey());    
    }
  }

  static private List<Map.Entry<String, LeaseOpenTime>> findLeaseWithPrefixPath(
    String prefix, SortedMap<String, LeaseOpenTime> path2lease) {

    if (LOG.isDebugEnabled()) {
      LOG.debug(LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    List<Map.Entry<String, LeaseOpenTime>> entries =
      new ArrayList<Map.Entry<String, LeaseOpenTime>>();
    final int srclen = prefix.length();

    for(Map.Entry<String, LeaseOpenTime> entry : path2lease.tailMap(prefix).entrySet()) {
      final String p = entry.getKey();
      if (!p.startsWith(prefix)) {
        return entries;
      }
      if (p.length() == srclen || p.charAt(srclen) == Path.SEPARATOR_CHAR) {
        entries.add(entry);
      }
    }
    return entries;
  }


  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit; 
  }
  
  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  class Monitor implements Runnable {
    final String name = getClass().getSimpleName();
    private volatile boolean running = true;
    
    public void stop() {
      running = false;
    }

    /** Check leases periodically. */
    public void run() {
      for(; running && fsnamesystem.isRunning(); ) {
        InjectionHandler.processEvent(InjectionEvent.LEASEMANAGER_CHECKLEASES);
        fsnamesystem.writeLock();
        try {
          // this check needs to be done when holding write lock to ensure
          // the interruption from FSNamesystem.stopLeaseMonitor() does not
          // come during checkLeases(). Please see
          // FSNamesystem.stopLeaseMonitor(). This is crucial during failover!!
          if ((Thread.currentThread().isInterrupted() || !running)
              && InjectionHandler
                  .trueCondition(InjectionEvent.LEASEMANAGER_CHECKINTERRUPTION)) {
            LOG.info("LeaseManager received shutdown signal - exiting");
            break;
          }
          if (!fsnamesystem.isInSafeMode()) {
            checkLeases();
          }
        } finally {
          fsnamesystem.writeUnlock();
        }

        try {
          // The interruption should only affect the sleep()
          Thread.sleep(leaseCheckInterval);
        } catch(InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted", ie);
          }
        }
      }
    }
  }

  /** Check the leases beginning from the oldest. */
  synchronized void checkLeases() {
    int numPathsChecked = 0;
    for(; sortedLeases.size() > 0; ) {
      final Lease oldest = sortedLeases.first();
      if (!oldest.expiredHardLimit()) {
        return;
      }
      
      // internalReleaseLease() removes paths corresponding to empty files,
      // i.e. it needs to modify the collection being iterated over
      // causing ConcurrentModificationException
      String[] leasePaths = new String[oldest.getPaths().size()];
      oldest.getPaths().toArray(leasePaths);
      LOG.info("Lease " + oldest
          + " has expired hard limit. Recovering lease for paths: "
          + Arrays.toString(leasePaths));
      for(String p : leasePaths) {
        if (++numPathsChecked > this.maxPathsPerCheck) {
          return;
        }
        try {
          fsnamesystem.getFSNamesystemMetrics().numLeaseRecoveries.inc();
          fsnamesystem.internalReleaseLeaseOne(
              oldest, p, this.discardLastBlockIfNoSync);
        } catch (IOException e) {
          LOG.error("Cannot release the path "+p+" in the lease "+oldest, e);
          removeLease(oldest, p);
          fsnamesystem.getFSNamesystemMetrics().numLeaseManagerMonitorExceptions.inc();
        }
      }
    }
  }

  /** {@inheritDoc} */
  public synchronized String toString() {
    return getClass().getSimpleName() + "= {"
        + "\n leases=" + leases
        + "\n sortedLeases=" + sortedLeases
        + "\n sortedLeasesByPath=" + sortedLeasesByPath
        + "\n}";
  }
}
