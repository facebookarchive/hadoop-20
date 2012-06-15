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

/**
 * 
 * This class provides locking mechanism that allows for concurrent reads.
 * 
 * In addition, it provides an upgradeable read lock, that can be acquired
 * by a single thread. It blocks any writes, but does not block other 
 * ordinary readers, unless the lock is upgraded.
 * 
 * upgradeableReadLock is mutualy exclusive with another upgradeable read lock,
 * which guarantees that the upgrade will succeed. It's also mutualy aclusive
 * with the read lock. It's mutually exlusive with a read lock, only when
 * upgraded.
 * 
 * Usage:
 * 
 * 1. similarly for readLock()
 * writeLock();
 * try{
 *   ...
 * } finally {
 *   writeUnlock();
 * }
 * 
 * 2.
 * upgradeableReadLock();
 * try{
 *   ..
 *   ..
 *   upgradeLock();
 *   ..
 *   downgradeLock();
 *   ..
 *   upgradeLock();
 *   ..
 *   downgradeLock();
 * } finally {
 *   upgradeableReadUnlock();
 * }
 * 
 */

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FSNamesystemLock {

  private ReentrantReadWriteLock lock1;
  private ReentrantReadWriteLock lock2;
  private boolean hasRwLock;

  FSNamesystemLock(boolean hasRwLock) {
    this.hasRwLock = hasRwLock;
    this.lock1 = new ReentrantReadWriteLock();
    this.lock2 = new ReentrantReadWriteLock();
  }

  FSNamesystemLock() {
    this(true);
  }

  /**
   * Acquire read lock.
   */
  void readLock() {
    if (this.hasRwLock) {
      this.lock2.readLock().lock();
    } else {
      writeLock();
    }
  }

  /**
   * Release read lock.
   */
  void readUnlock() {
    if (this.hasRwLock) {
      this.lock2.readLock().unlock();
    } else {
      writeUnlock();
    }
  }

  /**
   * Acquire full write lock.
   */
  void writeLock() {
    this.lock1.writeLock().lock();
    this.lock2.writeLock().lock();
  }

  /**
   * Release full write lock.
   */
  void writeUnlock() {
    this.lock2.writeLock().unlock();
    this.lock1.writeLock().unlock();
  }

  boolean hasWriteLock() {
    // it's enough to say if the readLock.writelock is acquired
    // as it's only acquired by writeLock or by upgrade
    return this.lock2.isWriteLockedByCurrentThread();
  }

  /**
   * Acquire the upgradeable lock.
   */
  void upgradeableReadLock() {
    this.lock1.writeLock().lock();
  }

  /**
   * Downgrade if necessary.
   * Release the upgradeable lock.
   */
  void upgradeableReadUnlock() {
    // we need to check if it was upgraded
    if (lock2.isWriteLockedByCurrentThread()) {
      this.lock2.writeLock().unlock();
    }
    this.lock1.writeLock().unlock();
  }

  /**
   * Upgrade the upgradeable lock.
   * Upgrading should only be used after acquiring 
   * the upgradeable lock.
   */
  void upgradeLock() {
    this.lock2.writeLock().lock();
  }

  /**
   * Downgrade the upgradeable lock.
   * Downgrading should not be used in "finally"
   * Downgrade will fail if the lock has not been upgraded.
   */
  void downgradeLock() {
    this.lock2.writeLock().unlock();
  }
}
