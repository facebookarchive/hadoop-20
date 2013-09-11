package org.apache.hadoop.hdfs.notifier.server;

import java.util.Queue;

import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;

class EmptyServerHistory implements IServerHistory {

  @Override
  public void setHistoryLength(long newHistoryLength) {}

  @Override
  public void setHistoryLimit(long newHistoryLimit) {}
  
  @Override
  public void run() {}

  @Override
  public boolean isRampUp() {
    return false;
  }

  @Override
  public void storeNotification(NamespaceNotification notification) {}

  @Override
  public void addNotificationsToQueue(NamespaceEvent event, long txId,
      Queue<NamespaceNotification> notifications)
      throws TransactionIdTooOldException {}

}
