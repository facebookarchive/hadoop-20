package org.apache.hadoop.hdfs.notifier.server;

public class EmptyServerClientTracker implements IServerClientTracker{

  @Override
  public void run() {}

  @Override
  public void setClientTimeout(long timeout) {}

  @Override
  public void setHeartbeatTimeout(long timeout) {}

  @Override
  public void clientHandleNotificationFailed(long clientId, long lastFailed) {}

  @Override
  public void clientHandleNotificationSuccessful(long clientId, long lastSent) {}

}
