package org.apache.hadoop.hdfs.notifier.server;

public class EmptyServerClientTracker implements IServerClientTracker{

  @Override
  public void run() {}

  @Override
  public void setClientTimeout(long timeout) {}

  @Override
  public void setHeartbeatTimeout(long timeout) {}

  @Override
  public void handleFailedDispatch(long clientId, long lastFailed) {}

  @Override
  public void handleSuccessfulDispatch(long clientId, long lastSent) {}

}
