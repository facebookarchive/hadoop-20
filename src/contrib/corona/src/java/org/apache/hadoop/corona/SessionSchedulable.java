package org.apache.hadoop.corona;

/**
 * Maintains parameters for a session of a given type
 */
public class SessionSchedulable extends Schedulable {

  private final Session session;

  private long localityWaitStartTime;
  private LocalityLevel localityRequired;
  private boolean localityWaitStarted;

  public SessionSchedulable(Session session, String type) {
    super(session.getName(), type);
    this.session = session;
    this.localityRequired = LocalityLevel.NODE;
    this.localityWaitStartTime = Long.MAX_VALUE;
    this.localityWaitStarted = false;
  }

  @Override
  public void snapshot() {
    synchronized (session) {
      if (session.deleted) {
        requested = 0;
        granted = 0;
      } else {
        requested = session.getRequestCountForType(getType());
        granted =session.getGrantCountForType(getType());
      }
    }
  }

  @Override
  public long getStartTime() {
    return session.getStartTime();
  }

  public Session getSession() {
    return session;
  }

  public boolean isLocalityGoodEnough(LocalityLevel currentLevel) {
    return !localityRequired.isBetterThan(currentLevel);
  }

  public void startLocalityWait(long now) {
    if (localityWaitStarted == true) {
      return;
    }
    localityWaitStarted = true;
    localityWaitStartTime = now;
  }

  public void adjustLocalityRequirement(
      long now, long nodeWait, long rackWait) {
    if (!localityWaitStarted) {
      return;
    }
    if (localityRequired == LocalityLevel.ANY) {
      return;
    }
    if (localityRequired == LocalityLevel.NODE) {
      if (now - localityWaitStartTime > nodeWait) {
        setLocalityLevel(LocalityLevel.RACK);
      }
    }
    if (localityRequired == LocalityLevel.RACK) {
      if (now - localityWaitStartTime > rackWait) {
        setLocalityLevel(LocalityLevel.ANY);
      }
    }
  }

  public void setLocalityLevel(LocalityLevel level) {
    localityRequired = level;
    localityWaitStarted = false;
    localityWaitStartTime = Long.MAX_VALUE;
  }
}
