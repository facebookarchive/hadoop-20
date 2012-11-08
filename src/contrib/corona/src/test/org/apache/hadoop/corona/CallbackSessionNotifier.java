package org.apache.hadoop.corona;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class CallbackSessionNotifier extends SessionNotifier {

  private ConcurrentHashMap<String, SessionListener> sessions =
      new ConcurrentHashMap<String, SessionListener>();

  public static final Log LOG = LogFactory.getLog(FakeSessionNotifier.class);
  public CallbackSessionNotifier(SessionManager sessionManager,
                             ClusterManager clusterManager, ClusterManagerMetrics metrics) {
    super(null, null, metrics);
  }

  public void notifyGrantResource(String handle, List<ResourceGrant> granted) {
    reportGrantMetrics(granted);
    sessions.get(handle).notifyGrantResource(granted);
  }

  public void notifyRevokeResource(String handle, List<ResourceGrant> revoked,
                                   boolean force) {
    reportRevokeMetrics(revoked);
    LOG.info("notifyRevokeResource handle:" + handle +
        " revoked:" + revoked.size());
  }



  @Override
  public void notifyDeadNode(String handle, String nodeName) {
    LOG.info("notifyDeadNode handle: " + handle + " node=" + nodeName);
  }

  public void deleteSession(String handle) {
    LOG.info("deleteSession handle:" + handle);
  }

  public void setConf(Configuration _conf) {
  }

  public Configuration getConf() {
    return null;
  }

  public void addSession(String handle, SessionListener listener) {
    this.sessions.put(handle, listener);
  }
}
