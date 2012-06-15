package org.apache.hadoop.corona;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class FakeSessionNotifier extends SessionNotifier {

  public static final Log LOG = LogFactory.getLog(FakeSessionNotifier.class);
  public FakeSessionNotifier(SessionManager sessionManager,
      ClusterManager clusterManager, ClusterManagerMetrics metrics) {
    super(null, null, metrics);
  }

  public void notifyGrantResource(String handle, List<ResourceGrant> granted) {
    reportGrantMetrics(granted);
    LOG.info("notifyGrantResource handle:" + handle +
        " granted:" + granted.size());
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
}
