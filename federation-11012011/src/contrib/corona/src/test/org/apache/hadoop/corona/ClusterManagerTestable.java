package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;

public class ClusterManagerTestable extends ClusterManager {

  FakeConfigManager configManager = new FakeConfigManager();

  public static class NodeManagerTestable extends NodeManager {
    public NodeManagerTestable(ClusterManager clusterManager) {
      super(clusterManager);
    }
  }

  public static class SchedulerTestable extends Scheduler {
    public SchedulerTestable(NodeManager nodeManager,
        SessionManager sessionManager, SessionNotifier sessionNotifier,
        Collection<String> types, ConfigManager configManager) {
      super(nodeManager, sessionManager, sessionNotifier, types, configManager);
    }
  }

  public static class SessionManagerTestable extends SessionManager {
    public SessionManagerTestable(ClusterManager clusterManager) {
      super(clusterManager);
    }
  }

  public ClusterManagerTestable(Configuration conf) throws IOException {
    this(new CoronaConf(conf));
  }

  public ClusterManagerTestable(CoronaConf conf) throws IOException {
    this.conf = conf;
    initLegalTypes();

    ContextFactory.resetFactory();
    setNoEmitMetricsContext();
    metrics = new ClusterManagerMetrics(getTypes());
    sessionManager = new SessionManagerTestable(this);
    nodeManager = new NodeManagerTestable(this);
    sessionNotifier = new FakeSessionNotifier(sessionManager, this, metrics);

    configManager = new FakeConfigManager();
    scheduler = new SchedulerTestable(nodeManager, sessionManager,
        sessionNotifier, getTypes(), configManager);
    scheduler.start();
    sessionManager.setConf(conf);
    nodeManager.setConf(conf);
    scheduler.setConf(conf);

    startTime = clock.getTime();
  }

  public SessionManagerTestable getSessionManager() {
    return (SessionManagerTestable)sessionManager;
  }

  public FakeConfigManager getConfigManager() {
    return this.configManager;
  }

  private void setNoEmitMetricsContext() throws IOException {
    ContextFactory factory = ContextFactory.getFactory();
    factory.setAttribute(ClusterManagerMetrics.CONTEXT_NAME + ".class",
        NoEmitMetricsContext.class.getName());
  }

}
