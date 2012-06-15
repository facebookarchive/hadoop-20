package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;
import org.apache.hadoop.util.HostsFileReader;

public class ClusterManagerTestable extends ClusterManager {

  FakeConfigManager configManager = new FakeConfigManager();

  public static class NodeManagerTestable extends NodeManager {
    public NodeManagerTestable(
        ClusterManager clusterManager, CoronaConf conf) throws IOException {
      super(clusterManager,
        new HostsFileReader(conf.getHostsFile(), conf.getExcludesFile()));
    }
  }

  public static class SchedulerTestable extends Scheduler {
    public SchedulerTestable(NodeManager nodeManager,
        SessionManager sessionManager, SessionNotifier sessionNotifier,
        Collection<ResourceType> types, ClusterManagerMetrics metrics,
        ConfigManager configManager, CoronaConf conf) {
      super(nodeManager, sessionManager, sessionNotifier, types, metrics, conf,
        configManager);
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
    this(conf, false);
  }
  public ClusterManagerTestable(CoronaConf conf, boolean callbackSession) throws IOException {
    this.conf = conf;
    initLegalTypes();

    ContextFactory.resetFactory();
    setNoEmitMetricsContext();
    metrics = new ClusterManagerMetrics(getTypes());
    sessionManager = new SessionManagerTestable(this);
    nodeManager = new NodeManagerTestable(this, conf);
    if (callbackSession) {
      sessionNotifier = new CallbackSessionNotifier(sessionManager, this, metrics);
    } else {
      sessionNotifier = new FakeSessionNotifier(sessionManager, this, metrics);
    }

    sessionHistoryManager = new SessionHistoryManager();
    sessionHistoryManager.setConf(conf);

    configManager = new FakeConfigManager();
    scheduler = new SchedulerTestable(nodeManager, sessionManager,
        sessionNotifier, getTypes(), metrics, configManager, conf);
    scheduler.setConf(conf);
    scheduler.start();
    sessionManager.setConf(conf);
    nodeManager.setConf(conf);

    startTime = clock.getTime();
  }

  @Override
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
