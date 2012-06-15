package org.apache.hadoop.corona;

import junit.framework.Assert;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.spi.OutputRecord;

class ClusterManagerMetricsVerifier {
  final int requestedMaps;
  final int requestedReduces;
  final int releasedMaps;
  final int releasedReduces;
  final int grantedMaps;
  final int grantedReduces;
  final int revokedMaps;
  final int revokedReduces;
  final ClusterManager cm;

  ClusterManagerMetricsVerifier(ClusterManager cm,
      int requestedMaps, int requestedReduces,
      int grantedMaps, int grantedReduces,
      int releasedMaps, int releasedReduces,
      int revokedMaps, int revokedReduces) {
    this.cm = cm;
    this.requestedMaps = requestedMaps;
    this.requestedReduces = requestedReduces;
    this.releasedMaps = releasedMaps;
    this.releasedReduces = releasedReduces;
    this.grantedMaps = grantedMaps;
    this.grantedReduces = grantedReduces;
    this.revokedMaps = revokedMaps;
    this.revokedReduces = revokedReduces;
  }

  void verifyAll() throws Exception {
    verifyMetrics("requested_m", requestedMaps);
    verifyMetrics("requested_r", requestedReduces);
    verifyMetrics("granted_m", grantedMaps);
    verifyMetrics("granted_r", grantedReduces);
    verifyMetrics("released_m", releasedMaps);
    verifyMetrics("released_r", releasedReduces);
    verifyMetrics("revoked_m", revokedMaps);
    verifyMetrics("revoked_r", revokedReduces);
  }

  private void verifyMetrics(String name, int expectValue) throws Exception {
    MetricsContext context = MetricsUtil.getContext(
        ClusterManagerMetrics.CONTEXT_NAME);
    cm.metrics.doUpdates(context);
    OutputRecord record = context.getAllRecords().get(
        ClusterManagerMetrics.CONTEXT_NAME).iterator().next();
    Assert.assertEquals(expectValue, record.getMetric(name).intValue());
  }
}
