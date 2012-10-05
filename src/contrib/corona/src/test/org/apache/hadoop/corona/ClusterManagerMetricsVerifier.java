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
    verifyMetrics("requested_map", requestedMaps);
    verifyMetrics("requested_reduce", requestedReduces);
    verifyMetrics("granted_map", grantedMaps);
    verifyMetrics("granted_reduce", grantedReduces);
    verifyMetrics("released_map", releasedMaps);
    verifyMetrics("released_reduce", releasedReduces);
    verifyMetrics("revoked_map", revokedMaps);
    verifyMetrics("revoked_reduce", revokedReduces);
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
