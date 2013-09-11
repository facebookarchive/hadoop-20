/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.metrics;

import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestTimeVaryingMetricsMinMax {

  @Test
  public void testBasic() throws Exception {
    MetricsRegistry mr = new MetricsRegistry();
    MetricsTimeVaryingRate metric = new MetricsTimeVaryingRate("test", mr);
    metric.inc(1, 10);

    MetricsContext context = MetricsUtil.getContext("test");
    MetricsRecord metricsRecord = MetricsUtil.createRecord(context, "test");
    metric.pushMetric(metricsRecord);

    assertEquals(10, metric.getMaxTime());
  }
}
