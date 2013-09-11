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
package org.apache.hadoop.conf;

import org.json.JSONObject;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestJsonConfiguration {

  private JSONObject createFinalJsonObject() throws Exception {
    // Construct json object for core_site
    JSONObject core_site = new JSONObject();
    core_site.put("io_file_buffer_size", 65536);
    core_site.put("hadoop_disable_shell", true);
    core_site.put("fs_hdfs_impl", "org.apache.hadoop.hdfs.DistributedAvatarFileSystem");

    // Construct json object for hdfs_site
    JSONObject hdfs_site = new JSONObject();
    hdfs_site.put("fs_ha_retrywrites", true);
    hdfs_site.put("dfs_replication", 3);
    hdfs_site.put("dfs_replication_max", 512);
    hdfs_site.put("dfs_replication_min", 1);

    // Construct the final json object consisting for core-site and hdfs-site
    JSONObject json = new JSONObject();
    json.put("core-site", core_site);
    json.put("hdfs-site", hdfs_site);
    return json;
  }

  private void verifyConfigurations(Configuration conf) throws Exception {
    // Verify all the configurations are correct.
    assertEquals(7, conf.size());
    assertEquals(65536, conf.getInt("io.file.buffer.size", -1));
    assertEquals(true, conf.getBoolean("hadoop.disable.shell", false));
    assertEquals("org.apache.hadoop.hdfs.DistributedAvatarFileSystem",
        conf.get("fs.hdfs.impl"));
    assertEquals(true, conf.getBoolean("fs.ha.retrywrites", false));
    assertEquals(3, conf.getInt("dfs.replication", -1));
    assertEquals(512, conf.getInt("dfs.replication.max", -1));
    assertEquals(1, conf.getInt("dfs.replication.min", -1));
  }

  @Test
  public void testJsonObject() throws Exception {
    verifyConfigurations(new Configuration(createFinalJsonObject()));
  }
}
