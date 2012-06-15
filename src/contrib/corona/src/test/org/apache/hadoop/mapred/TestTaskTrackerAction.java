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

package org.apache.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskTrackerAction.ActionType;

/**
 * A Unit-test to test job execution of Mini Corona Map-Reduce Cluster.
 */
public class TestTaskTrackerAction extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestTaskTrackerAction.class);
  
  public void testTaskTrackerActionWritable() throws Exception {
    String sessionId = "123";
    InetSocketAddress jobTrackerAddress = new InetSocketAddress("host", 0);
    CoronaSessionInfo info =
      new CoronaSessionInfo(sessionId, jobTrackerAddress);
    KillJobAction action = new KillJobAction(new JobID("0001", 0), info);
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bOut);
    action.write(out);
    out.close();
    LOG.info("Sending:" + bOut.toString());
    TaskTrackerAction recoveredAction =
        TaskTrackerAction.createAction(ActionType.KILL_JOB);
    DataInputStream in = new DataInputStream(
        new ByteArrayInputStream(bOut.toByteArray()));
    recoveredAction.readFields(in);
    CoronaSessionInfo recoveredInfo =
      (CoronaSessionInfo)(recoveredAction.getExtensible());
    Assert.assertEquals(recoveredInfo.getSessionHandle(),
                        info.getSessionHandle());
    Assert.assertEquals(recoveredInfo.getJobTrackerAddr(),
                        info.getJobTrackerAddr());
  }
  
}
