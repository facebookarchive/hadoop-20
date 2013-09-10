/*
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.ipc.RPC;

/**
 * Implements logic behind asking local JT for permission to commit tasks in
 * remote JT
 */
@SuppressWarnings("deprecation")
public class CoronaCommitPermission {
  /** Empty array */
  public static final CommitTaskAction[] EMPTY_COMMIT_ACTIONS =
      new CommitTaskAction[0];

  /** Asks for permission to commit */
  public static class CommitPermissionClient {
    /** RPC client to authority that gives permissions to commit */
    private InterCoronaJobTrackerProtocol serverClient;
    /** Attempt id of this job tracker */
    private TaskAttemptID attemptId;
    /** True if there was unrecoverable error in connecting to authority */
    private boolean faultyClient = false;

    /** Creates client that automatically allows committing any tasks */
    public CommitPermissionClient() {
    }

    /**
     * Creates client that asks provided authority for permissions to commit
     * @param attemptId attempt id of remote JT asking for permissions
     * @param serverAddress addres of authority
     * @param conf job configuration
     * @throws IOException
     */
    public CommitPermissionClient(TaskAttemptID attemptId,
        InetSocketAddress serverAddress, JobConf conf) throws IOException {
      this.attemptId = attemptId;
      this.serverClient = RPC.waitForProxy(InterCoronaJobTrackerProtocol.class,
          InterCoronaJobTrackerProtocol.versionID, serverAddress, conf,
          RemoteJTProxy.getRemotJTTimeout(conf));
    }

    /**
     * Given array of task attempts that caller wants to commit returns array of
     * task attempts matching tasks of provided attempts, that were last
     * committing
     * @param actions list of actions to commit
     * @return array with attempt id of proper tasks that was/are committing
     * @throws IOException
     */
    public TaskAttemptID[] getAndSetCommitting(List<CommitTaskAction> actions)
        throws IOException {
      if (faultyClient)
        throw new IOException("Authority client is faulty");
      TaskAttemptID[] commmitPermissions;
      if (serverClient == null) {
        // Allow any commits
        commmitPermissions = new TaskAttemptID[actions.size()];
        for (int i = 0; i < commmitPermissions.length; ++i)
          commmitPermissions[i] = null;
      } else {
        try {
          TaskAttemptID[] toCommit = new TaskAttemptID[actions.size()];
          for (int i = 0; i < toCommit.length; ++i)
            toCommit[i] = actions.get(i).getTaskID();
          // Get=and-set
          commmitPermissions = serverClient.getAndSetCommitting(attemptId,
              toCommit);
        } catch (IOException e) {
          faultyClient = true;
          throw e;
        }
      }
      return commmitPermissions;
    }

    /**
     * Closes authority server client
     */
    public void close() {
      RPC.stopProxy(serverClient);
      serverClient = null;
    }

  }

  /** Dispatcher commit permissions */
  public static class CommitPermissionServer {
    /** This is actual map that implements get and set */
    private ConcurrentHashMap<TaskID, TaskAttemptID> taskToAttempt =
        new ConcurrentHashMap<TaskID, TaskAttemptID>();

    /**
     * Given array of task attempts that caller wants to commit returns array of
     * task attempts matching tasks of provided attempts, that were last
     * committing
     * @param attemptId of JT asking for permission
     * @param toCommit attempts that we want to commit
     * @return array with attempt id of proper tasks that was/are committing
     */
    public TaskAttemptID[] getAndSetCommitting(TaskAttemptID[] toCommit) {
      TaskAttemptID[] wasCommitting = new TaskAttemptID[toCommit.length];
      for (int i = 0; i < toCommit.length; ++i) {
        TaskAttemptID attempt = toCommit[i];
        TaskID task = attempt.getTaskID();
        wasCommitting[i] = taskToAttempt.put(task, attempt);
      }
      return wasCommitting;
    }
  }

}
