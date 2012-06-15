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

import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.MiniCoronaCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class TestMiniCoronaTaskFail extends TestCase {
  private static String taskLog = "Task attempt log";
  private static String cleanupLog = "cleanup attempt log";

  final static Log LOG = LogFactory.getLog(TestMiniCoronaTaskFail.class);

  public static class MapperClass extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
    String taskid;
    public void configure(JobConf job) {
      taskid = job.get("mapred.task.id");
    }
    public void map (LongWritable key, Text value,
                     OutputCollector<Text, IntWritable> output,
                     Reporter reporter) throws IOException {
      System.err.println(taskLog);
      if (taskid.endsWith("_0")) {
        throw new IOException();
      } else if (taskid.endsWith("_1")) {
        System.exit(-1);
      } else if (taskid.endsWith("_2")) {
        throw new Error();
      }
    }
  }

  static class CommitterWithLogs extends FileOutputCommitter {
    public void abortTask(TaskAttemptContext context) throws IOException {
      System.err.println(cleanupLog);
      super.abortTask(context);
    }
  }

  static class CommitterWithFailTaskCleanup extends FileOutputCommitter {
    public void abortTask(TaskAttemptContext context) throws IOException {
      System.err.println(cleanupLog);
      System.exit(-1);
    }
  }

  static class CommitterWithFailTaskCleanup2 extends FileOutputCommitter {
    public void abortTask(TaskAttemptContext context) throws IOException {
      System.err.println(cleanupLog);
      throw new IOException();
    }
  }

  public JobClient prepareJob(JobConf conf, Path inDir,
      Path outDir, String input)  throws IOException {
    // set up the input file system and write input text.
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      // write input into input file
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    // configure the mapred Job
    conf.setMapperClass(MapperClass.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setSpeculativeExecution(false);
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                    "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);
    // return the RunningJob handle.
    return new JobClient(conf);
  }

  private void validateAttempt(TaskInProgress tip, TaskAttemptID attemptId,
		  TaskStatus ts, boolean isCleanup, boolean containsCleanupLog)
  throws IOException {
    assertEquals(isCleanup, tip.isCleanupAttempt(attemptId));
    assertTrue(ts != null);
    assertEquals(TaskStatus.State.FAILED, ts.getRunState());
    // validate tasklogs for task attempt
    String log = TestMiniMRMapRedDebugScript.readTaskLog(
    TaskLog.LogName.STDERR, attemptId, false);
    assertTrue(log.contains(taskLog));
    if (containsCleanupLog) {
      // validate task logs: tasklog should contain both task logs
      // and cleanup logs when task failure is caused by throwing IOE
      assertTrue(log.contains(cleanupLog));
    }
    if (isCleanup) {
      // validate tasklogs for cleanup attempt
      log = TestMiniMRMapRedDebugScript.readTaskLog(
      TaskLog.LogName.STDERR, attemptId, true);
      assertTrue(log.contains(cleanupLog));
    }
  }

  private void validateJob(RunningJob job, CoronaJobTracker jt,
      boolean cleanupNeeded)  throws IOException {
    assertEquals(JobStatus.SUCCEEDED, job.getJobState());

    JobID jobId = job.getID();
    // construct the task id of first map task
    // this should not be cleanup attempt since the first attempt
    // fails with an exception
    TaskAttemptID attemptId =
      new TaskAttemptID(new TaskID(jobId, true, 0), 0);
    TaskInProgress tip = jt.getTip(attemptId.getTaskID());
    TaskStatus ts = tip.getTaskStatus(attemptId);
    validateAttempt(tip, attemptId, ts, false, true);

    attemptId =  new TaskAttemptID(new TaskID(jobId, true, 0), 1);
    // this should be cleanup attempt since the second attempt fails
    // with System.exit
    ts = tip.getTaskStatus(attemptId);
    validateAttempt(tip, attemptId, ts, cleanupNeeded, false);

    attemptId =  new TaskAttemptID(new TaskID(jobId, true, 0), 2);
    // this should be cleanup attempt since the third attempt fails
    // with Error
    ts = tip.getTaskStatus(attemptId);
    validateAttempt(tip, attemptId, ts, cleanupNeeded, false);
  }

  public void testWithDFS() throws IOException {
    MiniDFSCluster dfs = null;
    MiniCoronaCluster mr = null;
    CoronaJobTracker jt = null;
    FileSystem fileSys = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 4, true, null);
      fileSys = dfs.getFileSystem();
      String namenode = fileSys.getUri().toString();
      mr = new MiniCoronaCluster.Builder().
          numTaskTrackers(10).namenode(namenode).numDir(1).build();
      final Path inDir = new Path("./input");
      final Path outDir = new Path("./output");
      String input = "The quick brown fox\nhas many silly\nred fox sox\n";
      {
        LOG.info("launch job with fail tasks");
        // launch job with fail tasks
        JobConf jobConf = mr.createJobConf();
        jobConf.setOutputCommitter(CommitterWithLogs.class);
        JobClient jc = prepareJob(jobConf, inDir, outDir, input);
        RunningJob rJob = jc.submitJob(jobConf);
        jc.monitorAndPrintJob(jobConf, rJob);
        jt = (CoronaJobTracker) jc.jobSubmitClient;
        validateJob(rJob, jt, true);
        fileSys.delete(outDir, true);
      }
      {
        LOG.info("launch job with fail tasks and fail-cleanups with exit(-1)");
        // launch job with fail tasks and fail-cleanups with exit(-1)
        JobConf jobConf = mr.createJobConf();
        jobConf.setOutputCommitter(CommitterWithFailTaskCleanup.class);
        JobClient jc = prepareJob(jobConf, inDir, outDir, input);
        RunningJob rJob = jc.submitJob(jobConf);
        jt = (CoronaJobTracker) jc.jobSubmitClient;
        rJob.waitForCompletion();
        validateJob(rJob, jt, true);
        fileSys.delete(outDir, true);
      }
      {
        LOG.info("launch job with fail tasks and fail-cleanups with IOE");
        // launch job with fail tasks and fail-cleanups with IOE
        JobConf jobConf = mr.createJobConf();
        jobConf.setOutputCommitter(CommitterWithFailTaskCleanup2.class);
        JobClient jc = prepareJob(jobConf, inDir, outDir, input);
        RunningJob rJob = jc.submitJob(jobConf);
        jt = (CoronaJobTracker) jc.jobSubmitClient;
        rJob.waitForCompletion();
        validateJob(rJob, jt, true);
        fileSys.delete(outDir, true);
      }

      {
        LOG.info("launch job with fail tasks and turn off task-cleanup task");
        // launch job with fail tasks and turn off task-cleanup task
        JobConf jobConf = mr.createJobConf();
        jobConf.setOutputCommitter(CommitterWithLogs.class);
        jobConf.setTaskCleanupNeeded(false);
        JobClient jc = prepareJob(jobConf, inDir, outDir, input);
        RunningJob rJob = jc.submitJob(jobConf);
        jt = (CoronaJobTracker) jc.jobSubmitClient;
        rJob.waitForCompletion();
        validateJob(rJob, jt, false);
        fileSys.delete(outDir, true);
      }
      {
        LOG.info("launch job with all attempts failing");
        JobConf jobConf = mr.createJobConf();
        jobConf.setMaxMapAttempts(3);
        JobClient jc = prepareJob(jobConf, inDir, outDir, input);
        RunningJob rJob = jc.submitJob(jobConf);
        jt = (CoronaJobTracker) jc.jobSubmitClient;
        rJob.waitForCompletion();
        assertTrue(rJob.isComplete() && !rJob.isSuccessful());
        fileSys.delete(outDir, true);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  public static void main(String[] argv) throws Exception {
    TestMiniCoronaTaskFail td = new TestMiniCoronaTaskFail();
    td.testWithDFS();
  }
}
