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
import java.io.IOException;
import java.util.Arrays;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 *
 */
public class TestUtilizationCollector extends TestCase {
  
  /**
   * A {@link LinuxUtilizationGauger} fed with fake /proc/ and "ps -eo"
   * information
   */
  static class FakeLinuxUtilizationGauger extends LinuxUtilizationGauger {
    @Override
    protected String[] getPS() {
      // There are two tasktrackers on this machine
      String[] result =
      {"  PID  PPID %CPU   RSS COMMAND",
       "    1     0  0.0   500 init [3]",
       "    2     1  0.0     0 [migration/0]",
       "    6     1  0.0     0 [migration/2]",
       "    7     1  0.0     0 [ksoftirqd/2]",
       "    8     1  0.0     0 [migration/3]",
       "    9     1  0.0     0 [ksoftirqd/3]",
       " 3016     1  0.0  1844 /usr/sbin/automount",
       " 3023     1  0.0   688 /usr/sbin/automount",
       "10089     1  4.8 319332 java jar org.apache.hadoop.mapred.TaskTracker",
       "11508 10089 16.3 883184 mapred/local/taskTracker/jobcache/job_200909200053_23092/task_200909200053_23092_r_000315_0/work ",
       "13983     1  0.0  7520 ntpd",
       "15735     1  6.9 673432 /usr/local/jdk-6u14-64/bin/java -Xmx1024m -server",
       "17036  3298  0.0  2332 pickup",
       "17841     1  0.0  2192 /usr/local/x",
       "18176 10089  1.4 176012 mapred/local/taskTracker/jobcache/job_200909200053_23202/task_200909200053_23202_r_000015_0/work",
       "18234 18176  0.0   372  -m 1572864",
       "18235 18234  0.0  1328 /bin/sh",
       "18268 18235  0.1 25648 java IndexReduce /nsidx 1",
       "18426  3114  0.0  5280 sshd",
       "18437 18426  0.0 13292 rsync",
       "19951 10089 18.4 617096 mapred/local/taskTracker/jobcache/job_200909200053_23193/task_200909200053_23193_r_000074_0/work",
       "20381 10089  3.1 193252 mapred/local/taskTracker/jobcache/job_200909200053_23209/task_200909200053_23209_r_000006_0/work",
       "20437 20381  1.6 11912 python",
       "20894     1  0.0   776 syslogd -m 0",
       "20906     1  0.0   404 klogd -x",
       "23818     1  1.6  8012 python",
       "24659     1  0.0  7748 python",
       "25183     1  1.1 38912 python",
       "25246 10089 94.7 469268 mapred/local/taskTracker/jobcache/job_200909200053_23295/task_200909200053_23295_m_000125_0/work",
       "25351 25246 80.6 12924 python",
       "26398  3424  0.0  3452 /var/cf",
       "26735 10089  120 908892 mapred/local/taskTracker/jobcache/job_200909200053_23250/task_200909200053_23250_m_002299_0/work",
       "26888 10089 49.8 373348 mapred/local/taskTracker/jobcache/job_200909200053_23275/task_200909200053_23275_r_000007_0/work",
       "26931 26888  0.0   376  -m 1572864 /usr/local/bin/python",
       "26932 26931  0.0  3944 /usr/local/bin/python",
       "27323 27319  0.0  2456 -bash",
       "27503 10089  101 450616 mapred/local/taskTracker/jobcache/job_200909200053_23303/task_200909200053_23303_m_000737_0/work",
       "27590 10089  110 277508 mapred/local/taskTracker/jobcache/job_200909200053_23304/task_200909200053_23304_m_000000_0/work",
       "27680     1  0.0   492 /sbin/a",
       "27698  2672  0.0   564 sleep 10",
       "10011     1  4.8 319332 hadoop.mapred.TaskTracker",
       "19955 10011 18.4 617096 mapred/local/taskTracker/jobcache/job_200909200053_23111/task_200909200053_23193_r_000074_0/work",
       "27748 27323  0.0   816 ps -eo pid,ppid,pcpu,rss,command"};
      return result;
    }

    final static String[] memInfoContent = {
       "MemTotal:     64973540 kB",
       "MemFree:       1947664 kB",
       "Buffers:      15146812 kB",
       "Cached:       13968244 kB",
       "SwapCached:     130200 kB",
       "Active:       23125288 kB",
       "Inactive:     16161212 kB",
       "SwapTotal:     2000084 kB",
       "SwapFree:      1737340 kB",
       "Dirty:            2996 kB",
       "Writeback:           0 kB",
       "AnonPages:    10040632 kB",
       "Mapped:        5130148 kB",
       "Slab:         23409512 kB",
       "SReclaimable: 22975476 kB",
       "SUnreclaim:     434036 kB",
       "PageTables:     245404 kB",
       "NFS_Unstable:        0 kB",
       "Bounce:              0 kB",
       "CommitLimit:  34486852 kB",
       "Committed_AS: 30538284 kB",
       "VmallocTotal: 34359738367 kB",
       "VmallocUsed:      1632 kB",
       "VmallocChunk: 34359736703 kB",
       "HugePages_Total:     0",
       "HugePages_Free:      0",
       "HugePages_Rsvd:      0",
       "Hugepagesize:     2048 kB",
    };

    final static String[] cpuInfoContent = {
        "processor	: 0",
        "vendor_id	: GenuineIntel",
        "cpu family	: 6",
        "model		: 23",
        "model name	: Intel(R) Xeon(R) CPU           L5420  @ 2.50GHz",
        "stepping	: 6",
        "cpu MHz		: 2500.000",
        "cache size	: 6144 KB",
        "physical id	: 0",
        "siblings	: 4",
        "core id		: 0",
        "cpu cores	: 4",
        "fpu		: yes",
        "fpu_exception	: yes",
        "cpuid level	: 10",
        "wp		: yes",
        "flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm syscall lm constant_tsc pni monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr dca lahf_lm",
        "bogomips	: 5003.50",
        "clflush size	: 64",
        "cache_alignment	: 64",
        "address sizes	: 38 bits physical, 48 bits virtual",
        "power management:",
        "",
        "processor	: 1",
        "vendor_id	: GenuineIntel",
        "cpu family	: 6",
        "model		: 23",
        "model name	: Intel(R) Xeon(R) CPU           L5420  @ 2.50GHz",
        "stepping	: 6",
        "cpu MHz		: 2500.000",
        "cache size	: 6144 KB",
        "physical id	: 1",
        "siblings	: 4",
        "core id		: 0",
        "cpu cores	: 4",
        "fpu		: yes",
        "fpu_exception	: yes",
        "cpuid level	: 10",
        "wp		: yes",
        "flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm syscall lm constant_tsc pni monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr dca lahf_lm",
        "bogomips	: 5000.13",
        "clflush size	: 64",
        "cache_alignment	: 64",
        "address sizes	: 38 bits physical, 48 bits virtual",
        "power management:",
        "",
        "processor	: 2",
        "vendor_id	: GenuineIntel",
        "cpu family	: 6",
        "model		: 23",
        "model name	: Intel(R) Xeon(R) CPU           L5420  @ 2.50GHz",
        "stepping	: 6",
        "cpu MHz		: 2500.000",
        "cache size	: 6144 KB",
        "physical id	: 0",
        "siblings	: 4",
        "core id		: 2",
        "cpu cores	: 4",
        "fpu		: yes",
        "fpu_exception	: yes",
        "cpuid level	: 10",
        "wp		: yes",
        "flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm syscall lm constant_tsc pni monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr dca lahf_lm",
        "bogomips	: 5000.13",
        "clflush size	: 64",
        "cache_alignment	: 64",
        "address sizes	: 38 bits physical, 48 bits virtual",
        "power management:",
        "",
        "processor	: 3",
        "vendor_id	: GenuineIntel",
        "cpu family	: 6",
        "model		: 23",
        "model name	: Intel(R) Xeon(R) CPU           L5420  @ 2.50GHz",
        "stepping	: 6",
        "cpu MHz		: 2500.000",
        "cache size	: 6144 KB",
        "physical id	: 1",
        "siblings	: 4",
        "core id		: 2",
        "cpu cores	: 4",
        "fpu		: yes",
        "fpu_exception	: yes",
        "cpuid level	: 10",
        "wp		: yes",
        "flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm syscall lm constant_tsc pni monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr dca lahf_lm",
        "bogomips	: 5000.06",
        "clflush size	: 64",
        "cache_alignment	: 64",
        "address sizes	: 38 bits physical, 48 bits virtual",
        "power management:",
        "",
        "processor	: 4",
        "vendor_id	: GenuineIntel",
        "cpu family	: 6",
        "model		: 23",
        "model name	: Intel(R) Xeon(R) CPU           L5420  @ 2.50GHz",
        "stepping	: 6",
        "cpu MHz		: 2500.000",
        "cache size	: 6144 KB",
        "physical id	: 0",
        "siblings	: 4",
        "core id		: 1",
        "cpu cores	: 4",
        "fpu		: yes",
        "fpu_exception	: yes",
        "cpuid level	: 10",
        "wp		: yes",
        "flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm syscall lm constant_tsc pni monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr dca lahf_lm",
        "bogomips	: 5000.00",
        "clflush size	: 64",
        "cache_alignment	: 64",
        "address sizes	: 38 bits physical, 48 bits virtual",
        "power management:",
        "",
        "processor	: 5",
        "vendor_id	: GenuineIntel",
        "cpu family	: 6",
        "model		: 23",
        "model name	: Intel(R) Xeon(R) CPU           L5420  @ 2.50GHz",
        "stepping	: 6",
        "cpu MHz		: 2500.000",
        "cache size	: 6144 KB",
        "physical id	: 1",
        "siblings	: 4",
        "core id		: 1",
        "cpu cores	: 4",
        "fpu		: yes",
        "fpu_exception	: yes",
        "cpuid level	: 10",
        "wp		: yes",
        "flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm syscall lm constant_tsc pni monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr dca lahf_lm",
        "bogomips	: 5000.06",
        "clflush size	: 64",
        "cache_alignment	: 64",
        "address sizes	: 38 bits physical, 48 bits virtual",
        "power management:",
        "",
        "processor	: 6",
        "vendor_id	: GenuineIntel",
        "cpu family	: 6",
        "model		: 23",
        "model name	: Intel(R) Xeon(R) CPU           L5420  @ 2.50GHz",
        "stepping	: 6",
        "cpu MHz		: 2500.000",
        "cache size	: 6144 KB",
        "physical id	: 0",
        "siblings	: 4",
        "core id		: 3",
        "cpu cores	: 4",
        "fpu		: yes",
        "fpu_exception	: yes",
        "cpuid level	: 10",
        "wp		: yes",
        "flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm syscall lm constant_tsc pni monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr dca lahf_lm",
        "bogomips	: 5000.06",
        "clflush size	: 64",
        "cache_alignment	: 64",
        "address sizes	: 38 bits physical, 48 bits virtual",
        "power management:",
        "",
        "processor	: 7",
        "vendor_id	: GenuineIntel",
        "cpu family	: 6",
        "model		: 23",
        "model name	: Intel(R) Xeon(R) CPU           L5420  @ 2.50GHz",
        "stepping	: 6",
        "cpu MHz		: 2500.000",
        "cache size	: 6144 KB",
        "physical id	: 1",
        "siblings	: 4",
        "core id		: 3",
        "cpu cores	: 4",
        "fpu		: yes",
        "fpu_exception	: yes",
        "cpuid level	: 10",
        "wp		: yes",
        "flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm syscall lm constant_tsc pni monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr dca lahf_lm",
        "bogomips	: 5000.00",
        "clflush size	: 64",
        "cache_alignment	: 64",
        "address sizes	: 38 bits physical, 48 bits virtual",
        "power management:",
    };

    @Override
    public void initialGauge() {
      try {
        parseMemInfo(memInfoContent);
        parseCpuInfo(cpuInfoContent);
        getTaskTrackerUtilization().setHostName("localhost");
      } catch (IOException e) {
        System.err.println("Failed to initialized LinuxUtilizationGauger");
      }
    }
  }

  /**
   * Test the basic funcitons of LinuxUtilizationGauger including parsing
   * /proc/ directory and parsing "ps -eo" results
   */
  public void testLinuxUtilizationGauger() {
    FakeLinuxUtilizationGauger fakeLinuxUtilizationGauger =
            new FakeLinuxUtilizationGauger();
    fakeLinuxUtilizationGauger.initialGauge();
    fakeLinuxUtilizationGauger.gauge();
    assertEquals(fakeLinuxUtilizationGauger.toString(),
            String.format(TaskTrackerUtilization.contentFormat, "localhost",
            8, 20.00, 64.97, 79.33, 9.90) +
            String.format(LocalJobUtilization.contentFormat,
                          "TaskTracker", 0.24, 0.64) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23092", 0.41, 0.88) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23111", 0.46, 0.62) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23193", 0.46, 0.62) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23202", 0.04, 0.20) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23209", 0.12, 0.21) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23250", 3.00, 0.91) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23275", 1.25, 0.38) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23295", 4.38, 0.48) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23303", 2.53, 0.45) +
            String.format(LocalJobUtilization.contentFormat,
                           "job_200909200053_23304", 2.75, 0.28));
  }
  
  private static final String JOB_ID1 = "job_200909200053_0001";
  private static final String JOB_ID2 = "job_200909200053_0002";
  private static final String JOB_ID3 = "job_200909200053_0003";
  private static final String JOB_ID4 = "job_200909200053_0004";
  private static final String JOB_ID5 = "job_200909200053_0005";

  /**
   * A {@link UtilizationCollector} without any Daemon running
   */
  static class FakeCollector extends UtilizationCollector {
    public FakeCollector() {
      // this will create a Collector without RPC and aggregate Daemon
      super(); 
    }
    @Override
    public void aggregateReports() {
      super.aggregateReports();
    }

    public void setTimeLimit(Long timeLimit) {
      this.timeLimit = timeLimit;
    }

    public void setAggregatePeriod(Long aggregateSleepTime) {
      this.aggregatePeriod = aggregateSleepTime;
    }

    public void setStopTimeLimit(Long stopTimeLimit) {
      this.stopTimeLimit = stopTimeLimit;
    }

  }

  /**
   * Create a collector without RPC server. Test if the aggregation computation
   * is functioning correctly
   * @throws Exception
   */
  public void testCollectorLocally() throws Exception {
    FakeCollector collector = new FakeCollector();
    collector.setTimeLimit(5000L);
    collector.setAggregatePeriod(1000L);
    collector.setStopTimeLimit(1500L);

    TaskTrackerUtilization ttUtil = new TaskTrackerUtilization();
    ttUtil.setHostName("host1");
    ttUtil.setCpuTotalGHz(20);
    ttUtil.setCpuUsageGHz(4);
    ttUtil.setMemTotalGB(30);
    ttUtil.setMemUsageGB(20);
    ttUtil.setNumCpu(10);

    LocalJobUtilization[] localJobUtil = new LocalJobUtilization[2];
    for (int i = 0; i < localJobUtil.length; i++) {
      localJobUtil[i] = new LocalJobUtilization();
    }
    localJobUtil[0].setJobId(JOB_ID1);
    localJobUtil[0].setCpuUsageGHz(2);
    localJobUtil[0].setMemUsageGB(10);
    localJobUtil[1].setJobId(JOB_ID5);
    localJobUtil[1].setCpuUsageGHz(2);
    localJobUtil[1].setMemUsageGB(10);

    collector.reportTaskTrackerUtilization(ttUtil, localJobUtil);
    collector.aggregateReports();

    assertEquals(collector.getClusterUtilization().toString(),
            "Nodes: 1, #Jobs: 2, #CPU: 10, CPU GHz: 20.00\n" +
            "Mem GB: 30.00, %CPU: 20.00, %Mem: 66.67\n");
    assertEquals(collector.getTaskTrackerUtilization("host1").toString(),
            String.format(TaskTrackerUtilization.contentFormat, "host1",
            10, 20.00, 30.00, 20.00, 66.67));
    assertEquals(
         collector.getJobUtilization(JOB_ID1).toString(),
         String.format(JobUtilization.contentFormat,
         JOB_ID1, 10.00, 33.33, 10.00, 33.33, 33.33, 0.10, 0.33, 0.00, 1.00));

    ttUtil = new TaskTrackerUtilization();
    ttUtil.setHostName("host1");
    ttUtil.setCpuTotalGHz(20);
    ttUtil.setCpuUsageGHz(10);
    ttUtil.setMemTotalGB(30);
    ttUtil.setMemUsageGB(20);
    ttUtil.setNumCpu(10);

    localJobUtil = new LocalJobUtilization[3];
    for (int i = 0; i < localJobUtil.length; i++) {
      localJobUtil[i] = new LocalJobUtilization();
    }
    localJobUtil[0].setJobId(JOB_ID1);
    localJobUtil[0].setCpuUsageGHz(4);
    localJobUtil[0].setMemUsageGB(5);
    localJobUtil[1].setJobId(JOB_ID2);
    localJobUtil[1].setCpuUsageGHz(6);
    localJobUtil[1].setMemUsageGB(5);
    localJobUtil[2].setJobId(JOB_ID3);
    localJobUtil[2].setCpuUsageGHz(10);
    localJobUtil[2].setMemUsageGB(10);

    collector.reportTaskTrackerUtilization(ttUtil, localJobUtil);
    collector.aggregateReports();

    assertEquals(collector.getClusterUtilization().toString(),
            "Nodes: 1, #Jobs: 3, #CPU: 10, CPU GHz: 20.00\n" +
            "Mem GB: 30.00, %CPU: 50.00, %Mem: 66.67\n");
    assertEquals(collector.getTaskTrackerUtilization("host1").toString(),
            String.format(TaskTrackerUtilization.contentFormat,
                  "host1", 10, 20.00, 30.00, 50.00, 66.67));
    assertEquals(
         collector.getJobUtilization(JOB_ID1).toString(),
            String.format(JobUtilization.contentFormat,
            JOB_ID1, 20.00, 16.67, 20.00, 16.67, 33.33, 0.30, 0.50, 0.00, 2.00));
    assertEquals(
         collector.getJobUtilization(JOB_ID2).toString(),
            String.format(JobUtilization.contentFormat,
            JOB_ID2, 30.00, 16.67, 30.00, 16.67, 16.67, 0.30, 0.17, 0.00, 1.00));
    assertEquals(
         collector.getJobUtilization(JOB_ID3).toString(),
            String.format(JobUtilization.contentFormat,
            JOB_ID3, 50.00, 33.33, 50.00, 33.33, 33.33, 0.50, 0.33, 0.00, 1.00));
    assertEquals(
         collector.getJobUtilization(JOB_ID5).toString(),
            String.format(JobUtilization.contentFormat,
         JOB_ID5, 0.00, 0.00, 0.00, 0.00, 33.33, 0.10, 0.33, 1.00, 1.00));
    assertFalse(collector.getJobUtilization(JOB_ID5).getIsRunning());

    ttUtil = new TaskTrackerUtilization();
    ttUtil.setHostName("host2");
    ttUtil.setCpuTotalGHz(40);
    ttUtil.setCpuUsageGHz(20);
    ttUtil.setMemTotalGB(50);
    ttUtil.setMemUsageGB(30);
    ttUtil.setNumCpu(10);

    localJobUtil = new LocalJobUtilization[3];
    for (int i = 0; i < localJobUtil.length; i++) {
      localJobUtil[i] = new LocalJobUtilization();
    }
    localJobUtil[0].setJobId(JOB_ID1);
    localJobUtil[0].setCpuUsageGHz(10);
    localJobUtil[0].setMemUsageGB(20);
    localJobUtil[2].setJobId(JOB_ID4);
    localJobUtil[2].setCpuUsageGHz(10);
    localJobUtil[2].setMemUsageGB(10);

    collector.reportTaskTrackerUtilization(ttUtil, localJobUtil);
    collector.aggregateReports();

    assertEquals(collector.getClusterUtilization().toString(),
            "Nodes: 2, #Jobs: 4, #CPU: 20, CPU GHz: 60.00\n" +
            "Mem GB: 80.00, %CPU: 50.00, %Mem: 62.50\n");
    assertEquals(collector.getTaskTrackerUtilization("host1").toString(),
            String.format(TaskTrackerUtilization.contentFormat,
                  "host1", 10, 20.00, 30.00, 50.00, 66.67));
    assertEquals(collector.getTaskTrackerUtilization("host2").toString(),
            String.format(TaskTrackerUtilization.contentFormat,
                  "host2", 10, 40.00, 50.00, 50.00, 60.00));
    assertEquals(
         collector.getJobUtilization(JOB_ID1).toString(),
            String.format(JobUtilization.contentFormat,
         JOB_ID1, 23.33, 31.25, 25.00, 40.00, 40.00, 0.53, 0.81, 0.00, 3.00));
    assertEquals(
         collector.getJobUtilization(JOB_ID2).toString(),
            String.format(JobUtilization.contentFormat,
         JOB_ID2, 10.00, 6.25, 30.00, 16.67, 16.67, 0.40, 0.23, 0.00, 2.00));
    assertEquals(
         collector.getJobUtilization(JOB_ID3).toString(),
            String.format(JobUtilization.contentFormat,
         JOB_ID3, 16.67, 12.50, 50.00, 33.33, 33.33, 0.67, 0.46, 0.00, 2.00));
    assertEquals(
         collector.getJobUtilization(JOB_ID4).toString(),
            String.format(JobUtilization.contentFormat,
         JOB_ID4, 16.67, 12.50, 25.00, 20.00, 20.00, 0.17, 0.13, 0.00, 1.00));
    assertEquals(collector.getJobUtilization(JOB_ID5), null);
    assertEquals(collector.getAllTaskTrackerUtilization().length, 2);
    String[] taskTrackerUtilStrings =
            new String[collector.getAllTaskTrackerUtilization().length];
    for (int i = 0; i < taskTrackerUtilStrings.length; i++) {
        taskTrackerUtilStrings[i] =  collector.getAllTaskTrackerUtilization()[i].toString();
    }
    Arrays.sort(taskTrackerUtilStrings);
    assertEquals(taskTrackerUtilStrings[0],
            String.format(TaskTrackerUtilization.contentFormat,
                  "host1", 10, 20.00, 30.00, 50.00, 66.67));
    assertEquals(taskTrackerUtilStrings[1],
            String.format(TaskTrackerUtilization.contentFormat,
                  "host2", 10, 40.00, 50.00, 50.00, 60.00));

    assertEquals(collector.getAllRunningJobUtilization().length, 4);
    String[] jobUtilStrings =
            new String[collector.getAllRunningJobUtilization().length];
    for (int i = 0; i < jobUtilStrings.length; i++) {
        jobUtilStrings[i] =  collector.getAllRunningJobUtilization()[i].toString();
    }
    Arrays.sort(jobUtilStrings);
    assertEquals(jobUtilStrings[0],
            String.format(JobUtilization.contentFormat,
        JOB_ID1, 23.33, 31.25, 25.00, 40.00, 40.00, 0.53, 0.81, 0.00, 3.00));
    assertEquals(jobUtilStrings[1],
            String.format(JobUtilization.contentFormat,
        JOB_ID2, 10.00, 6.25, 30.00, 16.67, 16.67, 0.40, 0.23, 0.00, 2.00));
    assertEquals(jobUtilStrings[2],
            String.format(JobUtilization.contentFormat,
        JOB_ID3, 16.67, 12.50, 50.00, 33.33, 33.33, 0.67, 0.46, 0.00, 2.00));
    assertEquals(jobUtilStrings[3],
            String.format(JobUtilization.contentFormat,
        JOB_ID4, 16.67, 12.50, 25.00, 20.00, 20.00, 0.17, 0.13, 0.00, 1.00));
  }

  /**
   * Create a {@link UtilizationCollector} and the RPC clients. Test if the RPC Daemon and
   * the aggregation Daemon running correctly
   * @throws Exception
   */
  public void testCollectorRPC() throws Exception {
    Configuration conf = new Configuration();
    // How often do we aggregate the repoorts
    conf.setLong("mapred.resourceutilization.aggregateperiod", 100L);
    conf.setLong("mapred.resourceutilization.mirrorperiod", 100L);
    conf.set("mapred.resourceutilization.sever.address", "localhost:30011");
    // Start the RPC server
    UtilizationCollector collector = new UtilizationCollector(conf);
    Thread.sleep(1000L);
    UtilizationCollectorProtocol rpcReporter =
          (UtilizationCollectorProtocol) RPC.getProxy(UtilizationCollectorProtocol.class,
                                           UtilizationCollectorProtocol.versionID,
                                           UtilizationCollector.getAddress(conf),
                                           conf);
    UtilizationCollectorProtocol rpcClient =
          (UtilizationCollectorProtocol) RPC.getProxy(UtilizationCollectorProtocol.class,
                                           UtilizationCollectorProtocol.versionID,
                                           UtilizationCollector.getAddress(conf),
                                           conf);

    UtilizationCollectorCached collectorMirrored = UtilizationCollectorCached.getInstance(conf);

    TaskTrackerUtilization ttUtil = new TaskTrackerUtilization();
    ttUtil.setHostName("host1");
    ttUtil.setCpuTotalGHz(20);
    ttUtil.setCpuUsageGHz(4);
    ttUtil.setMemTotalGB(30);
    ttUtil.setMemUsageGB(20);
    ttUtil.setNumCpu(10);

    LocalJobUtilization[] localJobUtil = new LocalJobUtilization[2];
    for (int i = 0; i < localJobUtil.length; i++) {
      localJobUtil[i] = new LocalJobUtilization();
    }
    localJobUtil[0].setJobId(JOB_ID1);
    localJobUtil[0].setCpuUsageGHz(2);
    localJobUtil[0].setMemUsageGB(10);
    localJobUtil[1].setJobId(JOB_ID5);
    localJobUtil[1].setCpuUsageGHz(2);
    localJobUtil[1].setMemUsageGB(10);

    rpcReporter.reportTaskTrackerUtilization(ttUtil, localJobUtil);
    Thread.sleep(1000L);


    assertEquals(rpcClient.getClusterUtilization().toString(),
                 "Nodes: 1, #Jobs: 2, #CPU: 10, CPU GHz: 20.00\n" +
                 "Mem GB: 30.00, %CPU: 20.00, %Mem: 66.67\n");
    assertEquals(rpcClient.getTaskTrackerUtilization("host1").toString(),
            String.format(TaskTrackerUtilization.contentFormat,
                  "host1", 10, 20.00, 30.00, 20.00, 66.67));
    assertEquals(rpcClient.getAllRunningJobUtilization().length, 2);
    assertEquals(rpcClient.getAllTaskTrackerUtilization().length, 1);

    assertEquals(collectorMirrored.getClusterUtilization().toString(),
                 "Nodes: 1, #Jobs: 2, #CPU: 10, CPU GHz: 20.00\n" +
                 "Mem GB: 30.00, %CPU: 20.00, %Mem: 66.67\n");
    assertEquals(collectorMirrored.getTaskTrackerUtilization("host1").toString(),
            String.format(TaskTrackerUtilization.contentFormat,
                          "host1", 10, 20.00, 30.00, 20.00, 66.67));
    assertEquals(collectorMirrored.getAllRunningJobUtilization().length, 2);
    assertEquals(collectorMirrored.getAllTaskTrackerUtilization().length, 1);

    collectorMirrored.terminate();
    RPC.stopProxy(rpcReporter);
    RPC.stopProxy(rpcClient);
  }
}
