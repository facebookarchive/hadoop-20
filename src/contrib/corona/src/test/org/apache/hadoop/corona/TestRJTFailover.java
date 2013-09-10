package org.apache.hadoop.corona;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapred.CoronaJobTracker;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.CoronaFailureEvent;
import org.apache.hadoop.util.CoronaFailureEventInjector;
import org.apache.hadoop.util.ToolRunner;

import junit.framework.TestCase;

public class TestRJTFailover extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestRJTFailover.class);
  private MiniCoronaCluster corona = null;
  private CoronaFailureEventInjector rjtFailureInjector = new CoronaFailureEventInjector();
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    
    String[] racks = "/rack-1,/rack-1,/rack-2,/rack-3".split(",");
    String[] trackers = "tracker-1,tracker-2,tracker-3,tracker-4".split(",");
    
    corona = new MiniCoronaCluster.Builder().
      numTaskTrackers(4).
      racks(racks).
      hosts(trackers).rjtFailureInjector(rjtFailureInjector).
      build();
  }

  /**
   * Naming rule for the test case:
   * test+(FailureEventString)+RJT+Fail(means the job will fail)
   * test+(FailureEventString)+RJT+Failover+(number to do failover)
   * 
   * This class is the parent class for all the RJT failover class
   * Because the 10 minutes time limit for test case, just devide the RJT 
   * test case based on when to do failover 
   */
  
  public void testDummy() {
    LOG.info("Starting testDummy");
  }
  
  @Override
  protected void tearDown() {
    if (corona != null) {
      corona.shutdown();
    }
  }

  protected void runSleepJob(JobConf conf, int maps, int reduces, int mt, int rt)
      throws Exception {
    String[] args = {"-m", maps + "",
                     "-r", reduces + "",
                     "-mt", mt + "",
                     "-rt", rt + "" };
    ToolRunner.run(conf, new SleepJob(), args);
    // This sleep is here to wait for the JobTracker to go down completely
    TstUtils.reliableSleep(1000);
  }
  
  protected void doTestRJTFailover(
      String testName, String [] failureEvents, 
      int maps, int reduces,
      int mt, int rt,
      int failNum) throws Exception {
    LOG.info("Starting the test for " + testName);
    
    for (String fe:failureEvents) {
      CoronaFailureEvent event = CoronaFailureEvent.fromString(fe);
      if (event != null) {
        rjtFailureInjector.injectFailureEvent(event);
      }
    }
    
    JobConf conf = corona.createJobConf();
    conf.setBoolean("mapred.coronajobtracker.forceremote", true);
    conf.setInt(CoronaJobTracker.MAX_JT_FAILURES_CONF, failNum);
    
    long start = System.currentTimeMillis();
    try {
      this.runSleepJob(conf, maps, reduces, mt, rt);
      //assertTrue("RJT failover is in a wrong state", failNum > 0);
    } catch (Exception e) {
      LOG.info("job failed.", e);
      assertTrue("RJT failed to do failover", failNum == 0);
    }
    long end = System.currentTimeMillis();
    
    LOG.info("Time spent for :" + testName + 
        (end - start));
  }
  
}
