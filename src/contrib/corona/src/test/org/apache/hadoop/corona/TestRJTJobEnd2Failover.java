package org.apache.hadoop.corona;


public class TestRJTJobEnd2Failover extends TestRJTFailover {
  public void test40RJTFailover1() throws Exception
  {
    String[] failureEvents = {"4:0"};
    doTestRJTFailover("test40RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
 
  public void test41RJTFailover1() throws Exception
  {
    String[] failureEvents = {"4:1"};
    doTestRJTFailover("test41RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
 
  // In JobEnd2, the resourceUpdater thread has been closed, so 
  // we need not to emulate the can't ping CM failure
  
  public void test43RJTFailover1() throws Exception
  {
    String[] failureEvents = {"4:3"};
    doTestRJTFailover("test43RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
}
