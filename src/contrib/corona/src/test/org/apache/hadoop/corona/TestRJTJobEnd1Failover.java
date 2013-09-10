package org.apache.hadoop.corona;


public class TestRJTJobEnd1Failover extends TestRJTFailover {
  public void test30RJTFailover1() throws Exception
  {
    String[] failureEvents = {"3:0"};
    doTestRJTFailover("test30RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
 
  public void test31RJTFailover1() throws Exception
  {
    String[] failureEvents = {"3:1"};
    doTestRJTFailover("test31RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
  
  public void test32RJTFailover1() throws Exception
  {
    String[] failureEvents = {"3:2"};
    doTestRJTFailover("test32RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
  
  public void test33RJTFailover1() throws Exception
  {
    String[] failureEvents = {"3:3"};
    doTestRJTFailover("test33RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
}
