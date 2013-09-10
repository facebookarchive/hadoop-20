package org.apache.hadoop.corona;


public class TestRJTDoReduceFetchFailover extends TestRJTFailover {
  public void test20RJTFailover1() throws Exception
  {
    String[] failureEvents = {"2:0"};
    doTestRJTFailover("test20RJTFailover1", failureEvents, 1, 1, 1, 200, 1);
  }
  
  public void test21RJTFailover1() throws Exception
  {
    String[] failureEvents = {"2:1"};
    doTestRJTFailover("test21RJTFailover1", failureEvents, 1, 1, 1, 200, 1);
  }
  
  public void test22RJTFailover1() throws Exception
  {
    String[] failureEvents = {"2:2"};
    doTestRJTFailover("test22RJTFailover1", failureEvents, 1, 1, 1, 200, 1);
  }
 
  public void test23RJTFailover1() throws Exception
  {
    String[] failureEvents = {"2:3"};
    doTestRJTFailover("test23RJTFailover1", failureEvents, 1, 1, 1, 200, 1);
  }
  
}
