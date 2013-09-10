package org.apache.hadoop.corona;


public class TestRJTDoReduceFetchFail extends TestRJTFailover {
  public void test20RJTFail() throws Exception
  {
    String[] failureEvents = {"2:0"};
    doTestRJTFailover("test20RJTFail", failureEvents, 1, 1, 1, 200, 0);
  }
  
  public void test21RJTFail() throws Exception
  {
    String[] failureEvents = {"2:1"};
    doTestRJTFailover("test21RJTFail", failureEvents, 1, 1, 1, 200, 0);
  }
  
  public void test22RJTFail() throws Exception
  {
    String[] failureEvents = {"2:2"};
    doTestRJTFailover("test22RJTFail", failureEvents, 1, 1, 1, 200, 0);
  }
  
  public void test23RJTFail() throws Exception
  {
    String[] failureEvents = {"2:3"};
    doTestRJTFailover("test23RJTFail", failureEvents, 1, 1, 1, 200, 0);
  }
}
