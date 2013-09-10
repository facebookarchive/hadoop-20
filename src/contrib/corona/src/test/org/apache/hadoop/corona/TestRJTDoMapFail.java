package org.apache.hadoop.corona;


public class TestRJTDoMapFail extends TestRJTFailover {
  public void test10RJTFail() throws Exception
  {
    String[] failureEvents = {"1:0"};
    doTestRJTFailover("test10JTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test11RJTFail() throws Exception
  {
    String[] failureEvents = {"1:1"};
    doTestRJTFailover("test11RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test12RJTFail() throws Exception
  {
    String[] failureEvents = {"1:2"};
    doTestRJTFailover("test12RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test13RJTFail() throws Exception
  {
    String[] failureEvents = {"1:3"};
    doTestRJTFailover("test13RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
}
