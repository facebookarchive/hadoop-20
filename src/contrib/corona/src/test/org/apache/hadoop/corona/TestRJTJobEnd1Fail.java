package org.apache.hadoop.corona;


public class TestRJTJobEnd1Fail extends TestRJTFailover {
  public void test30RJTFail() throws Exception
  {
    String[] failureEvents = {"3:0"};
    doTestRJTFailover("test30RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test31RJTFail() throws Exception
  {
    String[] failureEvents = {"3:1"};
    doTestRJTFailover("test31RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test32RJTFail() throws Exception
  {
    String[] failureEvents = {"3:2"};
    doTestRJTFailover("test32RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test33RJTFail() throws Exception
  {
    String[] failureEvents = {"3:3"};
    doTestRJTFailover("test33RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
}
