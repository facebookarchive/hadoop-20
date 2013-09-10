package org.apache.hadoop.corona;


public class TestRJTStartFail extends TestRJTFailover {
  public void test00RJTFail() throws Exception
  {
    String[] failureEvents = {"0:0"};
    doTestRJTFailover("test00RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test01RJTFail() throws Exception
  {
    String[] failureEvents = {"0:1"};
    doTestRJTFailover("test01RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test02RJTFail() throws Exception
  {
    String[] failureEvents = {"0:2"};
    doTestRJTFailover("test02RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test03RJTFail() throws Exception
  {
    String[] failureEvents = {"0:3"};
    doTestRJTFailover("test03RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
}
