package org.apache.hadoop.corona;


public class TestRJTStartFailover extends TestRJTFailover{
  public void test00RJTFailover1() throws Exception
  {
    String[] failureEvents = {"0:0"};
    doTestRJTFailover("test00RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
  
  public void test01RJTFailover1() throws Exception
  {
    String[] failureEvents = {"0:1"};
    doTestRJTFailover("test01RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
  
  public void test02RJTFailover1() throws Exception
  {
    String[] failureEvents = {"0:2"};
    doTestRJTFailover("test02RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
  
  public void test03RJTFailover1() throws Exception
  {
    String[] failureEvents = {"0:3"};
    doTestRJTFailover("test03RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
  
}
