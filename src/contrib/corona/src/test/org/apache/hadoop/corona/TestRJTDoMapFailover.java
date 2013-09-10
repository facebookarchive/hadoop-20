package org.apache.hadoop.corona;


public class TestRJTDoMapFailover extends TestRJTFailover { 
  public void test10RJTFailover1() throws Exception
  {
    String[] failureEvents = {"1:0"};
    doTestRJTFailover("test10RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
   
  public void test11RJTFailover1() throws Exception
  {
    String[] failureEvents = {"1:1"};
    doTestRJTFailover("test11RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
 
  public void test12RJTFailover1() throws Exception
  {
    String[] failureEvents = {"1:2"};
    doTestRJTFailover("test12RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
 
  public void test13RJTFailover1() throws Exception
  {
    String[] failureEvents = {"1:3"};
    doTestRJTFailover("test13RJTFailover1", failureEvents, 1, 1, 1, 1, 1);
  }
  
}
