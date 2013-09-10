package org.apache.hadoop.corona;


public class TestRJTJobEnd2Fail extends TestRJTFailover {
  public void test40RJTFail() throws Exception
  {
    String[] failureEvents = {"4:0"};
    doTestRJTFailover("test40RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
  
  public void test41RJTFail() throws Exception
  {
    String[] failureEvents = {"4:1"};
    doTestRJTFailover("test41RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }

  //In JobEnd2, the resourceUpdater thread has been closed, so 
  // we need not to emulate the can't ping CM failure
  
  public void test43RJTFail() throws Exception
  {
    String[] failureEvents = {"4:3"};
    doTestRJTFailover("test43RJTFail", failureEvents, 1, 1, 1, 1, 0);
  }
}
