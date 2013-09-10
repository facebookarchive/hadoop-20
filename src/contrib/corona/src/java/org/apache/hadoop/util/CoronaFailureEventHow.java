package org.apache.hadoop.util;

public enum CoronaFailureEventHow {
  KILL_SELF(0),
  FAILED_PING_PARENT_JT(1),
  FAILED_PING_CM(2),
  FAILED_PINGED_BY_PARENT_JT(3);
  
  private int val;
  private CoronaFailureEventHow (int i) {
    val = i;
  }
  
  public int getValue() {
    return val;
  }
  
  @Override
  public String toString() {
    return Integer.toString(val);
  }
  
  public static CoronaFailureEventHow findByValue(int value) {
    switch(value) {
      case 0:
        return KILL_SELF;
      case 1:
        return FAILED_PING_PARENT_JT;
      case 2:
        return FAILED_PING_CM;
      case 3:
        return FAILED_PINGED_BY_PARENT_JT;
      default:
        return null;
    }
  }
}
