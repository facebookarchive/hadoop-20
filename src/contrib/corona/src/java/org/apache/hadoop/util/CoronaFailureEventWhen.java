package org.apache.hadoop.util;

public enum CoronaFailureEventWhen {
  // the starting phase of JT
  JT_START(0),
  // the mapping phase of JT
  JT_DO_MAP(1),
  // the reducing phase of JT
  JT_DO_REDUCE_FETCH(2),
  // the ending phase of JT but no job history file generated
  JT_END1(3),
  // the ending phase of JT and job history file has generated
  JT_END2(4);
  
  @Override
  public String toString() {
    return Integer.toString(val);
  }

  private final int val;
  private CoronaFailureEventWhen(int i) {
    val = i;
  }
  public int getValue() {
    return val;
  }
  
  
  public static CoronaFailureEventWhen findByValue(int value) {
    switch(value) {
      case 0:
        return JT_START;
      case 1:
        return JT_DO_MAP;
      case 2:
        return JT_DO_REDUCE_FETCH;
      case 3:
        return JT_END1;
      case 4:
        return JT_END2;
      default:
        return null;
    }
  }
}
