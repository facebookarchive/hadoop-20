package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class CoronaFailureEvent {
  public static final Log LOG =
      LogFactory.getLog(CoronaFailureEvent.class);
  @Override
  public String toString() {
    return when.toString() + ":" + how.toString();
  }

  public CoronaFailureEventWhen when;
  public CoronaFailureEventHow how;
  
  public CoronaFailureEvent() {}
  
  public CoronaFailureEvent(CoronaFailureEventWhen when, CoronaFailureEventHow how) {
    this.when = when;
    this.how = how;
  }
  
  public static CoronaFailureEvent fromString(String event) {
    try {
      String [] failure = event.split(":");
      if (failure.length == 2) {
        CoronaFailureEventWhen when = CoronaFailureEventWhen.findByValue(Integer.parseInt(failure[0]));
        CoronaFailureEventHow how = CoronaFailureEventHow.findByValue(Integer.parseInt(failure[1]));
        if (when == null || how == null) {
          return null;
        }
        CoronaFailureEvent failureEvent = new CoronaFailureEvent (when, how);
        LOG.info("add a failure event" + event);
        return failureEvent;
      }
    } catch (NumberFormatException e) {
       
    }
    
    return null;
  }
}
