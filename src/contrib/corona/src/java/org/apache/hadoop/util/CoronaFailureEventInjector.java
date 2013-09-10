package org.apache.hadoop.util;

import java.util.concurrent.ConcurrentLinkedQueue;

public class CoronaFailureEventInjector {
  private ConcurrentLinkedQueue<CoronaFailureEvent> failureEventQueue;
  
  public CoronaFailureEventInjector(){
    failureEventQueue = new ConcurrentLinkedQueue<CoronaFailureEvent> ();
  }
  
  public void injectFailureEvent(CoronaFailureEvent event) {
    failureEventQueue.offer(event);
  }
  
  public CoronaFailureEvent pollFailureEvent() {
    return failureEventQueue.poll();
  }
  
  public static CoronaFailureEventInjector getInjectorFromStrings (String [] events, int start) {
    if (start >= events.length) {
      return null;
    }
    
    CoronaFailureEventInjector injector = new CoronaFailureEventInjector();
    for (int i = start; i < events.length; ++ i) {
      CoronaFailureEvent event = CoronaFailureEvent.fromString(events[i]);
      if (event == null) {
        if (i == start) {
          return null;
        } else {
          break;
        }
      }
      
      injector.injectFailureEvent(event);
    }
    
    return injector;
  }
}
