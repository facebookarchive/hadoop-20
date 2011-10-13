package org.apache.hadoop.corona;

import java.util.*;

/**
 * A bunch of immutable information about a retired session
 */
public class RetiredSession {

  public final String sessionId;
  public final SessionInfo info;
  public final long startTime;
  public final SessionStatus status;

  public static class Context {
    public final int         maxConcurrentRequestCount;
    public final int         fulfilledRequestCount;
    public final int         revokedRequestCount;

    public Context() {
      this.maxConcurrentRequestCount = 0;
      this.fulfilledRequestCount = 0;
      this.revokedRequestCount = 0;
    }

    public Context(int maxConcurrentRequestCount,
                   int fulfilledRequestCount,
                   int revokedRequestCount) {

      this.maxConcurrentRequestCount = maxConcurrentRequestCount;
      this.fulfilledRequestCount = fulfilledRequestCount;
      this.revokedRequestCount = revokedRequestCount;
    }
  }

  public final HashMap<String, Context> typeToContext;

  protected Context getContext(String type) {
    Context c = typeToContext.get(type);
    if (c == null) {
      c = new Context();
      typeToContext.put(type, c);
    }
    return c;
  }

  public RetiredSession(Session session) {
    sessionId = session.sessionId;
    info = session.getInfo();
    startTime = session.getStartTime();
    status = session.status;

    Set<String> types = session.getTypes();

    typeToContext = new HashMap<String, Context> (types.size());
    for (String type: types) {
      typeToContext.put
        (type, new Context(session.getMaxConcurrentRequestCountForType(type),
                           session.getFulfilledRequestCountForType(type),
                           session.getRevokedRequestCountForType(type)));
    }
  }

  public String getName() {
    return info.name;
  }

  public String getUserId() {
    return info.userId;
  }

  public String getUrl() {
    return info.url;
  }

  public int getMaxConcurrentRequestCountForType(String type) {
    return getContext(type).maxConcurrentRequestCount;
  }

  public int getFulfilledRequestCountForType(String type) {
    return getContext(type).fulfilledRequestCount;
  }

  public int getRevokedRequestCountForType(String type) {
    return getContext(type).revokedRequestCount;
  }
}