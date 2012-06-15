package org.apache.hadoop.corona;

import java.util.List;

public interface SessionListener {
  public void notifyGrantResource(List<ResourceGrant> granted);
}
