package org.apache.hadoop.corona;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class SessionHistoryManager implements Configurable {

  CoronaConf conf = null;
  int maxSessionsPerDir = 0;
  String historyRoot = null;

  @Override
  public void setConf(Configuration conf) {
    this.conf = new CoronaConf(conf);
    initialize();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private void initialize() {
    historyRoot = conf.getSessionsLogDir();
    maxSessionsPerDir = conf.getMaxSessionsPerDir();
  }

  public synchronized String getLogPath(String sessionId) {
    int dotIdx = sessionId.indexOf('.');
    String ts = sessionId.substring(0, dotIdx);
    String sessionNum = sessionId.substring(dotIdx + 1);
    String dirName =
      ts + "." + (Integer.parseInt(sessionNum) / maxSessionsPerDir);
    return new Path(historyRoot, dirName).toString();
  }
}
