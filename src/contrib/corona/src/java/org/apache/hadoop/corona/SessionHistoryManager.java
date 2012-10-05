package org.apache.hadoop.corona;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class SessionHistoryManager implements Configurable {

  SimpleDateFormat format = new SimpleDateFormat("yyyyMMddhhmm");
  private volatile long rollSeqNum = 0;
  CoronaConf conf = null;
  int sessionsInCurrent = 0;
  int maxSessionsPerDir = 0;
  long historyRollPeriod = 0;
  long lastRollTime = 0;

  String fullCurrentHistory = "";
  String historyRoot = null;

  @Override
  public void setConf(Configuration conf) {
    this.conf = new CoronaConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public void initialize() throws IOException {
    historyRoot = conf.getSessionsLogDir();
    maxSessionsPerDir = conf.getMaxSessionsPerDir();

    // Roll history every hour by default
    historyRollPeriod = conf.getLogDirRotationInterval();
    rollHistory();
  }

  private void rollHistory() {
    String currentTStamp = format.format(new Date());
    fullCurrentHistory = (new Path(historyRoot,
        currentTStamp + "_" + (++rollSeqNum))).toString();
    lastRollTime = System.currentTimeMillis();
    sessionsInCurrent = 0;
  }

  public synchronized String getCurrentLogPath() {
    if (++sessionsInCurrent > maxSessionsPerDir
        || lastRollTime + historyRollPeriod < System.currentTimeMillis()) {
      rollHistory();
    }

    return fullCurrentHistory;
  }
}
