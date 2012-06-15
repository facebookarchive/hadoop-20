package org.apache.hadoop.mapred;

import java.io.IOException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

class TTLaunchTask {

  private static final int RETRIES = 10;
  private final ShellCommandExecutor exec;
  private int attempts;
  private String clusterName;

  public TTLaunchTask(ShellCommandExecutor exec, String clusterName) {
    super();
    this.attempts = 0;
    this.clusterName = clusterName;
    this.exec = exec;
  }

  public String getClusterName() {
    return clusterName;
  }

  public boolean hasAttempts() {
    return attempts < RETRIES;
  }

  public void execute() throws IOException {
    attempts++;
    exec.execute();
  }
}
