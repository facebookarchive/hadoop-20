package org.apache.hadoop.corona;

public enum LocalityLevel {
  NODE (0),
  RACK (1),
  ANY  (2);

  private final int levelNumber;
  private LocalityLevel(int levelNumber) {
    this.levelNumber = levelNumber;
  }

  public boolean isBetterThan(LocalityLevel other) {
    return this.levelNumber < other.levelNumber;
  }
}
