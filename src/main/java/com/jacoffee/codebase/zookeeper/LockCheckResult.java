package com.jacoffee.codebase.zookeeper;

public class LockCheckResult {

  private boolean locked;
  private String pathToWatch;

  public LockCheckResult(boolean holdTheLock, String pathToWatch) {
    this.locked = holdTheLock;
    this.pathToWatch = pathToWatch;
  }

  public boolean isLocked() {
    return locked;
  }

  public String getPathToWatch() {
    return pathToWatch;
  }

}
