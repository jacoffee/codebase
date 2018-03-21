package com.jacoffee.codebase.concurrency.producerconsumer;

public class Producer implements Runnable {

  private SharedResources sharedResources;

  public Producer(SharedResources sharedResources) {
    this.sharedResources = sharedResources;
  }

  @Override
  public void run() {
    for (int i = 0; i < 100; i++) {
      sharedResources.put();
    }
  }

}
