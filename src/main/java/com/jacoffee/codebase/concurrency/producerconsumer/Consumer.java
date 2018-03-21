package com.jacoffee.codebase.concurrency.producerconsumer;

public class Consumer implements Runnable {

  private SharedResources sharedResources;

  public Consumer(SharedResources sharedResources) {
    this.sharedResources = sharedResources;
  }

  @Override
  public void run() {
    for (int i = 0; i < 100; i++) {
      sharedResources.get();
    }
  }

}
