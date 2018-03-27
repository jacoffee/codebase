package com.jacoffee.codebase.concurrency.hashmapmodification;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class MultiThreadModification implements Runnable {

  private Map<String, Integer> map;

  public MultiThreadModification(Map<String, Integer> map) {
    this.map = map;
  }

  private List<String> seed = Lists.newArrayList("A", "B", "C", "D", "E", "F", "G", "H");

  @Override
  public void run() {
    // 0 <= x < 8
    int index = new Random().nextInt(seed.size());
    map.put(seed.get(index), 1);
    try {
      System.out.println(Thread.currentThread().getName() + " is sleeping");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      System.out.println(Thread.currentThread().getName() + " is interrupted");
      Thread.currentThread().interrupt();
    }
  }

}
