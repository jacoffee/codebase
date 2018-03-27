package com.jacoffee.codebase.concurrency.hashmapmodification;

import java.util.Map;
import java.util.Set;

public class MultiThreadAccess implements Runnable {

  private Map<String, Integer> map;

  public MultiThreadAccess(Map<String, Integer> map) {
    this.map = map;
  }

  @Override
  public void run() {
    Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
    synchronized (map) {
      for (Map.Entry<String, Integer> entry : entrySet) {
        System.out.println(String.format("%s : %d", entry.getKey(), entry.getValue()));
      }
    }
  }

}
