package com.jacoffee.codebase.concurrency.producerconsumer;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedResources {

    public final int MAX_SIZE = 100;
    private LinkedList<Integer> resources = new LinkedList<>();

    private AtomicInteger putCount = new AtomicInteger(0);
    private AtomicInteger getCount = new AtomicInteger(0);

    public synchronized void put() {
      // point to tell the producer that you should stop
      while (resources.size() == MAX_SIZE) {
        try {
          wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      resources.offer(1);
      System.out.println("put operation " + putCount.incrementAndGet());
      notifyAll();
    }

    public synchronized void get() {
      // point to tell the producer that you should stop
      while (resources.isEmpty()) {
        try {
          wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      resources.poll();
      System.out.println("get operation " + getCount.incrementAndGet());
      notifyAll();
    }

}
