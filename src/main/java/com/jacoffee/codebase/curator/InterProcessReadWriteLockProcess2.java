package com.jacoffee.codebase.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.CloseableUtils;

import java.util.Random;

public class InterProcessReadWriteLockProcess2 {

  public static void main(String[] args) {
    int count = 0;
    CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", new RetryOneTime(1));
    try {
      client.start();
      InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/_locknode_");

      while (true) {

        lock.writeLock().acquire();

        try {
          count ++;
          String msg =
              String.format(
                  "Times %d Thread %s in TestInterProcessReadWriteLock1 acquired the lock",
                  count,
                  Thread.currentThread().getName()
              );
          System.out.println(msg);
        } finally {
          System.out.println(
              String.format("Thread %s in TestInterProcessReadWriteLock1 releasing the lock", Thread.currentThread().getName())
          );
          lock.writeLock().release();
        }

        int randomSecond = new Random().nextInt(1000) + 1000;
        Thread.sleep(randomSecond);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      System.out.println("Closing curator");
      CloseableUtils.closeQuietly(client);
    }
  }

}
