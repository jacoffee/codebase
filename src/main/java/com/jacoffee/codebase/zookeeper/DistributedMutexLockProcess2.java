package com.jacoffee.codebase.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.atomic.AtomicBoolean;

public class DistributedMutexLockProcess2 {

  private static AtomicBoolean zkClosed = new AtomicBoolean(false);
  private static void registerShutdownHook(ZooKeeper zk) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Clean up in the shut down hook !!!");
      if (zkClosed.compareAndSet(false, true)) {
        if (zk != null) {
          try {
            zk.close();
          } catch (Exception e) {
            // just ignore
          }
        }
      }
    }));
  }

  public static void main(String[] args) throws Exception {
    ZooKeeper zk = null;
    DistributedMutexLock mutexLock = null;

    String basePath = "/_locknode_";
    String nameForTest = "DistributedMutexLockProcess2";
    try {
      zk = new ZooKeeper(
          "localhost:2181",
          60 * 1000,
          (WatchedEvent event) -> {}
      );
      mutexLock = new DistributedMutexLock(zk, basePath, nameForTest);
      if (mutexLock.tryLock()) {
        System.out.println(nameForTest + " gets the lock");
      }
      registerShutdownHook(zk);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (mutexLock != null) {
        mutexLock.releaseLock();
      }
      if (zk != null) {
        zk.close();
        zkClosed.compareAndSet(false, true);
      }
    }
  }

}
