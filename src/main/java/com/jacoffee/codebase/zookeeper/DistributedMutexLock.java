package com.jacoffee.codebase.zookeeper;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.common.PathUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
  + Inter process lock bases on zookeeper to help me understand the distributed lock recipe mentioned in
    https://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_outOfTheBox
*/
public class DistributedMutexLock {

  private final String PATH_SEPERATOR = "/";
  private final String lockName = "lock-";

  private ZooKeeper zooKeeper;
  private String basePath;
  private String nameForTest;

  // Thread -> Path || actually only one in this version
  private final ConcurrentMap<Thread, String> threadData = Maps.newConcurrentMap();

  public DistributedMutexLock(ZooKeeper zooKeeper, String basePath, String nameForTest) {
    this.zooKeeper = zooKeeper;
    this.basePath = basePath;
    this.nameForTest = nameForTest;
  }

  private Watcher watcher = (WatchedEvent event) -> notifyFromWatcher();

  // This method should only be called by a thread that is the owner of this object's monitor
  private void notifyFromWatcher() {
      synchronized (this) {
        System.out.println(" Notify thread " + Thread.currentThread().getName());
        notifyAll();
      }
  }

  private String createsTheLock(String path) throws Exception {
    return createsTheLock(path, null);
  }

  private void createPersistentParent(String path) {
    try {
      zooKeeper.create(path, new byte[0], OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException e) {
      if (e instanceof KeeperException.NoNodeException) {
        String parentPath = path.substring(0, path.lastIndexOf(PATH_SEPERATOR));

        createPersistentParent(parentPath);
        createPersistentParent(path);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private String createParentIfNeeded(String path, byte[] lockNodeBytes) throws Exception {
   PathUtils.validatePath(path);

   int lastPosition = path.lastIndexOf(PATH_SEPERATOR);
   if (lastPosition != 0) {
     String parentPath = path.substring(0, lastPosition);
     createPersistentParent(parentPath);
   }

    return zooKeeper.create(
        path,
        lockNodeBytes != null ? lockNodeBytes : new byte[0],
        OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL
    );
  }

  private String createsTheLock(String path, byte[] lockNodeBytes) throws Exception {
    return createParentIfNeeded(path, lockNodeBytes);
  }

  public String getIndexFromPath(String path) {
    int position = path.lastIndexOf(lockName);

    if (position < 0) {
      throw new IllegalArgumentException("Child base path should contains " + lockName);
    }

    String index = path.substring(position + lockName.length());
    return index;
  }

  // basePath -> /_locknode_; currentPath /_locknode_/lock-00000001
  public LockCheckResult getLockCheckResult(List<String> children, String currentPath)
    throws Exception {

    List<String> childrenIndex = Lists.newArrayList();
    for (String childPath : children) {
      childrenIndex.add(getIndexFromPath(childPath));
    }
    Collections.sort(childrenIndex);

    String currentIndex = getIndexFromPath(currentPath);

    int currentPositionInChildren = childrenIndex.indexOf(currentIndex);
    // lowest or -1
    boolean holdTheLock = currentPositionInChildren < 1;

    // next lowest
    String pathToWatch =
      holdTheLock ? null : (basePath + "/" + lockName + childrenIndex.get(currentPositionInChildren - 1));

    return new LockCheckResult(holdTheLock, pathToWatch);
  }

  public boolean tryLock() throws Exception {
      Thread currentThread = Thread.currentThread();

      String lockedPath = attemptLock();

      if (lockedPath != null) {
        threadData.put(currentThread, lockedPath);
        return true;
      }

      return false;
  }

  // eg basePath /_locknode_
  public String attemptLock() throws Exception {
    boolean hasTheLock = false;
    boolean doDeletePath = false;

    String path = String.format("%s/%s", basePath, lockName);
    String ourPath = null;

    try {
      // /_locknode_/lock-0000000001
      ourPath = createsTheLock(path);

      // try to get the lock until we can not
      while (!hasTheLock) {
        List<String> children = zooKeeper.getChildren(basePath, false);
        LockCheckResult lockCheckResult = getLockCheckResult(children, ourPath);

        if (lockCheckResult.isLocked()) {
          System.out.println(
              String.format(
                  "Thread %s in %s get lock on path",
                  Thread.currentThread().getName(), nameForTest, ourPath
              )
          );
          return ourPath;
        } else {
          String nextPathToWatch = lockCheckResult.getPathToWatch();
          System.out.println(" nextPathToWatch " + nextPathToWatch);

          // remember we have to get the object monitor before call wait()
          synchronized (this) {
            try {
              zooKeeper.getData(nextPathToWatch, watcher, null);
              String msg =
                String.format(
                    "Thread %s in %s enters lock-waiting pool",
                    Thread.currentThread().getName(), nameForTest
                );
              System.out.println(msg);
              wait();
            } catch (KeeperException.NoNodeException e) {
              // If the nextPathToWatch is gone, try next round of lock
            }
          }
        }
      }
      return ourPath;
    } catch (Exception e) {
      doDeletePath = true;
      throw e;
    } finally {
      if (doDeletePath) {
        deletePath(ourPath);
      }
    }
  }

  public void releaseLock() {
    Thread currentThread = Thread.currentThread();
    String lockedPath = threadData.get(currentThread);

    if (lockedPath == null) {
      throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
    }

    try {
      deletePath(lockedPath);
    } finally {
      // remove it anyway
      threadData.remove(currentThread);
    }
  }

  private void deletePath(String path) {
    try {
      System.out.println("Releasing lock on basePath " + path);
      zooKeeper.delete(path, -1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    }
  }

}
