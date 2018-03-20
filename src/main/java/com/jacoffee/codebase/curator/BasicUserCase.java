package com.jacoffee.codebase.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/*
   basic user case for curator
*/
public class BasicUserCase {

  private static CuratorFramework client = null;

  public static void init() {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
    client.start();

    Runtime.getRuntime().addShutdownHook(
      new Thread(() -> {
        System.out.println("Closing curator");
        CloseableUtils.closeQuietly(client);
      })
    );
  }

  static {
    init();
  }

  public static void getOp(String path) throws Exception {
    byte[] offsetBytes = client.getData().forPath(path);
    long offset = Long.valueOf(new String(offsetBytes, "UTF-8"));
    System.out.println(offset);
  }

  public static void getChildrenOp(String path) throws Exception {
    // return only the children path, not including parent
    // /xxx/yyy/1,2,3 will return 1,2,3
    List<String> childPath =
        client.getChildren().forPath(path);
    System.out.println(childPath);
  }

  public static void createOp(String path) throws Exception {
    String createdPath = client.create().creatingParentsIfNeeded().forPath(path);
    System.out.println(createdPath);
  }

  public static void updateOp(String path, byte[] data) throws Exception {
    Stat updateStat =
        client.setData().forPath("/consumers/user-method-invocation-group/offsets/kafka-word-count/4", data);
  }


  public static void createPerOp(String path) throws Exception {
    client.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT).forPath(path);
  }

  public static void main(String[] args) throws Exception {
    createPerOp("/_locknode_/xxxx");
  }

}
