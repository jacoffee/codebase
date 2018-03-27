package com.jacoffee.codebase.concurrency.hashmapmodification;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

public class ConcurrentModificationExceptionTest {

  public static void main(String[] args) throws InterruptedException {

    /*
        the following three originalMap | synchronizedMap | ConcurrentHashMap, only the last one
        can avoid concurrent modification
    */
    Map<String, Integer> originalMap =  new HashMap<>();
    Map<String, Integer> map = Collections.synchronizedMap(originalMap);
    // Map<String, Integer> map = new ConcurrentHashMap<>();

    ThreadFactory factory = new ThreadFactoryBuilder().build();

    factory.newThread(new MultiThreadModification(map)).start();
    factory.newThread(new MultiThreadAccess(map)).start();
    factory.newThread(new MultiThreadModification(map)).start();
    factory.newThread(new MultiThreadAccess(map)).start();
    factory.newThread(new MultiThreadModification(map)).start();
    factory.newThread(new MultiThreadAccess(map)).start();

    Thread.sleep(10000);
  }

}
