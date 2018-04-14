package com.jacoffee.codebase.concurrency.lock;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
   + case to show simple cas can not guarantee that the object or field has not been modified by others
    such as the coupon can only be sent once

    AtomicStampedReference comes to rescue: use stamp to make sure only once
*/
public class AtomicLoopHole {

  private static ExecutorService pool = Executors.newFixedThreadPool(10);

  public static void main(String[] args) {
    AtomicReference<Integer> account = new AtomicReference<>();
    account.set(19);
    charge(account);
    consume(account);
    pool.shutdown();
  }


  // charge use account whose balance is lower than 20
  private static void charge(AtomicReference<Integer> account) {
    for (int i =  0; i < 10; i ++) {
      Runnable task = () -> {
         Integer money = account.get();
         if (money < 20) {
           if (account.compareAndSet(money, money + 20)) {
             System.out.println("余额小于20元, 充值成功, 余额: " + account.get());
           } else {
             System.out.println("余额大于20元, 不用充值");
           }
         }
      };

      pool.submit(task);
    }
  }

  private static void consume(AtomicReference<Integer> account) {
      Runnable task = () -> {
        // single user
        for (int i = 0; i < 100; i ++) {
           while (true) {
             Integer money = account.get();
             if (money > 10) {
               if (account.compareAndSet(money, money - 10)) {
                 System.out.println("消费10元, 余额 " + account.get());
                 break;
               }
             } else {
               System.out.println("没有足够金额");
               break;
             }

             try {
               Thread.sleep(200);
             } catch (InterruptedException e) {
               e.printStackTrace();
             }
           }
        }
      };

      pool.submit(task);
    }

}
