package com.jacoffee.codebase.concurrency.producerconsumer;

public class ProducerConsumerTest {

  public static void main(String[] args) {
    SharedResources resources = new SharedResources();
    Thread producer = new Thread(new Producer(resources));
    Thread consumer = new Thread(new Consumer(resources));

    producer.start();
    consumer.start();
  }

}
