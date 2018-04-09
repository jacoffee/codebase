package com.jacoffee.codebase.message.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQConsumerTest {

  private final static String QUEUE_NAME = "user_invocation_monitor";

  public static void pull(Map<String, String> configs) {
    RabbitMQConsumer consumer = null;
    try {
      consumer = new RabbitMQConsumer(configs);
      consumer.consume(QUEUE_NAME);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  public static void push(Map<String, String> configs) throws IOException, TimeoutException {
    RabbitMQConsumer consumer = new RabbitMQConsumer(configs);
    consumer.batchConsume(QUEUE_NAME, "user_invocation_consumer");
  }

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    Map<String, String> configs = new HashMap<>();
    configs.put("username", "xxxxx");
    configs.put("password", "xxxxx");
    configs.put("host", "xxxxx");
    configs.put("port", "xxxxx");
    configs.put("virtualHost", "xxxxx");

    push(configs);
  }

}
