package com.jacoffee.codebase.message.rabbitmq;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQProducerTest {

  private final static String QUEUE_NAME = "user_invocation_monitor";

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

    Map<String, String> configs = new HashMap<>();
    configs.put("username", "guest");
    configs.put("password", "guest");
    configs.put("host", "localhost");
    configs.put("port", "5672");

    RabbitMQProducer producer = new RabbitMQProducer(configs);


    producer.batchSend(
        QUEUE_NAME,
        Lists.newArrayList("1 Never Lost", "2 Never Lost").stream().map(str -> str.getBytes()).iterator()
    );
    producer.close();
  }

}
