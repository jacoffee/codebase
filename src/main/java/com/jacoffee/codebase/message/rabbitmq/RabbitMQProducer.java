package com.jacoffee.codebase.message.rabbitmq;

import com.google.common.base.Stopwatch;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitMQProducer {

  private Connection connection;

  public RabbitMQProducer(Map<String, String> configs) throws IOException, TimeoutException {
    ConnectionFactory factory = init(configs);
    this.connection = factory.newConnection();
  }

  private ConnectionFactory init(Map<String, String> configs) {
    ConnectionFactory factory = new ConnectionFactory();


    if (configs.get("username") != null) {
      factory.setUsername(configs.get("username"));
    }

    if (configs.get("password") != null) {
      factory.setPassword(configs.get("password"));
    }

    if (configs.get("host") != null) {
      factory.setHost(configs.get("host"));
    }

    if (configs.get("port") != null) {
      factory.setPort(Integer.valueOf(configs.get("port")));
    }

    if (configs.get("virtualHost") != null) {
      factory.setVirtualHost(configs.get("virtualHost"));
    }

    return factory;
  }

  // single thread operation
  public void batchSend(String queueName, Iterator<byte[]> payloadIter) throws IOException, InterruptedException {
    Channel channel = connection.createChannel();
    channel.confirmSelect();

    int count = 0;
    Stopwatch stopwatch = new Stopwatch().start();
    while (payloadIter.hasNext()) {
      byte[] payload = payloadIter.next();
      channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, payload);
      count = count + 1;
    }


    channel.waitForConfirmsOrDie();

    stopwatch.stop();
    String msg = String.format("Send %s msg and confirmed in %s ms", count, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    System.out.println(msg);
  }

  // single thread operation
  public void send(String queueName, byte[] payload) throws IOException, InterruptedException {
    Channel channel = connection.createChannel();
    channel.confirmSelect();
    channel.basicPublish("", queueName, null, payload);
    channel.waitForConfirmsOrDie();
  }

  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception e) {
        // just ignore
      }
    }
  }

}
