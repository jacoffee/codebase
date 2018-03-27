package com.jacoffee.codebase.message.rabbitmq;

import com.google.common.base.Stopwatch;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitMQProducer extends AbstractChannel {

  public RabbitMQProducer(Map<String, String> configs) throws IOException, TimeoutException {
    ConnectionFactory factory = init(configs);
    this.connection = factory.newConnection();
  }

  // single thread operation
  public void batchSend(String queueName, Iterator<byte[]> payloadIter) throws IOException, InterruptedException, TimeoutException {
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

}
