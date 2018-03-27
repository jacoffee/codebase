package com.jacoffee.codebase.message.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

public class RabbitMQConsumer extends AbstractChannel {

  public RabbitMQConsumer(Map<String, String> configs) throws IOException, TimeoutException {
    ConnectionFactory factory = init(configs);
    this.connection = factory.newConnection();
  }

  /*
    + basicGet Retrieving Individual Messages ("Pull API")
    + basicConsume Server push
  */
  public String consume(String queueName) throws IOException, InterruptedException {
    String message = null;
    Channel channel = connection.createChannel();

    GetResponse response = channel.basicGet(queueName, false);
    if (response != null) {
      AMQP.BasicProperties props = response.getProps();
      byte[] payload = response.getBody();
      message = new String(payload);
      long deliveryTag = response.getEnvelope().getDeliveryTag();
      channel.basicAck(deliveryTag, false);
      return message;
    }

    return message;
  }

  /*
    + one consumer one consumer tag
    + delivery tag is kind of like offset in Kafka
  */
  public void batchConsume(String queueName, String consumerTag) throws IOException {
    Channel channel = connection.createChannel();
    channel.basicConsume(queueName, false, consumerTag, new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(
        String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body
      ) throws IOException {
        String routingKey = envelope.getRoutingKey();
        String contentType = properties.getContentType();
        long deliveryTag = envelope.getDeliveryTag();

        String message = new String(body);
        System.out.println(" Message received: " + message + " with delivery tag: " + deliveryTag);
        channel.basicAck(deliveryTag, false);
      }
    });
  }



}
