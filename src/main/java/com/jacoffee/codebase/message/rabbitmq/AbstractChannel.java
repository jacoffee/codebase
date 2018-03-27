package com.jacoffee.codebase.message.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Map;

public abstract class AbstractChannel implements AutoCloseable {

  public Connection connection;

  protected ConnectionFactory init(Map<String, String> configs) {
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
