package com.jacoffee.codebase.jmx;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
   probe bean info via jxm service url
*/
public class MBeanViaJmxService {

  /*
      JMX_PORT=9393 ./bin/kafka-server-start.sh -daemon config/server.properties
  */
  public static void main(String[] args) throws Exception {
    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9393/jmxrmi");
    JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
    MBeanServerConnection connection = jmxc.getMBeanServerConnection();

    /*
      kafka.coordinator
      kafka.utils
      kafka.controller
      kafka.network
      kafka.log
      kafka.server
      kafka.cluster
      kafka
    */
    System.out.println("---------------");
    System.out.println("Registered domain");
    for (String domain : connection.getDomains()) {
      System.out.println(domain);
    }
    System.out.println();

    System.out.println("---------------");
    System.out.println("MBean count");
    System.out.println(connection.getMBeanCount());
    System.out.println();

    System.out.println("---------------");
    System.out.println("Get attribute of MBean");
    String underReplicatedTopicPartition =
        "kafka.cluster:type=Partition,name=UnderReplicated,topic=message,partition=0";
    Object underReplicatedTopicPartitionCount =
      connection.getAttribute(new ObjectName(underReplicatedTopicPartition), "Value");
    System.out.println(underReplicatedTopicPartitionCount);
  }

}
