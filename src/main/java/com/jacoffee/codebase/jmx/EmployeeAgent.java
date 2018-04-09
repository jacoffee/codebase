package com.jacoffee.codebase.jmx;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class EmployeeAgent {

  public static void main(String[] args) throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Employee employee = new Employee("allen", 28);
    // // kafka.producer:type=ProducerSendThread,name=name
    ObjectName objectName = new ObjectName("com.jacoffee:type=employee,name=basicInfo");
    mBeanServer.registerMBean(employee, objectName);
    // get pid and jconsole pid ---> MBean
    LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES));
  }

}
