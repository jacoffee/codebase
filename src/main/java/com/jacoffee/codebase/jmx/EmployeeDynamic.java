package com.jacoffee.codebase.jmx;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;
import java.util.ArrayList;
import java.util.List;

public class EmployeeDynamic implements DynamicMBean {

  // Java Bean
  private Employee employee;

  // Attributes
  private List<MBeanAttributeInfo> attributes = new ArrayList<>();

  // Constructors
  private List<MBeanConstructorInfo> constructors = new ArrayList<>();

  // Operators
  private List<MBeanOperationInfo> operations = new ArrayList<>();

  // Notifications
  private List<MBeanNotificationInfo> notifications = new ArrayList<>();

  private MBeanInfo mBeanInfo;

  public EmployeeDynamic(Employee employee) {
    this.employee = employee;
    try {
      init();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void init() throws Exception {
    // ---------- Constructor
    //String name | String description | MBeanParameterInfo[] signature
    constructors.add(
      new MBeanConstructorInfo(
        "Employee(String name, int age)构造器",
        this.employee.getClass().getConstructors()[0]
      )
    );


    // ---------- Attributes
    attributes.add(
      new MBeanAttributeInfo("name", "java.lang.String", "姓名", true, false, false)
    );
    attributes.add(
      new MBeanAttributeInfo("age", "int", "年龄", true, false, false)
    );

    // ---------- Operations
    operations.add(
      new MBeanOperationInfo("sayHello", this.employee.getClass().getMethod("sayHello", java.lang.String.class))
    );

    // ---------- MBeanInfo
    /*
      public MBeanInfo(
        String className,String description,
        MBeanAttributeInfo[] attributes,
        MBeanConstructorInfo[] constructors,
        MBeanOperationInfo[] operations,
        MBeanNotificationInfo[] notifications
      )
    */
    this.mBeanInfo =
      new MBeanInfo(
        this.getClass().getName(),
        "EmployeeDynamic",
          attributes.toArray(new MBeanAttributeInfo[attributes.size()]),
          constructors.toArray(new MBeanConstructorInfo[constructors.size()]),
          operations.toArray(new MBeanOperationInfo[operations.size()]),
          notifications.toArray(new MBeanNotificationInfo[notifications.size()])
      );
  }

  // Get attribute of the internal Bean, i.e Person
  @Override
  public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
    if (attribute.equals("name")) {
      return this.employee.getName();
    } else if (attribute.equals("age")) {
      return this.employee.getAge();
    }
    return null;
  }

  @Override
  public void setAttribute(Attribute attribute) throws
      AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {

  }


  @Override
  public AttributeList getAttributes(String[] attributes) {
    if (attributes == null || attributes.length == 0) {
      return null;
    }

    try {
      AttributeList attributeList = new AttributeList();

      for (String attribute : attributes) {
        attributeList.add(new Attribute(attribute, getAttribute(attribute)));
      }

      return attributeList;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    return null;
  }

  // method invocation
  @Override
  public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
    if (actionName.equals("sayHello")) {
      return this.employee.sayHello(params[0].toString());
    }
    return null;
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    return this.mBeanInfo;
  }

}
