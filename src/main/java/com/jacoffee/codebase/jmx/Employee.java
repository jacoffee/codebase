package com.jacoffee.codebase.jmx;

public class Employee implements EmployeeMBean {

  // Attribute of MBean
  private String name;
  private int age;

  public Employee(String name, int age) {
    this.name = name;
    this.age = age;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getAge() {
    return age;
  }

  // Operation of MBean
  @Override
  public String sayHello(String hello) {
    System.out.println(hello);
    return this.name +  " : " + hello;
  }

}
