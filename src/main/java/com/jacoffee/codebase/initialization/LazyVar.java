package com.jacoffee.codebase.initialization;

/**
    double-checked locking to initialize two variable lazily and prevent re-initialization
    00000000
*/
public class LazyVar {

   private double bar;
   private String foo;
   // 0
   private volatile byte bitmap;

   private double internalBar() {
    synchronized (this) {
     if ((byte) (bitmap & 0x1) == 0) {
       this.bar = 3.0;
       this.bitmap = (byte) (bitmap | 0x1);
     }
     return this.bar;
    }
   }

  private String internalFoo() {
    synchronized (this) {
      if ((byte) (bitmap & 0x2) == 0) {
        this.foo = "jacoffee";
        this.bitmap = (byte) (bitmap | 0x2);
      }
      return this.foo;
    }
  }

  public double bar() {
    return (byte) (this.bitmap & 0x1) == 0 ? internalBar() : this.bar;
  }

  public String foo() {
    return (byte) (this.bitmap & 0x2) == 0 ? internalFoo() : this.foo;
  }

}
