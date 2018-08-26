package com.concretepage.lang;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
public class MainClass {
       public static void main(String[] args) throws InstantiationException, IllegalAccessException,
                 NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {

	      CustomClassLoaderDemo loader = new CustomClassLoaderDemo();
              Class<?> c = loader.findClass("com.concretepage.lang.Test");
              Object ob = c.newInstance();
              Method md = c.getMethod("show");
              md.invoke(ob);
       }
}