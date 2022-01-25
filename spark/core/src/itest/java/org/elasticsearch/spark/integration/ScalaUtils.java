package org.elasticsearch.spark.integration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

public class ScalaUtils {
    /*
     * Scala renamed scala.collection.JavaConversions to jdk.CollectionConverters in 2.13.
     */
    public static scala.collection.Map<String, String> propertiesAsScalaMap(Properties props) {
        Class conversionsClass;
        try {
            conversionsClass = Class.forName("scala.collection.JavaConversions");
        } catch (ClassNotFoundException e) {
            try {
                conversionsClass = Class.forName("jdk.CollectionConverters");
            } catch (ClassNotFoundException classNotFoundException) {
                throw new RuntimeException("No collection converter class found");
            }
        }
        Method method;
        try {
            method = conversionsClass.getMethod("propertiesAsScalaMap", Properties.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("No propertiesAsScalaMap method on " + conversionsClass);
        }
        try {
            Object result = method.invoke(null, props);
            return (scala.collection.Map<String, String>) result;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
