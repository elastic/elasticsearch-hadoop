package org.elasticsearch.spark.integration;

import scala.collection.mutable.Map;
import scala.collection.mutable.HashMap;

import java.util.Properties;

public class ScalaUtils {
    /*
     * Scala removed scala.collection.JavaConversions.propertiesAsScalaMap() in 2.13, replacing it with an implicit
     * jdk.CollectionConverters.asScala. This is an attempt to get a method that works in 2.10-2.13. It can be removed and replaced with
     * jdk.CollectionConverters once we no longer support scala older than 2.13.
     */
    public static scala.collection.Map<String, String> propertiesAsScalaMap(Properties props) {
        Map scalaMap = new HashMap();
        for (java.util.Map.Entry<Object, Object> entry : props.entrySet()) {
            scalaMap.put(entry.getKey(), entry.getValue());
        }
        return scalaMap;
    }
}

