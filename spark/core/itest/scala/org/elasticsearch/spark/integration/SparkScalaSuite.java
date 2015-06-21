package org.elasticsearch.spark.integration;

import java.net.URL;
import java.util.Enumeration;

import org.elasticsearch.hadoop.LocalEs;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractScalaEsScalaSpark.class })
public class SparkScalaSuite {

    static {
        try {
            // String clazz =
            // "org/eclipse/jetty/servlet/ServletContextHandler.class";
            String clazz = "javax/servlet/Servlet.class";
            Enumeration<URL> res = SparkScalaSuite.class.getClassLoader().getResources(clazz);
            if (!res.hasMoreElements()) {
                System.out.println("Cannot find class " + clazz);
            }
            while (res.hasMoreElements()) {
                System.out.println(res.nextElement().toURI().toASCIIString());
            }
        } catch (Exception ex) {
        }
    }

    @ClassRule
    public static ExternalResource resource = new LocalEs();
}
