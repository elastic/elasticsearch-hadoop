package org.elasticsearch.spark.integration;

import org.elasticsearch.hadoop.LocalEs;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractScalaEsScalaSpark.class })
public class SparkScalaSuite {

    // static {
    // try {
    // Enumeration<URL> res =
    // SparkScalaSuite.class.getClassLoader().getResources("javax/servlet/Servlet.class");
    // while (res.hasMoreElements()) {
    // System.out.println(res.nextElement().toURI().toASCIIString());
    // }
    // } catch (Exception ex) {
    // }
    // }
    @ClassRule
    public static ExternalResource resource = new LocalEs();
}
