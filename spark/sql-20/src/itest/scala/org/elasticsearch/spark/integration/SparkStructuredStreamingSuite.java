package org.elasticsearch.spark.integration;

import org.elasticsearch.hadoop.LocalEs;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractJavaEsSparkStructuredStreamingTest.class })
public class SparkStructuredStreamingSuite {

    @ClassRule
    public static ExternalResource resource = new LocalEs();
}
