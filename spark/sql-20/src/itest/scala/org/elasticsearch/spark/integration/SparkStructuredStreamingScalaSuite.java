package org.elasticsearch.spark.integration;

import org.elasticsearch.hadoop.LocalEs;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractScalaEsSparkStructuredStreaming.class })
public class SparkStructuredStreamingScalaSuite {

    @ClassRule
    public static ExternalResource resource = new LocalEs();
}
