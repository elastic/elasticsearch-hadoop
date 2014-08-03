/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.spark.integration;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static scala.collection.JavaConversions.propertiesAsScalaMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.spark.api.java.JavaEsSpark;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AbstractJavaEsSparkTest implements Serializable {

    private final transient SparkConf conf = new SparkConf().setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS)).setMaster("local").setAppName("estest");
    private transient SparkConf cfg;
    private transient JavaSparkContext sc = null;

    @Before
    public void setup() {
        cfg = conf.clone();
    }

    @After
    public void clean() throws Exception {
        if (sc != null) {
            sc.stop();
            // wait for jetty & spark to properly shutdown
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

    @Test
    public void testEsRDDWrite() throws Exception {
        Map<String, ?> doc1 = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> doc2 = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

        String target = "spark-test/java-basic-write";
        sc = new JavaSparkContext(cfg);
        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(doc1, doc2));
        // eliminate with static import
        JavaEsSpark.saveToEs(javaRDD, target);
        RestUtils.exists("spark-test/java-write");
        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("SFO"));
    }

    @Test
    public void testEsRDDRead() throws Exception {
        SparkConf clone = conf.clone();

        sc = new JavaSparkContext(clone);

        String target = "spark-test/java-basic-read";

        RestUtils.touch("spark-test");
        RestUtils.putData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.putData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh("spark-test");

//        JavaRDD<scala.collection.Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, target);
//        JavaRDD messages = esRDD.filter(new Function<scala.collection.Map<String, Object>, Boolean>() {
//            public Boolean call(scala.collection.Map<String, Object> map) {
//            	for (Entry<String, Object> entry: JavaConversions.asJavaMap(map).entrySet()) {
//					if (entry.getValue().toString().contains("message")) {
//						return Boolean.TRUE;
//					}
//				}
//            	return Boolean.FALSE;
//            }
//        });

        JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, target);
        JavaRDD<Map<String, Object>> messages = esRDD.filter(new Function<Map<String, Object>, Boolean>() {
			@Override
			public Boolean call(Map<String, Object> map) throws Exception {
				for (Entry<String, Object> entry: map.entrySet()) {
					if (entry.getValue().toString().contains("message")) {
						return Boolean.TRUE;
					}
				}
				return Boolean.FALSE;
			}
        });
        
        // jdk8
        //esRDD.filter(m -> m.values().stream().filter(v -> v.contains("message")));
        
        assertThat((int) messages.count(), is(2));
        System.out.println(messages.take(10));
        System.out.println(messages);
    }
}