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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.spark.rdd.Metadata;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.*;
import static org.elasticsearch.spark.rdd.Metadata.*;

import static org.hamcrest.Matchers.*;

import static scala.collection.JavaConversions.*;
import scala.Tuple2;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractJavaEsSparkTest implements Serializable {

    private static final transient SparkConf conf = new SparkConf().setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS)).setMaster("local").setAppName("estest");
    private static transient JavaSparkContext sc = null;

    @BeforeClass
    public static void setup() {
        sc = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void clean() throws Exception {
        if (sc != null) {
            sc.stop();
            // wait for jetty & spark to properly shutdown
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

    // @Test
    public void testEsRDDWrite() throws Exception {
        Map<String, ?> doc1 = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> doc2 = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

        String target = "spark-test/java-basic-write";
        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(doc1, doc2));
        // eliminate with static import
        JavaEsSpark.saveToEs(javaRDD, target);
        JavaEsSpark.saveToEs(javaRDD, ImmutableMap.of(ES_RESOURCE, target + "1"));

        assertEquals(2, JavaEsSpark.esRDD(sc, target).count());
        assertTrue(RestUtils.exists(target));
        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("SFO"));
    }

    // @Test
    public void testEsRDDWriteWithMappingId() throws Exception {
        Map<String, ?> doc1 = ImmutableMap.of("one", 1, "two", 2, "number", 1);
        Map<String, ?> doc2 = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran", "number", 2);

        String target = "spark-test/java-id-write";
        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(doc1, doc2));
        // eliminate with static import
        JavaEsSpark.saveToEs(javaRDD, target, ImmutableMap.of(ES_MAPPING_ID, "number"));

        assertEquals(2, JavaEsSpark.esRDD(sc, target).count());
        assertTrue(RestUtils.exists(target + "/1"));
        assertTrue(RestUtils.exists(target + "/2"));
        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("SFO"));
    }

    // @Test
    public void testEsRDDWriteWithDynamicMapping() throws Exception {
        Map<String, ?> doc1 = ImmutableMap.of("one", 1, "two", 2, "number", 1);
        Map<String, ?> doc2 = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran", "number", 2);

        String target = "spark-test/java-dyn-id-write";
        JavaPairRDD<?, ?> pairRdd = sc.parallelizePairs(ImmutableList.of(new Tuple2<Object,Object>(1, doc1),
                new Tuple2<Object, Object>(2, doc2)));
        //JavaPairRDD pairRDD = JavaPairRDD.fromJavaRDD(tupleRdd);
        // eliminate with static import
        JavaEsSpark.saveToEsWithMeta(pairRdd, target);

        assertEquals(2, JavaEsSpark.esRDD(sc, target).count());
        assertTrue(RestUtils.exists(target + "/1"));
        assertTrue(RestUtils.exists(target + "/2"));
        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("SFO"));
    }

    // @Test
    public void testEsRDDWriteWithDynamicMappingBasedOnMaps() throws Exception {
        Map<String, ?> doc1 = ImmutableMap.of("one", 1, "two", 2, "number", 1);
        Map<String, ?> doc2 = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran", "number", 2);

        String target = "spark-test/java-dyn-map-id-write";
        Map<Metadata, Object> header1 = ImmutableMap.<Metadata, Object> of(ID, 1, TTL, "1d");
        Map<Metadata, Object> header2 = ImmutableMap.<Metadata, Object> of(ID, "2", TTL, "2d");
        JavaRDD<Tuple2<Object, Object>> tupleRdd = sc.parallelize(ImmutableList.<Tuple2<Object, Object>> of(new Tuple2(header1, doc1), new Tuple2(header2, doc2)));
        JavaPairRDD pairRDD = JavaPairRDD.fromJavaRDD(tupleRdd);
        // eliminate with static import
        JavaEsSpark.saveToEsWithMeta(pairRDD, target);

        assertEquals(2, JavaEsSpark.esRDD(sc, target).count());
        assertTrue(RestUtils.exists(target + "/1"));
        assertTrue(RestUtils.exists(target + "/2"));
        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("SFO"));
    }

    // @Test
    public void testEsRDDWriteWithMappingExclude() throws Exception {
        Map<String, ?> doc1 = ImmutableMap.of("reason", "business", "airport", "SFO");
        Map<String, ?> doc2 = ImmutableMap.of("participants", 2, "airport", "OTP");

        String target = "spark-test/java-exclude-write";

        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(doc1, doc2));
        JavaEsSpark.saveToEs(javaRDD, target, ImmutableMap.of(ES_MAPPING_EXCLUDE, "airport"));

        assertEquals(2, JavaEsSpark.esRDD(sc, target).count());
        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), containsString("business"));
        assertThat(RestUtils.get(target + "/_search?"), containsString("participants"));
        assertThat(RestUtils.get(target + "/_search?"), not(containsString("airport")));
    }

    // @Test
    public void testEsMultiIndexRDDWrite() throws Exception {
      Map<String, ?> doc1 = ImmutableMap.of("reason", "business", "airport", "SFO");
      Map<String, ?> doc2 = ImmutableMap.of("participants", 2, "airport", "OTP");

      String target = "spark-test/java-trip-{airport}";

      JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(doc1, doc2));
      JavaEsSpark.saveToEs(javaRDD, target);

      assertTrue(RestUtils.exists("spark-test/java-trip-OTP"));
      assertTrue(RestUtils.exists("spark-test/java-trip-SFO"));

      assertThat(RestUtils.get("spark-test/java-trip-SFO/_search?"), containsString("business"));
      assertThat(RestUtils.get("spark-test/java-trip-OTP/_search?"), containsString("participants"));
    }

    // @Test
    public void testEsRDDWriteAsJsonMultiWrite() throws Exception {
      String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
      String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";

      JavaRDD<String> stringRDD = sc.parallelize(ImmutableList.of(json1, json2));
      JavaEsSpark.saveJsonToEs(stringRDD, "spark-test/json-{airport}");
      JavaEsSpark.saveJsonToEs(stringRDD, "spark-test/json1-{airport}", Collections.<String, String> emptyMap());
      JavaEsSpark.saveJsonToEs(stringRDD, ImmutableMap.of(ES_RESOURCE, "spark-test/json2-{airport}"));

      byte[] json1BA = json1.getBytes();
      byte[] json2BA = json2.getBytes();

      JavaRDD<byte[]> byteRDD = sc.parallelize(ImmutableList.of(json1BA, json2BA));
      JavaEsSpark.saveJsonByteArrayToEs(byteRDD, "spark-test/json-ba-{airport}");
      JavaEsSpark.saveJsonByteArrayToEs(byteRDD, "spark-test/json-ba1-{airport}", Collections.<String, String> emptyMap());
      JavaEsSpark.saveJsonByteArrayToEs(byteRDD, ImmutableMap.of(ES_RESOURCE, "spark-test/json-ba2-{airport}"));

      assertTrue(RestUtils.exists("spark-test/json-SFO"));
      assertTrue(RestUtils.exists("spark-test/json-OTP"));

      assertTrue(RestUtils.exists("spark-test/json1-SFO"));
      assertTrue(RestUtils.exists("spark-test/json1-OTP"));

      assertTrue(RestUtils.exists("spark-test/json2-SFO"));
      assertTrue(RestUtils.exists("spark-test/json2-OTP"));

      assertTrue(RestUtils.exists("spark-test/json-ba-SFO"));
      assertTrue(RestUtils.exists("spark-test/json-ba-OTP"));

      assertTrue(RestUtils.exists("spark-test/json-ba1-SFO"));
      assertTrue(RestUtils.exists("spark-test/json-ba1-OTP"));

      assertTrue(RestUtils.exists("spark-test/json-ba2-SFO"));
      assertTrue(RestUtils.exists("spark-test/json-ba2-OTP"));

      assertThat(RestUtils.get("spark-test/json-SFO/_search?"), containsString("business"));
      assertThat(RestUtils.get("spark-test/json-OTP/_search?"), containsString("participants"));
    }

    // @Test
    public void testEsRDDZRead() throws Exception {
        String target = "spark-test/java-basic-read";

        RestUtils.touch("spark-test");
        RestUtils.postData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.postData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh("spark-test");

//        JavaRDD<scala.collection.Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, target);
//        JavaRDD messages = esRDD.filter(new Function<scala.collection.Map<String, Object>, Boolean>() {
//            public Boolean call(scala.collection.Map<String, Object> map) {
//                for (Entry<String, Object> entry: JavaConversions.asJavaMap(map).entrySet()) {
//                    if (entry.getValue().toString().contains("message")) {
//                        return Boolean.TRUE;
//                    }
//                }
//                return Boolean.FALSE;
//            }
//        });

        JavaRDD<Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, target).values();
        System.out.println(esRDD.collect());
        JavaRDD<Map<String, Object>> messages = esRDD.filter(new Function<Map<String, Object>, Boolean>() {
            @Override
            public Boolean call(Map<String, Object> map) throws Exception {
                return map.containsKey("message");
            }
        });

        // jdk8
        //esRDD.filter(m -> m.stream().filter(v -> v.contains("message")));

        assertThat((int) messages.count(), is(2));
        System.out.println(messages.take(10));
        System.out.println(messages);
    }


    // @Test
    public void testEsRDDZReadJson() throws Exception {
        String target = "spark-test/java-basic-json-read";

        RestUtils.touch("spark-test");
        RestUtils.postData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.postData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh("spark-test");

        JavaRDD<String> esRDD = JavaEsSpark.esJsonRDD(sc, target).values();
        System.out.println(esRDD.collect());
        JavaRDD<String> messages = esRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String string) throws Exception {
                return string.contains("message");
            }
        });

        // jdk8
        //esRDD.filter(m -> m.contains("message")));

        assertThat((int) messages.count(), is(2));
        System.out.println(messages.take(10));
        System.out.println(messages);
    }

    @Test
    public void testEsRDDZReadMultiIndex() throws Exception {
        String index = "spark-test";

        for (int i = 0; i < 10; i++) {
            RestUtils.postData(index + "/foo", ("{\"message\" : \"Hello World\", \"counter\":" + i +", \"message_date\" : \"2014-05-25\"}").getBytes());
            RestUtils.postData(index + "/bar", ("{\"message\" : \"Goodbye World\", \"counter\":" + i +", \"message_date\" : \"2014-05-25\"}").getBytes());
        }

        RestUtils.refresh(index);

        //JavaRDD<Map<String, Object>> wildRDD = JavaEsSpark.esRDD(sc, ImmutableMap.of(ES_RESOURCE, "spark*/foo")).values();
        JavaRDD<Map<String, Object>> typeRDD = JavaEsSpark.esRDD(sc, ImmutableMap.of(ES_RESOURCE, "spark*")).values();

        JavaRDD<Map<String, Object>> allRDD = JavaEsSpark.esRDD(sc, "_all/foo", "").values();
        //assertEquals(wildRDD.count(), allRDD.count());
        System.out.println(typeRDD.count());
        // assertEquals(500, allRDD.count());
    }

    // @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testNoResourceSpecified() throws Exception {
        JavaRDD<Map<String, Object>> rdd = JavaEsSpark.esRDD(sc).values();
        rdd.count();
    }
}