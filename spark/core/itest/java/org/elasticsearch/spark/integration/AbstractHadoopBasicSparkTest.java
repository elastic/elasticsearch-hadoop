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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.KryoRegistrator;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.esotericsoftware.kryo.Kryo;

public class AbstractHadoopBasicSparkTest implements Serializable {

    private transient final SparkConf conf = new SparkConf().setMaster("local").setAppName("basictest");
    private transient SparkConf cfg = null;
    private transient JavaSparkContext sc;


    @Before
    public void setup() {
        cfg = conf.clone();
    }

    @After
    public void clean() throws Exception {
        if (sc != null) {
            sc.stop();
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

    @Test
    public void testBasicRead() {
        String input = TestUtils.sampleArtistsDat();
        sc = new JavaSparkContext(cfg);
        JavaRDD<String> data = sc.textFile(input).cache();

        assertThat((int) data.count(), is(greaterThan(300)));

        long radioHead = data.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("Radiohead"); }
        }).count();

        assertThat((int) radioHead, is(1));
        assertEquals(1, radioHead);

        long megadeth = data.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("Megadeth"); }
        }).count();

        assertThat((int) megadeth, is(1));
    }

    public static class MyRegistrator implements Serializable, KryoRegistrator {

        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(Text.class);
            kryo.register(MapWritable.class);
        }
    }

    @Test
    public void testHadoopOldApiRead() throws Exception {
        cfg.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //clone.set("spark.kryo.registrator", MyRegistrator.class.getName());

        sc = new JavaSparkContext(cfg);

        String target = "spark-test/hadoop-basic";

        RestUtils.touch("spark-test");
        RestUtils.postData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.postData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh("spark-test");

        JobConf hdpConf = HdpBootstrap.hadoopConfig();
        hdpConf.set(ConfigurationOptions.ES_RESOURCE, target);


        //JavaPairRDD data = sc.newAPIHadoopRDD(hdpConf, EsInputFormat.class, NullWritable.class, MapWritable.class);
        JavaPairRDD data = sc.hadoopRDD(hdpConf, EsInputFormat.class, NullWritable.class, MapWritable.class);

        long messages = data.filter(new Function<Tuple2<Text, MapWritable>, Boolean>() {
            public Boolean call(Tuple2<Text, MapWritable> t) { return t._2.containsKey(new Text("message")); }
        }).count();

        JavaRDD map = data.map(new Function<Tuple2<Text, MapWritable>, Map<String, Object>>() {
            public Map<String, Object> call(Tuple2<Text, MapWritable> v1) throws Exception {
                return (Map<String, Object>) WritableUtils.fromWritable(v1._2);
            }
        });

        JavaRDD fooBar = data.map(new Function<Tuple2<Text, MapWritable>, String>() {
            public String call(Tuple2<Text, MapWritable> v1) throws Exception {
                return v1._1.toString();
            }
        });

        assertThat((int) data.count(), is(2));
        System.out.println(data.take(10));
        System.out.println(messages);
        System.out.println(fooBar.take(2));
        System.out.println(map.take(10));
    }
}
