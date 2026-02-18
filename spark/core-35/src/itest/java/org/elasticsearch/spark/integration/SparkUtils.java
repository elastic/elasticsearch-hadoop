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

import java.lang.reflect.Constructor;

import org.apache.spark.SparkConf;
import org.elasticsearch.hadoop.Provisioner;
import org.elasticsearch.hadoop.util.ReflectionUtils;

import com.esotericsoftware.kryo.Kryo;

public abstract class SparkUtils {

    public static final String[] ES_SPARK_TESTING_JAR = new String[] {Provisioner.ESHADOOP_TESTING_JAR};

    public static Kryo sparkSerializer(SparkConf conf) throws Exception {
        // reflection galore
        Class<?> ks = Class.forName("org.apache.spark.serializer.KryoSerializer", true, conf.getClass().getClassLoader());
        Constructor<?> ctr = ks.getDeclaredConstructor(SparkConf.class);
        Object ksInstance = ctr.newInstance(conf);
        Kryo kryo = ReflectionUtils.invoke(ReflectionUtils.findMethod(ks, "newKryo"), ksInstance);
        return kryo;
    }
}
