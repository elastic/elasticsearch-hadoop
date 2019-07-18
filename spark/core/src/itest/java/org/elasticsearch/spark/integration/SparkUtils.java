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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ReflectionUtils;

import com.esotericsoftware.kryo.Kryo;

public abstract class SparkUtils {

    public static final String[] ES_SPARK_TESTING_JAR;

    static {
        // init ES-Hadoop JAR
        // expect the jar under build\libs
        try {
            File folder = new File(".." + File.separator + ".." + File.separator + "build" + File.separator + "libs" + File.separator).getCanonicalFile();
            System.out.println(folder.getAbsolutePath());
            // find proper jar
            File[] files = folder.listFiles(new FileFilter() {

                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().contains("-testing.jar");
                }
            });
            Assert.isTrue(files != null && files.length == 1,
                    String.format("Cannot find elasticsearch spark jar (too many or no file found);%s", Arrays.toString(files)));
            ES_SPARK_TESTING_JAR = new String[] { files[0].getAbsoluteFile().toURI().toString() };

        } catch (IOException ex) {
            throw new RuntimeException("Cannot find required files", ex);
        }
    }

    public static Kryo sparkSerializer(SparkConf conf) throws Exception {
        // reflection galore
        Class<?> ks = Class.forName("org.apache.spark.serializer.KryoSerializer", true, conf.getClass().getClassLoader());
        Constructor<?> ctr = ks.getDeclaredConstructor(SparkConf.class);
        Object ksInstance = ctr.newInstance(conf);
        Kryo kryo = ReflectionUtils.invoke(ReflectionUtils.findMethod(ks, "newKryo"), ksInstance);
        return kryo;
    }
}
