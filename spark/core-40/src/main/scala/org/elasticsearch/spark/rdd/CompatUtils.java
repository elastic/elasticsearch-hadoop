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
package org.elasticsearch.spark.rdd;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.ReflectionUtils;

import scala.Function0;

abstract class CompatUtils {

    private static final Class<?> SCHEMA_RDD_LIKE_CLASS;

    static {
        Class<?> clz = null;
        try {
            clz = Class.forName("org.apache.spark.sql.SchemaRDDLike", false, CompatUtils.class.getClassLoader());
        } catch (Exception ex) {
            // ignore
        }
        SCHEMA_RDD_LIKE_CLASS = clz;

        // apply the warning when the class is loaded (to cover all access points)

        // check whether the correct es-hadoop is used with the correct Spark version
        checkSparkLibraryCompatibility(false);
    }

    static void checkSparkLibraryCompatibility(boolean throwOnIncompatible) {
        // check whether the correct es-hadoop is used with the correct Spark version
        boolean isSpark13Level = ObjectUtils.isClassPresent("org.apache.spark.sql.DataFrame", SparkConf.class.getClassLoader());
        boolean isSpark20Level = ObjectUtils.isClassPresent("org.apache.spark.sql.streaming.StreamingQuery", SparkConf.class.getClassLoader());

        try {
            CompatibilityLevel compatibilityLevel = ObjectUtils.instantiate("org.elasticsearch.spark.sql.SparkSQLCompatibilityLevel", CompatUtils.class.getClassLoader());
            boolean isEshForSpark20 = "20".equals(compatibilityLevel.versionId());
            String esSupportedSparkVersion = compatibilityLevel.versionDescription();

            String errorMessage = null;

            if (!(isSpark13Level || isSpark20Level)) {
                String sparkVersion = getSparkVersionOr("1.0-1.2");
                errorMessage = String.format("Incorrect classpath detected; Elasticsearch Spark compiled for Spark %s but used with unsupported Spark version %s",
                        esSupportedSparkVersion, sparkVersion);
            } else if (isSpark20Level != isEshForSpark20) { // XOR can be applied as well but != increases readability
                String sparkVersion = getSparkVersionOr(isSpark13Level ? "1.3-1.6" : "2.0+");
                errorMessage = String.format("Incorrect classpath detected; Elasticsearch Spark compiled for Spark %s but used with Spark %s",
                        esSupportedSparkVersion, sparkVersion);
            }

            if (errorMessage != null) {
                if (throwOnIncompatible) {
                    throw new EsHadoopIllegalStateException(errorMessage);
                } else {
                    LogFactory.getLog("org.elasticsearch.spark.rdd.EsSpark").warn(errorMessage);
                }
            }
        } catch (EsHadoopIllegalStateException noClass) {
            // In the event that someone is using the core jar without sql support, (like in our tests) this will be logged instead.
            String errorMessage = "Elasticsearch Spark SQL support could not be verified.";
            if (throwOnIncompatible) {
                throw new EsHadoopIllegalStateException(errorMessage, noClass);
            } else {
                LogFactory.getLog("org.elasticsearch.spark.rdd.EsSpark").info(errorMessage + " Continuing with core support.");
            }
        }
    }

    private static String getSparkVersionOr(String defaultValue) {
        // need SparkContext which requires context
        // as such do another reflex dance
        String sparkVersion = null;

        // Spark 1.0 - 1.1: SparkContext$.MODULE$.SPARK_VERSION();
        // Spark 1.2+     : package$.MODULE$.SPARK_VERSION();
        Object target = org.apache.spark.SparkContext$.MODULE$;
        Method sparkVersionMethod = ReflectionUtils.findMethod(target.getClass(), "SPARK_VERSION");

        if (sparkVersionMethod == null) {
            target = org.apache.spark.package$.MODULE$;
            sparkVersionMethod = ReflectionUtils.findMethod(target.getClass(), "SPARK_VERSION");
        }

        if (sparkVersionMethod != null) {
            sparkVersion = ReflectionUtils.<String>invoke(sparkVersionMethod, target);
        } else {
            sparkVersion = defaultValue;
        }

        return sparkVersion;
    }

    static void addOnCompletition(TaskContext taskContext, final Function0<?> function) {
        taskContext.addTaskCompletionListener(new TaskCompletionListener() {
            @Override
            public void onTaskCompletion(TaskContext context) {
                function.apply();
            }
        });
    }

    static boolean isInterrupted(TaskContext taskContext) {
        return taskContext.isInterrupted();
    }

    static void warnSchemaRDD(Object rdd, Log log) {
        if (rdd != null && SCHEMA_RDD_LIKE_CLASS != null) {
            if (SCHEMA_RDD_LIKE_CLASS.isAssignableFrom(rdd.getClass())) {
                log.warn("basic RDD saveToEs() called on a Spark SQL SchemaRDD; typically this is a mistake(as the SQL schema will be ignored). Use 'org.elasticsearch.spark.sql' package instead");
            }
        }
    }
}