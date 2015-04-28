package org.elasticsearch.spark.rdd;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.ReflectionUtils;

import scala.Function0;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

abstract class CompatUtils {

    private static final boolean SPARK_11_AVAILABLE = ObjectUtils.isClassPresent("org.apache.spark.util.TaskCompletionListener", SparkConf.class.getClassLoader());

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
        boolean isSpark13 = ObjectUtils.isClassPresent("org.apache.spark.sql.DataFrame", SparkConf.class.getClassLoader());
        boolean isEshForSpark13 = !ObjectUtils.isClassPresent("org.elasticsearch.spark.sql.EsSchemaRDDWriter", CompatUtils.class.getClassLoader());

        // XOR can be applied as well but != increases readability
        if (isSpark13 != isEshForSpark13) {

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

            if (sparkVersionMethod == null) {
                sparkVersion = (isSpark13 ? "1.3+" : "1.0-1.2");
            }
            else {
                sparkVersion = ReflectionUtils.<String> invoke(sparkVersionMethod, target);
            }

            LogFactory.getLog("org.elasticsearch.spark.rdd.EsSpark").
                warn(String.format("Incorrect classpath detected; Elasticsearch Spark compiled for Spark %s but used with Spark %s",
                        (isEshForSpark13 ? "1.3 (or higher)" : "1.0-1.2"),
                        sparkVersion
                        ));
        }

    }

    private static abstract class Spark10TaskContext {
        private static Field INTERRUPTED_FIELD;

        static {
            Field field = ReflectionUtils.findField(TaskContext.class, "interrupted");
            ReflectionUtils.makeAccessible(field);
            INTERRUPTED_FIELD = field;
        }

        static void addOnCompletition(TaskContext taskContext, final Function0<?> function) {
            taskContext.addOnCompleteCallback(new AbstractFunction0() {
                @Override
                public BoxedUnit apply() {
                    function.apply();
                    return BoxedUnit.UNIT;
                }
            });

        }

        static boolean isInterrupted(TaskContext taskContext) {
            return ReflectionUtils.getField(INTERRUPTED_FIELD, taskContext);
        }
    }

    private static abstract class Spark11TaskContext {
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
    }

    static void addOnCompletition(TaskContext taskContext, Function0<?> function) {
        if (SPARK_11_AVAILABLE) {
            Spark11TaskContext.addOnCompletition(taskContext, function);
        }
        else {
            Spark10TaskContext.addOnCompletition(taskContext, function);
        }
    }

    static boolean isInterrupted(TaskContext taskContext) {
        return (SPARK_11_AVAILABLE ? Spark11TaskContext.isInterrupted(taskContext) : Spark10TaskContext.isInterrupted(taskContext));
    }

    static void warnSchemaRDD(Object rdd, Log log) {
        if (rdd != null && SCHEMA_RDD_LIKE_CLASS != null) {
            if (SCHEMA_RDD_LIKE_CLASS.isAssignableFrom(rdd.getClass())) {
                log.warn("basic RDD saveToEs() called on a Spark SQL SchemaRDD; typically this is a mistake(as the SQL schema will be ignored). Use 'org.elasticsearch.spark.sql' package instead");
            }
        }
    }
}