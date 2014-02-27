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
package org.elasticsearch.hadoop.mr.compat;

import java.lang.reflect.Proxy;

/**
 * Handler for the binary breakage between Hadoop 1 and Hadoop 2.
 */
public class CompatHandler {

    public static Counter counter(org.apache.hadoop.mapreduce.Counter target) {
        return proxy(target, Counter.class);
    }

    public static CounterGroup counterGroup(org.apache.hadoop.mapreduce.CounterGroup target) {
        return proxy(target, CounterGroup.class);
    }

    public static JobContext jobContext(org.apache.hadoop.mapreduce.JobContext target) {
        return proxy(target, JobContext.class);
    }

    public static MapContext mapContext(org.apache.hadoop.mapreduce.MapContext target) {
        return proxy(target, MapContext.class);
    }

    public static ReduceContext reduceContext(org.apache.hadoop.mapreduce.ReduceContext target) {
        return proxy(target, ReduceContext.class);
    }

    public static TaskAttemptContext taskAttemptContext(org.apache.hadoop.mapreduce.TaskAttemptContext target) {
        return proxy(target, TaskAttemptContext.class);
    }

    public static TaskInputOutputContext taskInputOutputContext(org.apache.hadoop.mapreduce.TaskInputOutputContext target) {
        return proxy(target, TaskInputOutputContext.class);
    }

    @SuppressWarnings("unchecked")
    private static <P, T> P proxy(T target, Class<P> proxy) {
        return (P) Proxy.newProxyInstance(proxy.getClassLoader(), new Class[] { CompatProxy.class, proxy }, new ReflectiveInvoker(target));
    }

    public static Object unwrap(Object object) {
        if (object instanceof CompatProxy) {
            return ((ReflectiveInvoker) Proxy.getInvocationHandler(object)).target();
        }
        return object;
    }
}
