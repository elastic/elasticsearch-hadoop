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
package org.elasticsearch.hadoop.integration.cascading;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.elasticsearch.hadoop.mr.Counter;
import org.elasticsearch.hadoop.util.ReflectionUtils;

import cascading.flow.Flow;
import cascading.stats.CascadingStats;

public abstract class StatsUtils {

    @SuppressWarnings("rawtypes")
    public static Flow proxy(final Flow flow) {
        Flow proxy = (Flow) Proxy.newProxyInstance(Flow.class.getClassLoader(), new Class[] { Flow.class }, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        Object invocation = ReflectionUtils.invoke(method, flow, args);
                        if ("complete".equals(method.getName())) {
                            printStats(flow);
                        }
                        return invocation;
            }
        });

        return proxy;
    }

    private static void printStats(Flow flow) {
        CascadingStats stats = flow.getStats();
        System.out.println("Cascading stats for " + Counter.class.getName());
        for (String counter : stats.getCountersFor(Counter.class)) {
            System.out.println(String.format("%s\t%s", counter, stats.getCounterValue(Counter.class.getName(), counter)));
        }
    }
}
