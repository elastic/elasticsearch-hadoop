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
import java.util.Map;

import org.elasticsearch.hadoop.mr.Counters;
import org.elasticsearch.hadoop.util.ReflectionUtils;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.stats.CascadingStats;

public class ExtendedLocalFlowConnector extends LocalFlowConnector {

    public ExtendedLocalFlowConnector() {
        super();
    }

    public ExtendedLocalFlowConnector(Map<Object, Object> properties) {
        super(properties);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Flow connect(FlowDef flowDef) {
        final Flow flow = super.connect(flowDef);
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

    private void printStats(Flow flow) {
        CascadingStats stats = flow.getStats();
        System.out.println("Cascading stats for " + Counters.class.getName());
        for (String counter : stats.getCountersFor(Counters.class)) {
            System.out.println(String.format("%s\t%s", counter, stats.getCounterValue(Counters.class.getName(), counter)));
        }
    }
}
