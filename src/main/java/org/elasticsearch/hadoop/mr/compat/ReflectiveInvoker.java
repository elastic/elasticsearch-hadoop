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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ReflectionUtils;

public class ReflectiveInvoker implements InvocationHandler {

    private final Object target;

    public ReflectiveInvoker(Object target) {
        this.target = target;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Method m = ReflectionUtils.findMethod(target.getClass(), method.getName(), method.getParameterTypes());
        Assert.notNull(m, String.format("Cannot find method %s on target %s", method, target));
        return m.invoke(target, args);
    }

    public Object target() {
        return target;
    }
}
