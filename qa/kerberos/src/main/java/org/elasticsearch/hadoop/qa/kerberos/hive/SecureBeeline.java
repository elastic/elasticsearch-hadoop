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

package org.elasticsearch.hadoop.qa.kerberos.hive;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import org.elasticsearch.hadoop.qa.kerberos.security.KeytabLogin;

public class SecureBeeline {
    public static void main(final String[] args) throws Exception {
        KeytabLogin.doAfterLogin(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                int firstArg = 0;
                String mainClassName = args[firstArg++];
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                Class<?> mainClass = Class.forName(mainClassName, true, loader);
                Method main = mainClass.getMethod("main", new Class[] {
                        Array.newInstance(String.class, 0).getClass()
                });
                String[] newArgs = Arrays.asList(args)
                        .subList(firstArg, args.length).toArray(new String[0]);
                try {
                    main.invoke(null, new Object[] { newArgs });
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e.getTargetException());
                }
                return null;
            }
        });
    }
}
