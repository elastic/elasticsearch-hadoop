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

import java.net.URL;
import java.util.Enumeration;

import org.elasticsearch.hadoop.fixtures.LocalEs;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractScalaEsScalaSpark.class })
public class SparkScalaSuite {

    static {
        try {
            // String clazz =
            // "org/eclipse/jetty/servlet/ServletContextHandler.class";
            String clazz = "javax/servlet/Servlet.class";
            Enumeration<URL> res = SparkScalaSuite.class.getClassLoader().getResources(clazz);
            if (!res.hasMoreElements()) {
                System.out.println("Cannot find class " + clazz);
            }
            while (res.hasMoreElements()) {
                res.nextElement().toURI().toASCIIString();
            }
        } catch (Exception ex) {
        }
    }

    @ClassRule
    public static ExternalResource resource = new LocalEs();
}
