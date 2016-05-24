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
package org.elasticsearch.integration.storm;

import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.elasticsearch.hadoop.LocalEs;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.unit.TimeValue;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
public abstract class AbstractStormSuite {

    static ILocalCluster stormCluster;
    static Config cfg = new Config();

    static boolean isLocal = true;
    static Set<String> topologies = new LinkedHashSet<String>();

    public static Counter COMPONENT_HAS_COMPLETED;

    // storm + suite
    //public static final Counter DONE = new Counter(2);

    @ClassRule
    public static ExternalResource resource = new LocalEs() {

        @Override
        protected void after() {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            } catch (InterruptedException e) {
                // ignore
            }
            super.after();
        }
    };


    @ClassRule
    public static ExternalResource storm = new ExternalResource() {
        @Override
        protected void before() throws Throwable {

            copyPropertiesIntoCfg(cfg);

            String stormMode = TestSettings.TESTING_PROPS.getProperty("storm", "local");

            isLocal = "local".equals(stormMode);
            //cfg.setDebug(true);
            cfg.setNumWorkers(Integer.parseInt(TestSettings.TESTING_PROPS.getProperty("storm.numworkers", "2")));

            stormCluster = new LocalCluster();
        }

        @Override
        protected void after() {
            try {
                if (stormCluster != null) {
                    for (String topo : topologies) {
                        stormCluster.killTopology(topo);
                    }
                    stormCluster.shutdown();
                    stormCluster = null;
                }
            } catch (Exception ex) {
            }
        }
    };

    private static void copyPropertiesIntoCfg(Config cfg) {
        Properties props = TestSettings.TESTING_PROPS;

        for (String property : props.stringPropertyNames()) {
            cfg.put(property, props.get(property));
        }
    }

    public static void run(final String name, final StormTopology topo, final Counter hasCompleted) throws Exception {
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    start(name, topo);
                    hasCompleted.waitForZero(TimeValue.timeValueSeconds(20));
                } finally {
                    stop(name);
                }

            }
        }, "test-storm-runner");
        th.setDaemon(true);

        copyPropertiesIntoCfg(cfg);
        th.start();
    }

    public static void start(String name, StormTopology topo) {
        try {
            topologies.add(name);
            stormCluster.submitTopology(name, cfg, topo);
        } catch (Exception ex) {
            throw new RuntimeException("Cannot submit topology " + name, ex);
        }
    }

    public static void stop(String name) {
        if (topologies.remove(name)) {
            try {
                stormCluster.killTopology(name);
            } catch (Exception ex) {
                throw new RuntimeException("Cannot kill topology " + name, ex);
            }
        }
    }
}
