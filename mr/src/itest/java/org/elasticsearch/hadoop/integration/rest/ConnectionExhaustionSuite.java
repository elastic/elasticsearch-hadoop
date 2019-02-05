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
package org.elasticsearch.hadoop.integration.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.LocalEs;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

//@RunWith(Suite.class)
//@Suite.SuiteClasses({ ConnectionExhaustionSuite.AbstractConnectionExhaustionTest.class })
public class ConnectionExhaustionSuite {

    @ClassRule
    public static ExternalResource resource = new LocalEs();

    public static class AbstractConnectionExhaustionTest {

        private static final boolean POOLED = true;

        private static final boolean SLEEP = !POOLED;

        private static final boolean CHATTY = false;

        private static final long MAX_RUN_TIME = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);
        private static final long SLEEP_TIME = 1L;

        private static final Settings SETTINGS = new TestSettings("connection/test");
        // Simulate multiple jobs running in the environment at a time
        private static final String[] JOB_KEYS;
        static {
            String job1 = UUID.randomUUID().toString();
            String job2 = UUID.randomUUID().toString();
            String job3 = UUID.randomUUID().toString();

            JOB_KEYS = new String[]{
                    job1,
                    job1,
                    job2,
                    job3,
                    job3,
                    job3
            };
        }

//        @Test
        public void exhaustConnections() throws InterruptedException {
            List<Thread> threads = new ArrayList<>();
            int workerNum = 0;
            for (String jobKey : JOB_KEYS) {
                final Settings workerSettings = SETTINGS.copy();
                if (POOLED) {
                    SettingsUtils.setJobTransportPoolingKey(workerSettings, jobKey);
                }
                Thread worker = new Thread(new Exhauster(++workerNum, workerSettings));
                worker.start();
                threads.add(worker);
            }

            for (Thread thread : threads) {
                thread.join();
            }
        }

        private class Exhauster implements Runnable {
            private final Log log = LogFactory.getLog(this.getClass());
            private final Settings workerSettings;
            private final String workerId;

            Exhauster(int workerNumber, Settings workerSettings) {
                this.workerId = "Worker # " + workerNumber;
                this.workerSettings = workerSettings;
            }

            @Override
            public void run() {
                long cycles = 0L;
                try {
                    long startTime = System.currentTimeMillis();
                    long lastStatus = startTime;
                    info("Starting time:" + startTime);
                    info("Running until:" + (startTime + MAX_RUN_TIME));
                    for (long currentTime = System.currentTimeMillis(); currentTime < startTime + MAX_RUN_TIME; currentTime = System.currentTimeMillis()) {
                        if (CHATTY) {
                            info("" + currentTime + ": Executing GET");
                        } else if (lastStatus + 1000 < currentTime) {
                            lastStatus = currentTime;
                            info("Processing... (Cycles : " + cycles + ")");
                        }
                        RestClient client = new RestClient(workerSettings);
                        client.getHttpDataNodes();
                        client.close();
                        cycles++;
                        if (SLEEP) TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
                    }
                    info("Completed test without exhaustion.");
                    info("Cycles completed: " + cycles);
                } catch (Exception e) {
                    error("Completed test with example of socket exhaustion. Cycles completed at failure: " + cycles, e);
                }
            }

            private void info(String mesg) {
                log.info(workerId + ": " + mesg);
            }

            private void error(String mesg, Throwable t) {
                log.error(workerId + ": " + mesg, t);
            }
        }
    }
}
