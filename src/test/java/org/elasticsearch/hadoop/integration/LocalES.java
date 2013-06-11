/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.integration;

import java.io.File;

import org.junit.rules.ExternalResource;

public class LocalES extends ExternalResource {

    private static ESEmbeddedServer master;
    private static ESEmbeddedServer slave;

    public static final String CLUSTER_NAME = "ES-HADOOP-TEST";
    private static final String ES_DATA_PATH = "build/es.data";
    public static final String DATA_PORTS = "9500-9599";
    public static final String TRANSPORT_PORTS = "9600-9699";
    public static final String DATA_PORTS_SLAVE = "9700-9799";
    public static final String TRANSPORT_PORTS_SLAVE = "9800-9899";

    private boolean USE_SLAVE = false;

    @Override
    protected void before() throws Throwable {
        if (master == null) {
            System.out.println("Starting Elasticsearch Master...");
            master = new ESEmbeddedServer(CLUSTER_NAME, ES_DATA_PATH, DATA_PORTS, TRANSPORT_PORTS);
            master.start();
        }

        if (USE_SLAVE && slave == null) {
            System.out.println("Starting Elasticsearch Slave...");
            slave = new ESEmbeddedServer(CLUSTER_NAME, ES_DATA_PATH, DATA_PORTS, TRANSPORT_PORTS);
            slave.start();
        }
    }

    @Override
    protected void after() {
        if (master != null) {
            if (USE_SLAVE && slave != null) {
                System.out.println("Stopping Elasticsearch Slave...");
                slave.stop();
                slave = null;
            }

            System.out.println("Stopping Elasticsearch Master...");
            master.stop();
            master = null;

            // delete data folder
            TestUtils.delete(new File(ES_DATA_PATH));
        }
    }
}
