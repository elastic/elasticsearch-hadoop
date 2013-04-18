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

import org.apache.commons.io.FileUtils;
import org.elasticsearch.hadoop.unit.util.TestUtils;
import org.junit.rules.ExternalResource;

public class LocalES extends ExternalResource {

    private static ESEmbeddedServer es;
    private static final String ES_DATA_PATH = "build/es.data";
    public static final String DATA_PORTS = "9700-9800";
    public static final String TRANSPORT_PORTS = "9800-9900";

    @Override
    protected void before() throws Throwable {
        TestUtils.hackHadoopStagingOnWin();

        if (es == null) {
            System.out.println("Starting Elasticsearch...");
            es = new ESEmbeddedServer(ES_DATA_PATH, DATA_PORTS, TRANSPORT_PORTS);
            es.start();
        }
    }

    @Override
    protected void after() {
        if (es != null) {
            System.out.println("Stopping Elasticsearch...");
            es.stop();
            es = null;

            // delete data folder
            FileUtils.deleteQuietly(new File(ES_DATA_PATH));
        }
    }
}
