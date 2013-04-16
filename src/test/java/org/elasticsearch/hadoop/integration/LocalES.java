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
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.rules.ExternalResource;

public class LocalES extends ExternalResource {

    private static ESEmbeddedServer es;
    private static final String ES_DATA_PATH = "build/es.data";

    @Override
    protected void before() throws Throwable {
        TestUtils.hackHadoopStagingOnWin();

        if (es == null) {
            System.out.println("Starting Elasticsearch...");
            es = new ESEmbeddedServer(ES_DATA_PATH);
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
