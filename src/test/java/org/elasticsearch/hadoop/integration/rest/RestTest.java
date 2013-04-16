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
package org.elasticsearch.hadoop.integration.rest;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.integration.TestSettings;
import org.elasticsearch.hadoop.rest.RestClient;
import org.junit.Test;

/**
 */
public abstract class RestTest {

    private RestClient client = new RestClient(new TestSettings());

    @Test
    public void testPagination() throws Exception {
        client.query("twitter/_search?q=kimchy", 0, 2);
    }
}
