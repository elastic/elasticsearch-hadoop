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

package org.elasticsearch.hadoop.util.ecs;

import org.elasticsearch.hadoop.util.BytesArray;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ElasticCommonSchemaTest {

    @Test
    public void testRenderMessage() throws Exception {
        ElasticCommonSchema ecs = new ElasticCommonSchema(ElasticCommonSchema.V0_992);

        MessageTemplate messageTemplate = ecs.buildTemplate()
                .setEventCategory("error")
                .setEventType("bulk")
                .build();

        BytesArray message = messageTemplate.generateMessage("2018-06-15T18:30:45-0500", "Boom!", "remote_exception", "BOOOOOM!", "{\"raw\":\"json\"}");

        assertNotNull(message);
        System.out.println(message);
    }
}