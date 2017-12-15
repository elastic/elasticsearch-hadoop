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
package org.elasticsearch.hadoop.hive;

import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class EsResourceTest {

    @Test
    public void testEsResourceRead() throws Exception {

        //The configuration is share in HiveInputFormat
        JobConf job = new JobConf();

        List<String> esResources = new ArrayList<String>();
        esResources.add("es_index_1/test");
        esResources.add("es_index_2/test");
        int correct = 0;
        for (String esResource : esResources) {
            job.set("es.resource", esResource);
            Settings settings = HadoopSettingsManager.loadFrom(job);

            if (settings.getResourceRead().equals(esResource)) correct++;

            //This is a wrong way to use it and may cause resourceRead value to be wrong.
            settings.setResourceRead(settings.getResourceRead());
        }
        assertEquals(correct, 1);

        assertNotEquals(esResources.size(), correct);
    }
}
