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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.SearchRequestBuilder;
import org.elasticsearch.hadoop.rest.query.QueryUtils;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.ScrollQuery;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReaderConfigBuilder;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingSet;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 */
public class AbstractRestQueryTest {
    private static Log log = LogFactory.getLog(AbstractRestQueryTest.class);
    private RestRepository client;
    private Settings settings;
    private EsMajorVersion version;

    @Before
    public void start() throws IOException {
        version = TestUtils.getEsClusterInfo().getMajorVersion();
        settings = new TestSettings("rest/savebulk");
        settings.setInternalVersion(version);
        //testSettings.setPort(9200)
        settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_VALUE_CLASS, JdkValueWriter.class.getName());
        settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_VALUE_CLASS, JdkValueWriter.class.getName());
        client = new RestRepository(settings);
        client.waitForYellow();
    }

    @After
    public void stop() throws Exception {
        client.close();
    }

    @Test
    public void testShardInfo() throws Exception {
        List<List<Map<String, Object>>> shards = client.getReadTargetShards();
        System.out.println(shards);
        assertNotNull(shards);
    }

    @Test
    public void testQueryBuilder() throws Exception {
        Settings sets = settings.copy();
        sets.setProperty(ConfigurationOptions.ES_QUERY, "?q=me*");
        sets.setInternalVersion(version);
        Resource read = new Resource(settings, true);
        SearchRequestBuilder qb =
                new SearchRequestBuilder(version, settings.getReadMetadata() && settings.getReadMetadataVersion())
                        .resource(read)
                        .query(QueryUtils.parseQuery(settings))
                        .scroll(settings.getScrollKeepAlive())
                        .size(settings.getScrollSize())
                        .limit(settings.getScrollLimit())
                        .fields(SettingsUtils.determineSourceFields(settings))
                        .filters(QueryUtils.parseFilters(settings));
        MappingSet mappingSet = client.getMappings();

        ScrollReaderConfigBuilder scrollCfg = ScrollReaderConfigBuilder.builder(new JdkValueReader(), settings)
                .setResolvedMapping(mappingSet.getResolvedView())
                .setReadMetadata(true)
                .setMetadataName("_metadata")
                .setReturnRawJson(false)
                .setIgnoreUnmappedFields(false)
                .setIncludeFields(Collections.<String>emptyList())
                .setExcludeFields(Collections.<String>emptyList())
                .setIncludeArrayFields(Collections.<String>emptyList());
        ScrollReader reader = new ScrollReader(scrollCfg);

        int count = 0;
        for (ScrollQuery query = qb.build(client, reader); query.hasNext();) {
            Object[] next = query.next();
            assertNotNull(next);
            count++;
        }

        assertTrue(count > 0);
    }
}