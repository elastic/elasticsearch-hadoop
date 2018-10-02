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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.mr.WritableBytesConverter;
import org.elasticsearch.hadoop.mr.WritableValueWriter;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.serialization.field.MapWritableFieldExtractor;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AbstractRestSaveTest {

    private static final Log LOG = LogFactory.getLog(AbstractRestSaveTest.class);

    @Test
    public void testBulkWrite() throws Exception {
        TestSettings testSettings = new TestSettings("rest/savebulk");
        //testSettings.setPort(9200)
        testSettings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_VALUE_CLASS, JdkValueWriter.class.getName());
        RestRepository client = new RestRepository(testSettings);

        Scanner in = new Scanner(getClass().getResourceAsStream("/artists.dat")).useDelimiter("\\n|\\t");

        Map<String, String> line = new LinkedHashMap<String, String>();

        for (; in.hasNextLine();) {
            // ignore number
            in.next();
            line.put("name", in.next());
            line.put("url", in.next());
            line.put("picture", in.next());
            client.writeToIndex(line);
            line.clear();
        }

        client.close();
    }

    @Test
    public void testEmptyBulkWrite() throws Exception {
        TestSettings testSettings = new TestSettings("rest/emptybulk");
        testSettings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_VALUE_CLASS, JdkValueWriter.class.getName());
        RestRepository restRepo = new RestRepository(testSettings);
        RestClient client = restRepo.getRestClient();
        client.bulk(new Resource(testSettings, false), new TrackingBytesArray(new BytesArray("{}")));
        restRepo.waitForYellow();
        restRepo.close();
        client.close();
    }

    @BeforeClass
    public static void createAliasTestIndices() throws Exception {
        RestUtils.put("alias_index1", ("{" +
                    "\"settings\": {" +
                        "\"number_of_shards\": 3," +
                        "\"number_of_replicas\": 0" +
                    "}" +
                "}'").getBytes());

        RestUtils.put("alias_index2", ("{" +
                    "\"settings\": {" +
                        "\"number_of_shards\": 3," +
                        "\"number_of_replicas\": 0" +
                    "}" +
                "}'").getBytes());
    }

    @Test
    public void testCreatePartitionWriterWithPatternedIndex() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_RESOURCE, "alias_index{id}/doc");
        InitializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, LOG);
        InitializationUtils.setBytesConverterIfNeeded(settings, WritableBytesConverter.class, LOG);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapWritableFieldExtractor.class, LOG);
        RestService.PartitionWriter writer = RestService.createWriter(settings, 1, 3, LOG);
        writer.close();
    }

    @Test
    public void testCreatePartitionWriterWithSingleIndex() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_RESOURCE, "alias_index1/doc");
        InitializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, LOG);
        InitializationUtils.setBytesConverterIfNeeded(settings, WritableBytesConverter.class, LOG);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapWritableFieldExtractor.class, LOG);
        RestService.PartitionWriter writer = RestService.createWriter(settings, 1, 3, LOG);
        writer.close();
    }

    @Test
    public void testCreatePartitionWriterWithSingleAlias() throws Exception {
        RestUtils.postData("_aliases", ("{" +
                    "\"actions\": [" +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index1\"," +
                                "\"alias\": \"single_alias\"" +
                            "}" +
                        "}" +
                    "]" +
                "}").getBytes());

        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_RESOURCE, "single_alias/doc");
        InitializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, LOG);
        InitializationUtils.setBytesConverterIfNeeded(settings, WritableBytesConverter.class, LOG);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapWritableFieldExtractor.class, LOG);
        RestService.PartitionWriter writer = RestService.createWriter(settings, 1, 3, LOG);
        writer.close();
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testCreatePartitionWriterWithMultipleAliases() throws Exception {
        RestUtils.postData("_aliases", ("{" +
                    "\"actions\": [" +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index1\"," +
                                "\"alias\": \"multi_alias\"" +
                            "}" +
                        "}," +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index2\"," +
                                "\"alias\": \"multi_alias\"" +
                            "}" +
                        "}" +
                    "]" +
                "}").getBytes());

        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_RESOURCE, "multi_alias/doc");
        InitializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, LOG);
        InitializationUtils.setBytesConverterIfNeeded(settings, WritableBytesConverter.class, LOG);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapWritableFieldExtractor.class, LOG);
        RestService.createWriter(settings, 1, 3, LOG);
        Assert.fail("Should not be able to read data from multi_alias run");
    }

    @Test
    public void testCreatePartitionWriterWithWritableMultipleAliases() throws Exception {
        RestUtils.postData("_aliases", ("{" +
                    "\"actions\": [" +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index1\"," +
                                "\"alias\": \"multi_alias_writable\"," +
                                "\"is_write_index\": true" +
                            "}" +
                        "}," +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index2\"," +
                                "\"alias\": \"multi_alias_writable\"" +
                            "}" +
                        "}" +
                    "]" +
                "}").getBytes());

        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_RESOURCE, "multi_alias_writable/doc");
        InitializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, LOG);
        InitializationUtils.setBytesConverterIfNeeded(settings, WritableBytesConverter.class, LOG);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapWritableFieldExtractor.class, LOG);
        RestService.PartitionWriter writer = RestService.createWriter(settings, 1, 3, LOG);
        writer.close();
    }
}
