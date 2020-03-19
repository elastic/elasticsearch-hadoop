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
import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.mr.WritableBytesConverter;
import org.elasticsearch.hadoop.mr.WritableValueWriter;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.serialization.JsonUtils;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.MapWritableFieldExtractor;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
        testSettings.setInternalClusterInfo(TestUtils.getEsClusterInfo());
        testSettings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_VALUE_CLASS, JdkValueWriter.class.getName());
        RestRepository restRepo = new RestRepository(testSettings);
        RestClient client = restRepo.getRestClient();
        client.bulk(new Resource(testSettings, false), new TrackingBytesArray(new BytesArray("{}")));
        restRepo.waitForYellow();
        restRepo.close();
        client.close();
    }

    @Test
    public void testRepositoryDelete() throws Exception {
        Settings settings = new TestSettings("rest/deletebulk");
        RestUtils.delete("rest");
        InitializationUtils.discoverClusterInfo(settings, LOG);
        settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_VALUE_CLASS, JdkValueWriter.class.getName());
        settings.setProperty(ConfigurationOptions.ES_MAPPING_DEFAULT_EXTRACTOR_CLASS, ConstantFieldExtractor.class.getName());
        settings.setProperty(ConfigurationOptions.ES_BATCH_FLUSH_MANUAL, "false");
        settings.setProperty(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES, "1000");
        settings.setProperty(ConfigurationOptions.ES_BATCH_SIZE_BYTES, "1mb");

        RestRepository repository = new RestRepository(settings);

        String id = "Tést\tData 5";
        String doc = "{\"index\":{\"_id\":\"" + StringUtils.jsonEncoding(id) + "\"}}\n{\"field\":1}\n";
        repository.writeProcessedToIndex(new BytesArray(doc));
        repository.flush();
        RestUtils.refresh("rest");

        assertThat(JsonUtils.query("hits").get("total").apply(JsonUtils.asMap(RestUtils.get("rest/deletebulk/_search"))), is(equalTo(1)));

        repository.delete();

        assertThat(JsonUtils.query("hits").get("total").apply(JsonUtils.asMap(RestUtils.get("rest/deletebulk/_search"))), is(equalTo(0)));
    }

    @Test
    public void testRepositoryDeleteEmptyIndex() throws Exception {
        Settings settings = new TestSettings("delete_empty/test");
        RestUtils.delete("delete_empty");
        InitializationUtils.discoverClusterInfo(settings, LOG);
        settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_VALUE_CLASS, JdkValueWriter.class.getName());
        settings.setProperty(ConfigurationOptions.ES_MAPPING_DEFAULT_EXTRACTOR_CLASS, ConstantFieldExtractor.class.getName());
        settings.setProperty(ConfigurationOptions.ES_BATCH_FLUSH_MANUAL, "false");
        settings.setProperty(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES, "1000");
        settings.setProperty(ConfigurationOptions.ES_BATCH_SIZE_BYTES, "1mb");

        RestRepository repository = new RestRepository(settings);
        repository.touch();

        assertThat(JsonUtils.query("hits").get("total").apply(JsonUtils.asMap(RestUtils.get("delete_empty/test/_search"))), is(equalTo(0)));

        repository.delete();

        assertThat(JsonUtils.query("hits").get("total").apply(JsonUtils.asMap(RestUtils.get("delete_empty/test/_search"))), is(equalTo(0)));
    }


    @BeforeClass
    public static void createAliasTestIndices() throws Exception {
        if (!RestUtils.exists("alias_index1")) {
            RestUtils.put("alias_index1", ("{" +
                        "\"settings\": {" +
                            "\"number_of_shards\": 3," +
                            "\"number_of_replicas\": 0" +
                        "}" +
                    "}'").getBytes());
        }

        if (!RestUtils.exists("alias_index2")) {
            RestUtils.put("alias_index2", ("{" +
                        "\"settings\": {" +
                            "\"number_of_shards\": 3," +
                            "\"number_of_replicas\": 0" +
                        "}" +
                    "}'").getBytes());
        }
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

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testCreatePartitionWriterWithMultipleIndices() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_RESOURCE, "alias_index1,alias_index2/doc");
        InitializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, LOG);
        InitializationUtils.setBytesConverterIfNeeded(settings, WritableBytesConverter.class, LOG);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapWritableFieldExtractor.class, LOG);
        RestService.createWriter(settings, 1, 3, LOG);
        fail("Cannot write to multiple indices.");
    }

    @Test
    public void testCreatePartitionWriterWithAliasUsingSingleIndex() throws Exception {
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
    public void testCreatePartitionWriterWithAliasUsingMultipleIndices() throws Exception {
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
        fail("Should not be able to read data from multi_alias run");
    }

    @Test
    public void testCreatePartitionWriterWithWritableAliasUsingMultipleIndices() throws Exception {
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

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testCreatePartitionWriterWithMultipleAliases() throws Exception {
        RestUtils.postData("_aliases", ("{" +
                    "\"actions\": [" +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index1\"," +
                                "\"alias\": \"more_aliases_1\"," +
                                "\"is_write_index\": true" +
                            "}" +
                        "}," +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index2\"," +
                                "\"alias\": \"more_aliases_1\"" +
                            "}" +
                        "}," +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index1\"," +
                                "\"alias\": \"more_aliases_2\"" +
                            "}" +
                        "}," +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index2\"," +
                                "\"alias\": \"more_aliases_2\"" +
                            "}" +
                        "}" +
                    "]" +
                "}").getBytes());

        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_RESOURCE, "more_aliases_1,more_aliases_2/doc");
        InitializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, LOG);
        InitializationUtils.setBytesConverterIfNeeded(settings, WritableBytesConverter.class, LOG);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapWritableFieldExtractor.class, LOG);
        RestService.createWriter(settings, 1, 3, LOG);
        fail("Multiple alias names are not supported. Only singular aliases.");
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testCreatePartitionWriterWithMultipleWritableAliases() throws Exception {
        RestUtils.postData("_aliases", ("{" +
                    "\"actions\": [" +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index1\"," +
                                "\"alias\": \"more_write_aliases_1\"," +
                                "\"is_write_index\": true" +
                            "}" +
                        "}," +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index2\"," +
                                "\"alias\": \"more_write_aliases_1\"" +
                            "}" +
                        "}," +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index1\"," +
                                "\"alias\": \"more_write_aliases_2\"," +
                                "\"is_write_index\": true" +
                            "}" +
                        "}," +
                        "{" +
                            "\"add\": {" +
                                "\"index\": \"alias_index2\"," +
                                "\"alias\": \"more_write_aliases_2\"" +
                            "}" +
                        "}" +
                    "]" +
                "}").getBytes());

        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_RESOURCE, "more_aliases_1,more_aliases_2/doc");
        InitializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, LOG);
        InitializationUtils.setBytesConverterIfNeeded(settings, WritableBytesConverter.class, LOG);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapWritableFieldExtractor.class, LOG);
        RestService.createWriter(settings, 1, 3, LOG);
        fail("Even if the aliases are writable, they should fail since we only accept singular aliases.");
    }
}
