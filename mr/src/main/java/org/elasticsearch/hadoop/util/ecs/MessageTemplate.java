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

import java.util.Map;
import java.util.Set;

import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;

public class MessageTemplate {

    // Parent
    private ElasticCommonSchema schema;

    // Base
    private Map<String, String> labels;
    private Set<String> tags;

    // Host
    private HostData host;

    // Event
    private String eventCategory;
    private String eventType;

    public MessageTemplate(ElasticCommonSchema schema, Map<String, String> labels, Set<String> tags, HostData host,
                           String eventCategory, String eventType) {
        Assert.hasText(eventCategory, "Missing " + FieldNames.FIELD_EVENT_CATEGORY + " value for ECS template.");
        Assert.hasText(eventType, "Missing " + FieldNames.FIELD_EVENT_TYPE + " value for ECS template.");
        this.schema = schema;
        this.labels = labels;
        this.tags = tags;
        this.host = host;
        this.eventCategory = eventCategory;
        this.eventType = eventType;
    }

    public BytesArray generateMessage(String ts, String message, String exceptionType, String exceptionMessage, String rawEvent) {
        Assert.hasText(ts, "Missing " + FieldNames.FIELD_TIMESTAMP + " value for ECS template.");
        Assert.hasText(message, "Missing " + FieldNames.FIELD_MESSAGE + " value for ECS template.");
        Assert.hasText(exceptionType, "Missing " + FieldNames.FIELD_ERROR_CODE + " value for ECS template.");
        Assert.hasText(exceptionMessage, "Missing " + FieldNames.FIELD_ERROR_MESSAGE + " value for ECS template.");
        Assert.hasText(rawEvent, "Missing " + FieldNames.FIELD_EVENT_RAW + " value for ECS template.");

        FastByteArrayOutputStream outputStream = new FastByteArrayOutputStream();
        JacksonJsonGenerator generator = new JacksonJsonGenerator(outputStream);
        generator.usePrettyPrint();

        generator.writeBeginObject();
        {
            // TS
            generator.writeFieldName(FieldNames.FIELD_TIMESTAMP).writeString(ts);

            // Tags
            if (tags != null && !tags.isEmpty()) {
                generator.writeFieldName(FieldNames.FIELD_TAGS).writeBeginArray();
                for (String tag : tags) {
                    generator.writeString(tag);
                }
                generator.writeEndArray();
            }

            // Labels
            if (labels != null && !labels.isEmpty()) {
                generator.writeFieldName(FieldNames.FIELD_LABELS).writeBeginObject();
                for (Map.Entry<String, String> label : labels.entrySet()) {
                    generator.writeFieldName(label.getKey()).writeString(label.getValue());
                }
                generator.writeEndObject();
            }

            // Message
            generator.writeFieldName(FieldNames.FIELD_MESSAGE).writeString(message);

            // Host
            if (host != null && host.hasData()) {
                generator.writeFieldName(FieldNames.FIELD_HOST).writeBeginObject();
                {
                    // Host info
                    writeNullable(generator, FieldNames.FIELD_HOST_NAME, host.getName());
                    writeNullable(generator, FieldNames.FIELD_HOST_IP, host.getIp());
                    writeNullable(generator, FieldNames.FIELD_HOST_ARCHITECTURE, host.getArchitecture());

                    // OS block
                    if (host.getOsName() != null || host.getOsVersion() != null) {
                        generator.writeFieldName(FieldNames.FIELD_HOST_OS).writeBeginObject();
                        {
                            writeNullable(generator, FieldNames.FIELD_HOST_OS_NAME, host.getOsName());
                            writeNullable(generator, FieldNames.FIELD_HOST_OS_VERSION, host.getOsVersion());
                        }
                        generator.writeEndObject();
                    }

                    // TZ
                    if (host.getTimezoneOffsetSec() != null) {
                        generator.writeFieldName(FieldNames.FIELD_HOST_TIMEZONE).writeBeginObject();
                        {
                            generator.writeFieldName(FieldNames.FIELD_HOST_TIMEZONE_OFFSET).writeBeginObject();
                            {
                                generator.writeFieldName(FieldNames.FIELD_HOST_TIMEZONE_OFFSET_SEC)
                                        .writeNumber(host.getTimezoneOffsetSec());
                            }
                            generator.writeEndObject();
                        }
                        generator.writeEndObject();
                    }
                }
                generator.writeEndObject();
            }

            // Error
            generator.writeFieldName(FieldNames.FIELD_ERROR).writeBeginObject();
            {
                generator.writeFieldName(FieldNames.FIELD_ERROR_CODE).writeString(exceptionType);
                generator.writeFieldName(FieldNames.FIELD_ERROR_MESSAGE).writeString(exceptionMessage);
            }
            generator.writeEndObject();

            // Event
            generator.writeFieldName(FieldNames.FIELD_EVENT).writeBeginObject();
            {
                generator.writeFieldName(FieldNames.FIELD_EVENT_CATEGORY).writeString(eventCategory);
                generator.writeFieldName(FieldNames.FIELD_EVENT_TYPE).writeString(eventType);
                generator.writeFieldName(FieldNames.FIELD_EVENT_RAW).writeString(rawEvent);
                generator.writeFieldName(FieldNames.FIELD_EVENT_VERSION).writeString(schema.getVersion());
            }
            generator.writeEndObject();
        }
        generator.writeEndObject();

        generator.close();
        return outputStream.bytes();
    }

    private void writeNullable(Generator generator, String key, String value) {
        if (value != null) {
            generator.writeFieldName(key).writeString(value);
        }
    }
}
