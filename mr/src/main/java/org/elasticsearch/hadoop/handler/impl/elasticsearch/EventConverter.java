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

package org.elasticsearch.hadoop.handler.impl.elasticsearch;

import java.io.IOException;

import org.elasticsearch.hadoop.handler.Exceptional;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.ecs.ElasticCommonSchema;
import org.elasticsearch.hadoop.util.ecs.MessageTemplate;

/**
 * Given a failure event that extends Exceptional, provide a mechanism for transforming its fields into an ECS document.
 */
public interface EventConverter<E extends Exceptional> {

    /**
     * Perform some logic against the template builder in order to inject values before the template is built.
     */
    ElasticCommonSchema.TemplateBuilder configureTemplate(ElasticCommonSchema.TemplateBuilder templateBuilder);

    /**
     * Converts an event into an ECS compliant document using the given message template.
     * @param event Exceptional event to convert
     * @param template template to use for conversion
     * @return BytesArray containing the document data
     */
    BytesArray generateEvent(E event, MessageTemplate template) throws IOException;
}
