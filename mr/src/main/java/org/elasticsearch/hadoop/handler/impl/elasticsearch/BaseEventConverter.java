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
import java.util.Calendar;
import javax.xml.bind.DatatypeConverter;

import org.elasticsearch.hadoop.handler.Exceptional;
import org.elasticsearch.hadoop.rest.EsHadoopRemoteException;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.ecs.ElasticCommonSchema;
import org.elasticsearch.hadoop.util.ecs.MessageTemplate;

/**
 * Base implementation for converting failure events into ECS documents
 */
public abstract class BaseEventConverter<E extends Exceptional> implements EventConverter<E> {

    private final String eventType;
    private final String eventMessage;

    public BaseEventConverter(String eventType, String eventMessage) {
        this.eventType = eventType;
        this.eventMessage = eventMessage;
    }

    @Override
    public ElasticCommonSchema.TemplateBuilder configureTemplate(ElasticCommonSchema.TemplateBuilder templateBuilder) {
        return templateBuilder.setEventType(eventType);
    }

    @Override
    public BytesArray generateEvent(E event, MessageTemplate template) throws IOException {
        // Generate timestamp
        String timestamp = getTimestamp(event);
        // Generate message
        String eventMessage = renderEventMessage(event);
        // Exception type extraction
        String exceptionType = renderExceptionType(event);
        // Exception message
        String exceptionMessage = renderExceptionMessage(event);
        // String raw event
        String rawEvent = getRawEvent(event);
        return template.generateMessage(timestamp, eventMessage, exceptionType, exceptionMessage, rawEvent);
    }

    /**
     * Visible for testing
     */
    public String getTimestamp(E event) {
        long millis = System.currentTimeMillis();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        return DatatypeConverter.printDateTime(cal);
    }

    /**
     * Visible for testing
     */
    public String renderEventMessage(E event) {
        return eventMessage;
    }

    /**
     * Visible for testing
     */
    public String renderExceptionType(E event) {
        Exception exception = event.getException();

        if (exception instanceof EsHadoopRemoteException) {
            EsHadoopRemoteException remoteException = ((EsHadoopRemoteException) exception);
            return remoteException.getType();
        } else {
            String classText = exception.getClass().getSimpleName();
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            for (char c : classText.toCharArray()) {
                if (Character.isUpperCase(c)) {
                    if (first) {
                        first = false;
                    } else {
                        builder.append("_");
                    }
                    builder.append(Character.toLowerCase(c));
                } else {
                    builder.append(c);
                }
            }
            return builder.toString();
        }
    }

    /**
     * Visible for testing
     */
    public String renderExceptionMessage(E event) {
        return event.getException().getMessage();
    }

    /**
     * Visible for testing
     */
    public abstract String getRawEvent(E event) throws IOException;
}
