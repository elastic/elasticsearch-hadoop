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
package org.elasticsearch.hadoop.serialization.field;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.xml.bind.DatatypeConverter;

import org.elasticsearch.hadoop.util.StringUtils;


public class DateIndexFormatter implements IndexFormatter {

    private String format;
    private SimpleDateFormat dateFormat;

    @Override
    public void configure(String format) {
        this.format = format;
        this.dateFormat = new SimpleDateFormat(format);
    }

    @Override
    public String format(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }

        Calendar calendar = DatatypeConverter.parseDateTime(value);
        return dateFormat.format(calendar.getTime());
    }
}
