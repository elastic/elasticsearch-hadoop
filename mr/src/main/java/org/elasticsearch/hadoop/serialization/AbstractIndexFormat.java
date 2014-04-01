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
package org.elasticsearch.hadoop.serialization;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ObjectUtils;



public abstract class AbstractIndexFormat implements IndexFormat, SettingsAware {

    protected Settings settings;
    protected String pattern;
    protected boolean hasPattern = false;
    protected List<Object> index;
    protected List<Object> type;

    @Override
    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    @Override
    public void compile(String pattern) {
        this.pattern = pattern;
        // break it down into index/type
        String[] split = pattern.split("/");
        Assert.isTrue(!ObjectUtils.isEmpty(split), "invalid pattern given " + pattern);
        Assert.isTrue(split.length == 2, "invalid pattern given " + pattern);

        // check pattern
        hasPattern = pattern.contains("{") && pattern.contains("}");
        index = parse(split[0].trim());
        type = parse(split[1].trim());
    }

    protected List<Object> parse(String string) {
        // break it down into fields
        List<Object> template = new ArrayList<Object>();
        while (string.contains("{")) {
            int startPattern = string.indexOf("{");
            template.add(string.substring(0, startPattern));
            int endPattern = string.indexOf("}");
            Assert.isTrue(endPattern > startPattern + 1, "Invalid pattern given " + string);
            template.add(createFieldExtractor(string.substring(startPattern + 1, endPattern)));
            string = string.substring(endPattern + 1).trim();
        }
        template.add(string);
        return template;
    }

    private void append(StringBuilder sb, List<Object> list, Object target) {
        for (Object object : list) {
            if (object instanceof FieldExtractor) {
                String field = ((FieldExtractor) object).field(target);
                if (field == null) {
                    throw new EsHadoopIllegalArgumentException(String.format("Cannot find match for %s", pattern));
                }
                else {
                    sb.append(field);
                }
            }
            else {
                sb.append(object.toString());
            }
        }
    }

    @Override
    public String field(Object target) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"_index\":\"");
        append(sb, index, target);
        sb.append("\",");
        sb.append("\"_type\":\"");
        append(sb, type, target);
        sb.append("\"");

        return sb.toString();
    }

    @Override
    public boolean hasPattern() {
        return hasPattern;
    }

    protected abstract Object createFieldExtractor(String fieldName);
}