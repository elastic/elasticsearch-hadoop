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

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;



public abstract class AbstractIndexExtractor implements IndexExtractor, SettingsAware {

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
            String nestedString = string.substring(startPattern + 1, endPattern);
            int separator = nestedString.indexOf(":");
            if (separator > 0) {
                Assert.isTrue(nestedString.length() > separator + 1, "Invalid format given " + nestedString);
                String format = nestedString.substring(separator + 1);
                nestedString = nestedString.substring(0, separator);
                template.add(wrapWithFormatter(format, createFieldExtractor(nestedString)));
            }
            else {
                template.add(createFieldExtractor(nestedString));
            }
            string = string.substring(endPattern + 1).trim();
        }
        if (StringUtils.hasText(string)) {
            template.add(string);
        }
        return template;
    }

    private Object wrapWithFormatter(String format, final FieldExtractor createFieldExtractor) {
        // instantiate field extractor
        final IndexFormatter iformatter = ObjectUtils.instantiate(settings.getMappingIndexFormatterClassName(), settings);
        iformatter.configure(format);
        return new FieldExtractor() {
            @Override
            public String field(Object target) {
                return iformatter.format(createFieldExtractor.field(target));
            }
        };
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

    protected abstract FieldExtractor createFieldExtractor(String fieldName);
}