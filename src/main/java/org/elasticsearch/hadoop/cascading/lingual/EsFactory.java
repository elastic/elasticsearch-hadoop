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
package org.elasticsearch.hadoop.cascading.lingual;

import java.io.IOException;
import java.util.Properties;

import org.elasticsearch.hadoop.cascading.CascadingUtils;
import org.elasticsearch.hadoop.util.StringUtils;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

@SuppressWarnings("rawtypes")
public class EsFactory {

    public static class EsScheme extends Scheme {
        Fields fields;

        EsScheme(Fields fields) {
            this.fields = fields;
        }

        @Override
        public void sourceConfInit(FlowProcess flowProcess, Tap tap, Object conf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sinkConfInit(FlowProcess flowProcess, Tap tap, Object conf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean source(FlowProcess flowProcess, SourceCall sourceCall) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sink(FlowProcess flowProcess, SinkCall sinkCall) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    public Tap createTap(Scheme scheme, String path, SinkMode sinkMode, Properties properties) {
        if (!(scheme instanceof EsScheme)) {
            throw new IllegalArgumentException("Unknown scheme; expected " + EsScheme.class.getName());
        }

        String host = properties.getProperty("host");
        String portString = properties.getProperty("port");
        int port = (StringUtils.hasText(portString) ? Integer.parseInt(portString) : -1);
        String query = properties.getProperty("query");

        return CascadingUtils.hadoopTap(host, port, path, query, ((EsScheme) scheme).fields);
    }

    public Scheme createScheme(Fields fields, Properties properties) {
        return new EsScheme(fields);
    }
}
