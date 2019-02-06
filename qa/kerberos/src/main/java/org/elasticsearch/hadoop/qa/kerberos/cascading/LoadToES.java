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

package org.elasticsearch.hadoop.qa.kerberos.cascading;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Dfs;
import cascading.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.cascading.EsTap;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.qa.kerberos.security.KeytabLogin;
import org.elasticsearch.hadoop.util.StringUtils;

public class LoadToES extends Configured implements Tool {

    public static final String CONF_FIELD_NAMES = "load.field.names";

    public static void main(final String[] args) throws Exception {
        KeytabLogin.doAfterLogin(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                System.exit(ToolRunner.run(new LoadToES(), args));
                return null;
            }
        });
    }

    @Override
    public int run(String[] args) throws Exception {
        String rawFields = getConf().get(CONF_FIELD_NAMES);
        List<String> fieldList = StringUtils.tokenize(rawFields);
        String[] fields = fieldList.toArray(new String[0]);

        String resource = HadoopSettingsManager.loadFrom(getConf()).getResourceWrite();

        Properties properties = asProperties(getConf());

        EsTap.initCredentials(properties);

        AppProps.setApplicationJarClass(properties, ReadFromES.class);

        Tap in = new Dfs(new TextDelimited(new Fields(fields)), args[0]);
        Tap out = new EsTap(resource, new Fields(fields));

        FlowConnector flow = new HadoopFlowConnector(properties);

        Pipe pipe = new Pipe("write-to-es");

        FlowDef flowDef = FlowDef.flowDef().addSource(pipe, in).addTailSink(pipe, out);
        flow.connect(flowDef).complete();

        return 0;
    }

    public static Properties asProperties(Configuration cfg) {
        Properties props = new Properties();

        if (cfg != null) {
            for (Map.Entry<String, String> entry : cfg) {
                props.setProperty(entry.getKey(), entry.getValue());
            }
        }

        return props;
    }
}
