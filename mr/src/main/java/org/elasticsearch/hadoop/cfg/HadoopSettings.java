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
package org.elasticsearch.hadoop.cfg;

import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.HadoopIOUtils;
import org.elasticsearch.hadoop.util.Assert;

public class HadoopSettings extends Settings {

    private final Configuration cfg;

    public HadoopSettings(Configuration cfg) {
        Assert.notNull(cfg, "Non-null properties expected");
        this.cfg = cfg;
    }

    @Override
    public String getProperty(String name) {
        return cfg.get(name);
    }

    @Override
    public void setProperty(String name, String value) {
        cfg.set(name, value);
    }

    @Override
    public Settings copy() {
        // force internal init
        cfg.size();
        return new HadoopSettings(new Configuration(cfg));
    }

    @Override
    public InputStream loadResource(String location) {
        return HadoopIOUtils.open(location, cfg);
    }

    @Override
    public Properties asProperties() {
        return HadoopCfgUtils.asProperties(cfg);
    }
}
