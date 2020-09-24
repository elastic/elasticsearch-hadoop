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
package org.elasticsearch.hadoop.util;

import java.io.IOException;
import java.util.Properties;

import org.elasticsearch.hadoop.cfg.PropertiesSettings;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE;

/**
 * Tweaked settings for testing.
 */
public class TestSettings extends PropertiesSettings {

    public final static Properties TESTING_PROPS = new Properties();

    static {
        // pick up system properties
        TESTING_PROPS.putAll(System.getProperties());

        // override with test settings
        try {
            TESTING_PROPS.load(TestUtils.class.getResourceAsStream("/test.properties"));
        } catch (IOException e) {
            throw new RuntimeException("Cannot load default Hadoop test properties");
        }

        // manually select the hadoop properties
        String fs = System.getProperty("hd.fs");
        String jt = System.getProperty("hd.jt");

        // override
        if (StringUtils.hasText(fs)) {
            System.out.println("Setting FS to " + fs);
            TESTING_PROPS.put("fs.default.name", fs.trim());
        }

        if (StringUtils.hasText(jt)) {
            System.out.println("Setting JT to " + jt);
            TESTING_PROPS.put("mapred.job.tracker", jt.trim());
        }
    }

    {
        // pick up dedicated ES port if present
        String embeddedEsLocalPort = System.getProperty(TestUtils.ES_LOCAL_PORT);
        if (StringUtils.hasText(System.getProperty(TestUtils.ES_LOCAL_PORT))) {
            TESTING_PROPS.put("es.port", embeddedEsLocalPort);
        }
    }

    public TestSettings() {
        super(new Properties());
        getProperties().putAll(TESTING_PROPS);
    }

    public TestSettings(String uri) {
        this();
        setProperty(ES_RESOURCE, uri);
    }

    public Properties getProperties() {
        return props;
    }
}
