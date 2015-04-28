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
package org.elasticsearch.hadoop.yarn.util;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.Properties;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

public abstract class PropertiesUtils {

    public static Properties load(Class<?> clazz, String cfgName) {
        try {
            InputStream stream = clazz.getResourceAsStream(cfgName);
            Properties props = new Properties();
            props.load(stream);
            return props;
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot find/load file " + cfgName, ex);
        }
    }

    public static Properties merge(Properties target, Properties other) {
        if (target == null) {
            target = new Properties();
        }

        if (other == null || other.isEmpty()) {
            return target;
        }

        Set<String> propertyNames = other.stringPropertyNames();

        for (String prop : propertyNames) {
            target.setProperty(prop, other.getProperty(prop));
        }

        return target;
    }

    public static Properties fromCmdLine(String[] args, int offset) {
        Properties prop = new Properties();

        String loadCfg = "loadConfig";

        for (int i = offset; i < args.length; i++) {
            String[] strings = args[i].split("=");
            if (strings.length != 2) {
                throw new IllegalArgumentException(String.format("Invalid argument %s", args[i]));
            }
            if (loadCfg.equals(strings[0])) {
                merge(prop, load(strings[1]));
            }
            prop.setProperty(strings[0], strings[1]);
        }
        return prop;
    }

    private static Properties load(String string) {
        Properties prop = new Properties();
        try {
            InputStream in = (string.contains(":") ? new URL(string).openStream() : new FileInputStream(string));
            prop.load(in);
        } catch (IOException ex) {
            throw new IllegalArgumentException(String.format("Cannot open source %s", string), ex);
        }
        return prop;
    }

    public static String propsToBase64(Properties props) {
        StringWriter sw = new StringWriter();
        if (props != null) {
            try {
                props.store(sw, "");
            } catch (IOException ex) {
                throw new IllegalArgumentException(ex);
            }
        }
        return DatatypeConverter.printBase64Binary(sw.toString().getBytes(StringUtils.UTF_8));
    }

    public static Properties propsFromBase64String(String source) {
        Properties copy = new Properties();
        if (source != null) {
            try {
                copy.load(new InputStreamReader(new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(source)), StringUtils.UTF_8));
            } catch (IOException ex) {
                throw new IllegalArgumentException(ex);
            }
        }
        return copy;
    }
}