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
package org.elasticsearch.spark.cfg;

import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.IOUtils;

import scala.Option;
import scala.Tuple2;

public class SparkSettings extends Settings {

    private final SparkConf cfg;

    public SparkSettings(SparkConf cfg) {
        Assert.notNull(cfg, "non-null spark configuration expected");
        this.cfg = cfg;
    }

    @Override
    public InputStream loadResource(String location) {
        // delegate to good ol' TCCL
        return IOUtils.open(location);
    }

    @Override
    public Settings copy() {
        return new SparkSettings(cfg.clone());
    }

    @Override
    public String getProperty(String name) {
        Option<String> op = cfg.getOption(name);
        if (!op.isDefined()) {
            op = cfg.getOption("spark." + name);
        }
        return (op.isDefined() ? op.get() : null);
    }

    @Override
    public void setProperty(String name, String value) {
        cfg.set(name, value);
    }

    @Override
    public Properties asProperties() {
        Properties props = new Properties();

        if (cfg != null) {
            for (Tuple2<String, String> tuple : cfg.getAll()) {
                props.setProperty(tuple._1, tuple._2);
            }
        }

        return props;
    }
}
