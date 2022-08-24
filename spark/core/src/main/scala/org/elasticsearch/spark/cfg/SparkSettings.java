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

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.IOUtils;

import org.elasticsearch.hadoop.util.Version;
import scala.Option;
import scala.Tuple2;

public class SparkSettings extends Settings {

    private final SparkConf cfg;

    public SparkSettings(SparkConf cfg) {
        Assert.notNull(cfg, "non-null spark configuration expected");
        this.cfg = cfg;
        String user;
        try {
            user = System.getenv("SPARK_USER") == null ?
                    UserGroupInformation.getCurrentUser().getShortUserName() :
                    System.getenv("SPARK_USER");
        } catch (IOException e) {
            user = "";
        }
        String appName = cfg.get("app.name", cfg.get("spark.app.name", ""));
        String appId = cfg.get("spark.app.id", "");
        String opaqueId = String.format(Locale.ROOT, "[spark] [%s] [%s] [%s]", user, appName, appId);
        this.setOpaqueId(opaqueId);
        this.setUserAgent(String.format(
                        Locale.ROOT,
                        "elasticsearch-hadoop/%s spark/%s",
                        Version.versionNumber(),
                        org.apache.spark.package$.MODULE$.SPARK_VERSION()
                )
        );
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
        	String sparkPrefix = "spark.";
            for (Tuple2<String, String> tuple : cfg.getAll()) {
                // spark. are special so save them without the prefix as well
            	// since its unlikely the other implementations will be aware of this convention
            	String key = tuple._1;
                props.setProperty(key, tuple._2);
                if (key.startsWith(sparkPrefix)) {
                	String simpleKey = key.substring(sparkPrefix.length());
                	// double check to not override a property defined directly in the config
                	if (!props.containsKey(simpleKey)) {
                		props.setProperty(simpleKey, tuple._2);
                	}
                }
            }
        }

        return props;
    }
}
