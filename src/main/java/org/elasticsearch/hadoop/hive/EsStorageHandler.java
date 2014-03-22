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
package org.elasticsearch.hadoop.hive;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.util.Assert;

import static org.elasticsearch.hadoop.hive.HiveConstants.*;

/**
 * Hive storage for writing data into an ElasticSearch index.
 *
 * The ElasticSearch host/port can be specified through Hadoop properties (see package description)
 * or passed to {@link #EsStorageHandler} through Hive <tt>TBLPROPERTIES</tt>
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class EsStorageHandler extends DefaultStorageHandler {

    private static Log log = LogFactory.getLog(EsStorageHandler.class);

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return EsHiveInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return EsHiveOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return EsSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        //TODO: add metahook support
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        init(tableDesc, true);
        copyToJobProperties(jobProperties, tableDesc.getProperties());
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        init(tableDesc, false);
        copyToJobProperties(jobProperties, tableDesc.getProperties());
    }


    // NB: save the table properties in a special place but nothing else; otherwise the settings might trip on each other
    private void init(TableDesc tableDesc, boolean read) {
        Configuration cfg = getConf();
        // NB: we can't just merge the table properties in, we need to save them per input/output otherwise clashes occur which confuse Hive

        Settings settings = SettingsManager.loadFrom(cfg);
        //settings.setProperty((read ? HiveConstants.INPUT_TBL_PROPERTIES : HiveConstants.OUTPUT_TBL_PROPERTIES), IOUtils.propsToString(tableDesc.getProperties()));
        if (read) {
            // no generic setting
        }
        else {
            // replace the default committer when using the old API
            HadoopCfgUtils.setOutputCommitterClass(cfg, EsOutputFormat.EsOutputCommitter.class.getName());
        }

        Assert.hasText(tableDesc.getProperties().getProperty(TABLE_LOCATION), String.format(
                "no table location [%s] declared by Hive resulting in abnormal execution;", TABLE_LOCATION));
    }

    private void copyToJobProperties(Map<String, String> jobProperties, Properties properties) {
        for (String key : properties.stringPropertyNames()) {
            jobProperties.put(key, properties.getProperty(key));
        }
    }


    @Override
    @Deprecated
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        throw new UnsupportedOperationException();
    }
}