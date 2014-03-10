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

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

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
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        init(tableDesc, false);
    }

    private void init(TableDesc tableDesc, boolean read) {
        Configuration cfg = getConf();
        Settings settings = SettingsManager.loadFrom(cfg).merge(tableDesc.getProperties());

        // NB: ESSerDe is already initialized at this stage but should still have a reference to the same cfg object
        // NB: the value writer is not needed by Hive but it's set for consistency and debugging purposes

        InitializationUtils.checkIdForOperation(settings);
        if (read) {
            InitializationUtils.setValueReaderIfNotSet(settings, HiveValueReader.class, log);
            settings.setProperty(InternalConfigurationOptions.INTERNAL_ES_TARGET_FIELDS, StringUtils.concatenate(HiveUtils.columnToAlias(settings), ","));
            // set read resource
            settings.setResourceRead(settings.getResourceRead());
        }
        else {
            InitializationUtils.setValueWriterIfNotSet(settings, HiveValueWriter.class, log);
            InitializationUtils.setBytesConverterIfNeeded(settings, HiveBytesConverter.class, log);
            // replace the default committer when using the old API
            HadoopCfgUtils.setOutputCommitterClass(cfg, EsOutputFormat.ESOutputCommitter.class.getName());
            // set write resource
            settings.setResourceWrite(settings.getResourceWrite());
        }

        InitializationUtils.setFieldExtractorIfNotSet(settings, HiveFieldExtractor.class, log);
        try {
            InitializationUtils.discoverEsVersion(settings, log);
        } catch (IOException ex) {
            throw new EsHadoopIllegalStateException("Cannot discover Elasticsearch version", ex);
        }


        Assert.hasText(tableDesc.getProperties().getProperty(TABLE_LOCATION), String.format(
                "no table location [%s] declared by Hive resulting in abnormal execution;", TABLE_LOCATION));
    }

    @Override
    @Deprecated
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        throw new UnsupportedOperationException();
    }
}