/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.hive;

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
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.mr.ESOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.serialization.SerializationUtils;
import org.elasticsearch.hadoop.util.Assert;

import static org.elasticsearch.hadoop.hive.HiveConstants.*;

/**
 * Hive storage for writing data into an ElasticSearch index.
 *
 * The ElasticSearch host/port can be specified through Hadoop properties (see package description)
 * or passed to {@link #ESStorageHandler} through Hive <tt>TBLPROPERTIES</tt>
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class ESStorageHandler extends DefaultStorageHandler {

    private static Log log = LogFactory.getLog(ESStorageHandler.class);

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return ESHiveInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return ESHiveOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return ESSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        //TODO: add metahook support
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        init(tableDesc);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        init(tableDesc);
    }

    private void init(TableDesc tableDesc) {
        Configuration cfg = getConf();
        Settings settings = SettingsManager.loadFrom(cfg).merge(tableDesc.getProperties()).clean();

        // NB: ESSerDe is already initialized at this stage
        // NB: the value writer is not needed by Hive but it's set for consistency and debugging purposes

        InitializationUtils.checkIdForOperation(settings);

        SerializationUtils.setValueWriterIfNotSet(settings, HiveValueWriter.class, log);
        SerializationUtils.setValueReaderIfNotSet(settings, HiveValueReader.class, log);
        InitializationUtils.setFieldExtractorIfNotSet(settings, HiveFieldExtractor.class, log);

        settings.save();

        // replace the default committer when using the old API
        HadoopCfgUtils.setOutputCommitterClass(cfg, ESOutputFormat.ESOutputCommitter.class.getName());

        Assert.hasText(tableDesc.getProperties().getProperty(TABLE_LOCATION), String.format(
                "no table location [%s] declared by Hive resulting in abnormal execution;", TABLE_LOCATION));
    }

    @Override
    @Deprecated
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        throw new UnsupportedOperationException();
    }
}