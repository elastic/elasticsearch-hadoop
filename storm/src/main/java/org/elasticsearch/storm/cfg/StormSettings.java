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
package org.elasticsearch.storm.cfg;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.Booleans;

import static org.elasticsearch.storm.cfg.StormConfigurationOptions.*;

public class StormSettings extends Settings {

    private final Map<Object, Object> cfg;

    public StormSettings(Map<?, ?> settings) {
        // the Storm APersistentMap is read-only so make a copy
        this.cfg = new LinkedHashMap<Object, Object>(settings);
    }

    public boolean getStormTickTupleFlush() {
        return Booleans.parseBoolean(getProperty(ES_STORM_BOLT_TICK_TUPLE_FLUSH, ES_STORM_BOLT_TICK_TUPLE_FLUSH_DEFAULT));
    }

    public boolean getStormBoltAck() {
        return Booleans.parseBoolean(getProperty(ES_STORM_BOLT_ACK, ES_STORM_BOLT_ACK_DEFAULT));
    }

    public int getStormBulkSize() {
        String value = getProperty(ES_STORM_BOLT_FLUSH_ENTRIES_SIZE);
        if (StringUtils.hasText(value)) {
            return Integer.valueOf(value);
        }
        return getBatchSizeInEntries();
    }

    public boolean getStormSpoutReliable() {
        return Booleans.parseBoolean(getProperty(ES_STORM_SPOUT_RELIABLE, ES_STORM_SPOUT_RELIABLE_DEFAULT));
    }

    public int getStormSpoutReliableQueueSize() {
        return Integer.parseInt(getProperty(ES_STORM_SPOUT_RELIABLE_QUEUE_SIZE, ES_STORM_SPOUT_RELIABLE_QUEUE_SIZE_DEFAULT));
    }

    public int getStormSpoutReliableRetriesPerTuple() {
        return Integer.parseInt(getProperty(ES_STORM_SPOUT_RELIABLE_RETRIES_PER_TUPLE, ES_STORM_SPOUT_RELIABLE_RETRIES_PER_TUPLE_DEFAULT));
    }

    public TupleFailureHandling getStormSpoutReliableTupleFailureHandling() {
        return TupleFailureHandling.valueOf(getProperty(ES_STORM_SPOUT_RELIABLE_TUPLE_FAILURE_HANDLE, ES_STORM_SPOUT_RELIABLE_TUPLE_FAILURE_HANDLE_DEFAULT).toUpperCase(Locale.ENGLISH));
    }

    public List<String> getStormSpoutFields() {
        return StringUtils.tokenize(getProperty(ES_STORM_SPOUT_FIELDS, ES_STORM_SPOUT_FIELDS_DEFAULT));
    }

    @Override
    public InputStream loadResource(String location) {
        return IOUtils.open(location);
    }

    @Override
    public Settings copy() {
        return new StormSettings(new LinkedHashMap<Object, Object>(cfg));
    }

    @Override
    public String getProperty(String name) {
        Object value = cfg.get(name);
        return (value != null ? value.toString() : null);

    }

    @Override
    public void setProperty(String name, String value) {
        cfg.put(name, value);
    }

    @Override
    public Properties asProperties() {
        Properties props = new Properties();

        if (cfg != null) {
            for (Entry<Object, Object> entry : props.entrySet()) {
                if (entry.getKey() instanceof String) {
                    props.put(entry.getKey().toString(), entry.getValue().toString());
                }
            }
        }
        return props;
    }
}
