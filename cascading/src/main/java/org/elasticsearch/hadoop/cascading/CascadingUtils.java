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
package org.elasticsearch.hadoop.cascading;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.util.FieldAlias;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.ReflectionUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;

import cascading.scheme.SinkCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.hadoop.TupleSerializationProps;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;

public abstract class CascadingUtils {

    private static final String MAPPING_NAMES = "es.mapping.names";
    private static final boolean CASCADING_22_AVAILABLE = ObjectUtils.isClassPresent("cascading.tuple.type.CoercibleType", Tap.class.getClassLoader());

    static Settings addDefaultsToSettings(Properties flowProperties, Properties tapProperties, Log log) {
        Settings settings = HadoopSettingsManager.loadFrom(CascadingUtils.extractOriginalProperties(flowProperties)).merge(tapProperties);

        InitializationUtils.validateSettings(settings);

        InitializationUtils.setValueWriterIfNotSet(settings, CascadingValueWriter.class, log);
        InitializationUtils.setValueReaderIfNotSet(settings, JdkValueReader.class, log);
        InitializationUtils.setBytesConverterIfNeeded(settings, CascadingLocalBytesConverter.class, log);
        InitializationUtils.setFieldExtractorIfNotSet(settings, CascadingFieldExtractor.class, log);

        return settings;
    }

    static void initialDiscovery(Settings settings, Log log) {
        InitializationUtils.discoverEsVersion(settings, log);
        InitializationUtils.discoverNodesIfNeeded(settings, log);
        InitializationUtils.filterNonClientNodesIfNeeded(settings, log);
        InitializationUtils.filterNonDataNodesIfNeeded(settings, log);
        InitializationUtils.filterNonIngestNodesIfNeeded(settings, log);
    }

    static void finalValidation(Settings settings, boolean read) {
        if (read) {
            InitializationUtils.validateSettingsForReading(settings);
        } else {
            InitializationUtils.validateSettingsForWriting(settings);
        }
    }

    static void addSerializationToken(Object config) {
        Configuration cfg = (Configuration) config;
        String tokens = cfg.get(TupleSerializationProps.SERIALIZATION_TOKENS);

        String lmw = LinkedMapWritable.class.getName();

        // if no tokens are defined, add one starting with 140
        if (tokens == null) {
            cfg.set(TupleSerializationProps.SERIALIZATION_TOKENS, "140=" + lmw);
            LogFactory.getLog(EsTap.class).trace(
                    String.format("Registered Cascading serialization token %s for %s", 140, lmw));
        }
        else {
            // token already registered
            if (tokens.contains(lmw)) {
                return;
            }

            // find token id
            Map<Integer, String> mapping = new LinkedHashMap<Integer, String>();
            tokens = tokens.replaceAll("\\s", ""); // allow for whitespace in token set

            for (String pair : tokens.split(",")) {
                String[] elements = pair.split("=");
                mapping.put(Integer.parseInt(elements[0]), elements[1]);
            }

            for (int id = 140; id < 255; id++) {
                if (!mapping.containsKey(Integer.valueOf(id))) {
                    cfg.set(TupleSerializationProps.SERIALIZATION_TOKENS, Util.join(",", Util.removeNulls(tokens, id + "=" + lmw)));
                    LogFactory.getLog(EsTap.class).trace(String.format("Registered Cascading serialization token %s for %s", id, lmw));
                    return;
                }
            }
        }
    }

    static FieldAlias alias(Settings settings) {
        return new FieldAlias(SettingsUtils.aliases(settings.getProperty(MAPPING_NAMES), false), false);
    }

    static List<String> asStrings(Fields fields) {
        if (fields == null || !fields.isDefined()) {
            // use auto-generated name
            return Collections.emptyList();
        }

        int size = fields.size();
        List<String> names = new ArrayList<String>(size);
        for (int fieldIndex = 0; fieldIndex < size; fieldIndex++) {
            names.add(fields.get(fieldIndex).toString());
        }

        return names;
    }

    static Collection<String> fieldToAlias(Settings settings, Fields fields) {
        FieldAlias fa = alias(settings);
        List<String> names = asStrings(fields);
        for (int i = 0; i < names.size(); i++) {
            String original = names.get(i);
            String alias = fa.toES(original);
            if (alias != null) {
                names.set(i, alias);
            }
        }
        return names;
    }

    static Properties extractOriginalProperties(Properties copy) {
        Field field = ReflectionUtils.findField(Properties.class, "defaults", Properties.class);
        ReflectionUtils.makeAccessible(field);
        return ReflectionUtils.getField(field, copy);
    }

    static Settings init(Settings settings, String nodes, int port, String resource, String query, boolean read) {
        if (StringUtils.hasText(nodes)) {
            settings.setHosts(nodes);
        }

        if (port > 0) {
            settings.setPort(port);
        }

        if (StringUtils.hasText(query)) {
            settings.setQuery(query);
        }

        if (StringUtils.hasText(resource)) {
            if (read) {
                settings.setResourceRead(resource);
            }
            else {
                settings.setResourceWrite(resource);
            }
        }

        return settings;
    }

    private static abstract class CoercibleOps {
        static void setObject(TupleEntry entry, Comparable<?> field, Object object) {
            if (object != null && entry.getFields().getType(field) instanceof CoercibleType) {
                entry.setObject(field, object.toString());
            }
            else {
                entry.setObject(field, object);
            }
        }

        static Tuple coerceToString(SinkCall<?, ?> sinkCall) {
            TupleEntry entry = sinkCall.getOutgoingEntry();
            Fields fields = entry.getFields();
            Tuple tuple = entry.getTuple();

            if (fields.hasTypes()) {
                Type types[] = new Type[fields.size()];
                for (int index = 0; index < fields.size(); index++) {
                    Type type = fields.getType(index);
                    if (type instanceof CoercibleType<?>) {
                        types[index] = String.class;
                    }
                    else {
                        types[index] = type;
                    }
                }

                tuple = entry.getCoercedTuple(types);
            }
            return tuple;
        }
    }

    private static abstract class LegacyOps {
        static void setObject(TupleEntry entry, Comparable<?> field, Object object) {
            entry.setObject(field, object);
        }

        static Tuple coerceToString(SinkCall<?, ?> sinkCall) {
            return sinkCall.getOutgoingEntry().getTuple();
        }
    }

    static void setObject(TupleEntry entry, Comparable<?> field, Object object) {
        if (CASCADING_22_AVAILABLE) {
            CoercibleOps.setObject(entry, field, object);
        }
        else {
            LegacyOps.setObject(entry, field, object);
        }
    }

    static Tuple coerceToString(SinkCall<?, ?> sinkCall) {
        return (CASCADING_22_AVAILABLE ? CoercibleOps.coerceToString(sinkCall) : LegacyOps.coerceToString(sinkCall));
    }

    @SuppressWarnings("rawtypes")
    public static Tap hadoopTap(String host, int port, String path, String query, Fields fields, Properties props) {
        return new EsHadoopTap(host, port, path, query, fields, props);
    }
}