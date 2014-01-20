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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.serialization.command.BulkCommands;
import org.elasticsearch.hadoop.serialization.command.Command;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FieldAlias;

@SuppressWarnings("deprecation")
public class EsSerDe implements SerDe {

    private static Log log = LogFactory.getLog(EsSerDe.class);

    private Properties tableProperties;
    private StructObjectInspector inspector;

    // serialization artifacts
    private BytesArray scratchPad = new BytesArray(512);
    private HiveType hiveType = new HiveType(null, null);
    private HiveBytesArrayWritable result = new HiveBytesArrayWritable();
    private StructTypeInfo structTypeInfo;
    private FieldAlias alias;
    private Command command;

    private boolean writeInitialized = false;

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        inspector = HiveUtils.structObjectInspector(tbl);
        structTypeInfo = HiveUtils.typeInfo(inspector);
        alias = HiveUtils.alias(new PropertiesSettings(tbl));
        this.tableProperties = tbl;
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        if (blob == null || blob instanceof NullWritable) {
            return null;
        }
        return hiveFromWritable(structTypeInfo, blob, alias);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // TODO: can compute serialize stats but not deserialized ones
        return null;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return HiveBytesArrayWritable.class;
    }

    @Override
    public Writable serialize(Object data, ObjectInspector objInspector) throws SerDeException {
        lazyInitializeWrite();

        // serialize the type directly to json (to avoid converting to Writable and then serializing)
        scratchPad.reset();
        hiveType.setObjectInspector(objInspector);
        hiveType.setObject(data);

        command.write(hiveType).copyTo(scratchPad);
        result.setContent(scratchPad);
        return result;
    }

    private void lazyInitializeWrite() {
        if (writeInitialized) {
            return;
        }
        writeInitialized = true;
        Settings settings = SettingsManager.loadFrom(tableProperties);

        InitializationUtils.setValueWriterIfNotSet(settings, HiveValueWriter.class, log);
        InitializationUtils.setFieldExtractorIfNotSet(settings, HiveFieldExtractor.class, log);
        InitializationUtils.setBytesConverterIfNeeded(settings, HiveBytesConverter.class, log);
        this.command = BulkCommands.create(settings);
    }


    @SuppressWarnings("unchecked")
    static Object hiveFromWritable(TypeInfo type, Writable data, FieldAlias alias) {
        if (data == null || data instanceof NullWritable) {
            return null;
        }

        switch (type.getCategory()) {
        case LIST: {// or ARRAY
            ListTypeInfo listType = (ListTypeInfo) type;
            TypeInfo listElementType = listType.getListElementTypeInfo();

            ArrayWritable aw = (ArrayWritable) data;

            List<Object> list = new ArrayList<Object>();
            for (Writable writable : aw.get()) {
                list.add(hiveFromWritable(listElementType, writable, alias));
            }

            return list;
        }

        case MAP: {
            MapTypeInfo mapType = (MapTypeInfo) type;
            Map<Writable, Writable> mw = (Map<Writable, Writable>) data;

            Map<Object, Object> map = new LinkedHashMap<Object, Object>();

            for (Entry<Writable, Writable> entry : mw.entrySet()) {
                map.put(hiveFromWritable(mapType.getMapKeyTypeInfo(), entry.getKey(), alias),
                        hiveFromWritable(mapType.getMapValueTypeInfo(), entry.getValue(), alias));
            }

            return map;
        }
        case STRUCT: {
            StructTypeInfo structType = (StructTypeInfo) type;
            List<String> names = structType.getAllStructFieldNames();
            List<TypeInfo> info = structType.getAllStructFieldTypeInfos();

            // return just the values
            List<Object> struct = new ArrayList<Object>();

            MapWritable map = (MapWritable) data;
            Text reuse = new Text();
            for (int index = 0; index < names.size(); index++) {
                reuse.set(alias.toES(names.get(index)));
                struct.add(hiveFromWritable(info.get(index), map.get(reuse), alias));
            }
            return struct;
        }

        case UNION: {
            throw new UnsupportedOperationException("union not yet supported");//break;
        }

        case PRIMITIVE:
        default:
            // return as is
            return data;
        }
    }
}