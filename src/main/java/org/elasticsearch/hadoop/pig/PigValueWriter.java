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
package org.elasticsearch.hadoop.pig;

import java.util.List;
import java.util.Map;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.serialization.ValueWriter;
import org.elasticsearch.hadoop.util.StringUtils;

public class PigValueWriter implements ValueWriter<PigTuple>, SettingsAware {

    private final boolean writeUnknownTypes;
    private FieldAlias alias;

    public PigValueWriter() {
        writeUnknownTypes = false;
        alias = new FieldAlias();
    }

    @Override
    public void setSettings(Settings settings) {
        alias = FieldAlias.load(settings);
    }


    @Override
    public boolean write(PigTuple type, Generator generator) {
        return write(type.getTuple(), type.getSchema(), generator, false);
    }

    public boolean write(Object object, ResourceFieldSchema field, Generator generator, boolean writeFieldName) {
        byte type = (field != null ? field.getType() : DataType.findType(object));

        if (writeFieldName) {
            generator.writeFieldName(alias.toES(field.getName()));
        }

        if (object == null) {
            generator.writeNull();
            return true;
        }

        switch (type) {
        case DataType.ERROR:
        case DataType.UNKNOWN:
            return handleUnknown(object, field, generator);
        case DataType.NULL:
            generator.writeNull();
            break;
        case DataType.BOOLEAN:
            generator.writeBoolean((Boolean) object);
            break;
        case DataType.INTEGER:
            generator.writeNumber(((Number) object).intValue());
            break;
        case DataType.LONG:
            generator.writeNumber(((Number) object).longValue());
            break;
        case DataType.FLOAT:
            generator.writeNumber(((Number) object).floatValue());
            break;
        case DataType.DOUBLE:
            generator.writeNumber(((Number) object).doubleValue());
            break;
        case DataType.BYTE:
            generator.writeNumber((Byte) object);
            break;
        case DataType.CHARARRAY:
            generator.writeString(object.toString());
            break;
        case DataType.BYTEARRAY:
            generator.writeBinary(((DataByteArray) object).get());
            break;
        // DateTime introduced in Pig 11
        case 30: //DataType.DATETIME
            generator.writeString(DateConverter.convertToES(object));
            break;
        case DataType.MAP:
            ResourceSchema nestedSchema = field.getSchema();
            ResourceFieldSchema[] nestedFields = nestedSchema.getFields();

            generator.writeBeginObject();
            int index = 0;
            // Pig maps are actually String -> Object association so we can save the key right away
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                generator.writeFieldName(alias.toES(entry.getKey().toString()));
                write(entry.getValue(), nestedFields[index++], generator, false);
            }
            generator.writeEndObject();
            break;

        case DataType.TUPLE:
            nestedSchema = field.getSchema();
            nestedFields = nestedSchema.getFields();

            // use getAll instead of get(int) to avoid having to handle Exception...
            List<Object> tuples = ((Tuple) object).getAll();

            generator.writeBeginObject();
            for (int i = 0; i < nestedFields.length; i++) {
                String name = nestedFields[i].getName();
                // handle schemas without names
                name = (StringUtils.hasText(name) ? alias.toES(name) : Integer.toString(i));
                generator.writeFieldName(name);
                write(tuples.get(i), nestedFields[i], generator, false);
            }
            generator.writeEndObject();
            break;

        case DataType.BAG:
            nestedSchema = field.getSchema();
            ResourceFieldSchema bagType = nestedSchema.getFields()[0];

            generator.writeBeginArray();
            for (Tuple tuple : (DataBag) object) {
                write(tuple, bagType, generator, false);
            }
            generator.writeEndArray();
            break;
        default:
            if (writeUnknownTypes) {
                return handleUnknown(object, field, generator);
            }
            return false;
        }
        return true;
    }

    protected boolean handleUnknown(Object value, ResourceFieldSchema field, Generator generator) {
        return false;
    }
}