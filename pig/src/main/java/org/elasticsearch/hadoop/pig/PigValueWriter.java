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
package org.elasticsearch.hadoop.pig;

import java.util.List;
import java.util.Map;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.builder.FilteringValueWriter;
import org.elasticsearch.hadoop.util.FieldAlias;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.Booleans;

public class PigValueWriter extends FilteringValueWriter<PigTuple> {

    private final boolean writeUnknownTypes;
    private boolean useTupleFieldNames;
    private FieldAlias alias;

    public PigValueWriter() {
        this(false);
    }

    public PigValueWriter(boolean useTupleFieldNames) {
        writeUnknownTypes = false;
        this.useTupleFieldNames = useTupleFieldNames;
        alias = new FieldAlias(false);
    }

    @Override
    public void setSettings(Settings settings) {
        super.setSettings(settings);
        alias = PigUtils.alias(settings);
        useTupleFieldNames = Booleans.parseBoolean(settings.getProperty(PigUtils.NAMED_TUPLE), PigUtils.NAMED_TUPLE_DEFAULT);
    }

    @Override
    public Result write(PigTuple type, Generator generator) {
        return writeRootTuple(type.getTuple(), type.getSchema(), generator, true);
    }

    private Result write(Object object, ResourceFieldSchema field, Generator generator) {
        byte type = (field != null ? field.getType() : DataType.findType(object));

        if (object == null) {
            generator.writeNull();
            return Result.SUCCESFUL();
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
            generator.writeString(PigUtils.convertDateToES(object));
            break;
            // DateTime introduced in Pig 12
        case 65: //DataType.BIGINTEGER
            throw new EsHadoopSerializationException("Big integers are not supported by Elasticsearch - consider using a different type (such as string)");
            // DateTime introduced in Pig 12
        case 70: //DataType.BIGDECIMAL
            throw new EsHadoopSerializationException("Big decimals are not supported by Elasticsearch - consider using a different type (such as string)");
        case DataType.MAP:
            ResourceSchema nestedSchema = field.getSchema();

            // maps that are empty or have different value types have no schema - so no way to check them upfront...
            ResourceFieldSchema valueType = (nestedSchema != null ? nestedSchema.getFields()[0] : null);

            generator.writeBeginObject();
            // Pig maps are actually String -> Object association so we can save the key right away
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                String fieldName = entry.getKey().toString();
                if (shouldKeep(generator.getParentPath(), fieldName)) {
                    generator.writeFieldName(alias.toES(fieldName));
                    Result result = write(entry.getValue(), valueType, generator);
                    if (!result.isSuccesful()) {
                        return result;
                    }
                }
            }
            generator.writeEndObject();
            break;

        case DataType.TUPLE:
            return writeTuple(object, field, generator, useTupleFieldNames, false);

        case DataType.BAG:
            nestedSchema = field.getSchema();

            // empty tuple shortcut
            if (nestedSchema == null) {
                generator.writeBeginArray();
                generator.writeEndArray();
                break;
            }

            ResourceFieldSchema bagType = nestedSchema.getFields()[0];

            generator.writeBeginArray();
            for (Tuple tuple : (DataBag) object) {
                Result result = write(tuple, bagType, generator);
                if (!result.isSuccesful()) {
                    return result;
                }
            }
            generator.writeEndArray();
            break;
        default:
            if (writeUnknownTypes) {
                return handleUnknown(object, field, generator);
            }
            return Result.FAILED(object);
        }
        return Result.SUCCESFUL();
    }

    private Result writeRootTuple(Tuple tuple, ResourceFieldSchema field, Generator generator, boolean writeTupleFieldNames) {
        return writeTuple(tuple, field, generator, writeTupleFieldNames, true);
    }

    private Result writeTuple(Object object, ResourceFieldSchema field, Generator generator, boolean writeTupleFieldNames, boolean isRoot) {
        ResourceSchema nestedSchema = field.getSchema();

        Result result = Result.SUCCESFUL();
        boolean writeAsObject = isRoot || writeTupleFieldNames;

        boolean isEmpty = (nestedSchema == null);

        if (!isEmpty) {
            // check if the tuple contains only empty fields
            boolean allEmpty = true;
            // as an exception, Pig maps can have different value types but in that case
            // there has to be no schema declared (yey!).
            // That being said, maps that are empty also have no schema (boo!).
            // So before we say it's some empty field, check to see if it's a mixed value type map first.
            int currentField = 0;
            Tuple currentTuple = (Tuple) object;
            for (ResourceFieldSchema nestedField : nestedSchema.getFields()) {
                allEmpty = (nestedField.getSchema() == null && !isPopulatedMixedValueMap(nestedField, currentField, currentTuple) && PigUtils.isComplexType(nestedField));
                // break look
                if (!allEmpty) {
                    break;
                }
                currentField++;
            }
            isEmpty = allEmpty;
        }

        // empty tuple shortcut
        if (isEmpty) {
            if (!isRoot) {
                generator.writeBeginArray();
            }
            if (writeAsObject) {
                generator.writeBeginObject();
                generator.writeEndObject();
            }
            if (!isRoot) {
                generator.writeEndArray();
            }
            return result;
        }

        ResourceFieldSchema[] nestedFields = nestedSchema.getFields();

        // use getAll instead of get(int) to avoid having to handle Exception...
        List<Object> tuples = ((Tuple) object).getAll();

        if (!isRoot) {
            generator.writeBeginArray();
        }

        if (writeAsObject) {
            generator.writeBeginObject();
        }

        for (int i = 0; i < nestedFields.length; i++) {
            // Do this based on if the field name is included.
            if (writeAsObject) {
                String name = nestedFields[i].getName();
                // handle schemas without names
                boolean fieldHasName = StringUtils.hasText(name);
                String actualName = (fieldHasName ? alias.toES(name) : Integer.toString(i));
                if (shouldKeep(generator.getParentPath(), actualName)) {
                    generator.writeFieldName(actualName);
                    Result res = write(tuples.get(i), nestedFields[i], generator);
                    if (!res.isSuccesful()) {
                        return res;
                    }
                }
            } else {
                Result res = write(tuples.get(i), nestedFields[i], generator);
                if (!res.isSuccesful()) {
                    return res;
                }
            }
        }
        if (writeAsObject) {
            generator.writeEndObject();
        }
        if (!isRoot) {
            generator.writeEndArray();
        }

        return result;
    }

    /**
     * Checks to see if the given field is a schema-less Map that has values.
     * @return true if Map has no schema but has values (mixed schema map). false if not a Map or if Map is just empty.
     */
    private boolean isPopulatedMixedValueMap(ResourceFieldSchema schema, int field, Tuple object) {
        if (schema.getType() != DataType.MAP) {
            // Can't be a mixed value map if it's not a map at all.
            return false;
        }

        try {
            Object fieldValue = object.get(field);
            Map<?, ?> map = (Map<?, ?>) fieldValue;
            return schema.getSchema() == null && !(map == null || map.isEmpty());
        } catch (ExecException e) {
            throw new EsHadoopIllegalStateException(e);
        }
    }


    protected Result handleUnknown(Object value, ResourceFieldSchema field, Generator generator) {
        return Result.FAILED(value);
    }
}