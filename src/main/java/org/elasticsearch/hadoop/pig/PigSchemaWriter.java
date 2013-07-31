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

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.ValueWriter;
import org.elasticsearch.hadoop.util.StringUtils;

class PigSchemaWriter implements ValueWriter<ResourceSchema> {

    private final String typeName;

    PigSchemaWriter(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public boolean write(ResourceSchema schema, Generator generator) {
        generator.writeBeginObject();
        generator.writeFieldName(typeName);
        generator.writeBeginObject();
        generator.writeFieldName("properties");
        generator.writeBeginObject();

        for (ResourceFieldSchema field : schema.getFields()) {
            if (!write(field, generator, true)) {
                return false;
            }
        }

        generator.writeEndObject();
        generator.writeEndObject();
        generator.writeEndObject();

        return true;
    }


    private boolean write(ResourceFieldSchema field, Generator generator, boolean writeFieldName) {
        byte type = field.getType();

        if (writeFieldName) {
            generator.writeFieldName(field.getName());
        }

        generator.writeBeginObject();

        String esType = "string";

        switch (type) {
        case DataType.ERROR:
        case DataType.UNKNOWN:
            return false;
        case DataType.BOOLEAN:
            esType = "boolean";
            break;
        case DataType.INTEGER:
            esType = "integer";
            break;
        case DataType.LONG:
            esType = "long";
            break;
        case DataType.FLOAT:
            esType = "float";
            break;
        case DataType.DOUBLE:
            esType = "double";
            break;
        case DataType.BYTE:
            esType = "byte";
            break;
        case DataType.BYTEARRAY:
            esType = "binary";
            break;
        // DateTime introduced in Pig 11
        case 30: //DataType.DATETIME
            esType = "date";
            break;
        case DataType.MAP:
            generator.writeFieldName("properties");
            generator.writeBeginObject();

            for (ResourceFieldSchema nestedField : field.getSchema().getFields()) {
                if (!write(nestedField, generator, true)) {
                    return false;
                }
            }

            generator.writeEndObject();
            break;

        case DataType.TUPLE:
            generator.writeFieldName("properties");
            generator.writeBeginObject();

            int index = 0;
            for (ResourceFieldSchema nestedField : field.getSchema().getFields()) {
                String name = nestedField.getName();
                // handle schemas without names
                name = (StringUtils.hasText(name) ? name : Integer.toString(index++));
                generator.writeFieldName(name);

                if (!write(nestedField, generator, false)) {
                    return false;
                }

            }

            generator.writeEndObject();
            break;

        case DataType.BAG:
            if (!write(field.getSchema().getFields()[0], generator, true)) {
                return false;
            }
            break;

        case DataType.CHARARRAY:
        // guess type as string
        case DataType.NULL:
        default:

        }

        generator.writeFieldName("type");
        generator.writeString(esType);
        generator.writeEndObject();
        return true;
    }
}